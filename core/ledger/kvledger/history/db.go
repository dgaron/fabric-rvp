/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	protoutil "github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
)

var logger = flogging.MustGetLogger("history")

// DBProvider provides handle to HistoryDB for a given channel
type DBProvider struct {
	leveldbProvider *leveldbhelper.Provider
}

// NewDBProvider instantiates DBProvider
func NewDBProvider(path string) (*DBProvider, error) {
	logger.Debugf("constructing HistoryDBProvider dbPath=%s", path)
	levelDBProvider, err := leveldbhelper.NewProvider(
		&leveldbhelper.Conf{
			DBPath:         path,
			ExpectedFormat: dataformat.CurrentFormat,
		},
	)
	if err != nil {
		return nil, err
	}
	return &DBProvider{
		leveldbProvider: levelDBProvider,
	}, nil
}

// MarkStartingSavepoint creates historydb to be used for a ledger that is created from a snapshot
func (p *DBProvider) MarkStartingSavepoint(name string, savepoint *version.Height) error {
	db := p.GetDBHandle(name)
	err := db.levelDB.Put(savePointKey, savepoint.ToBytes(), true)
	return errors.WithMessagef(err, "error while writing the starting save point for ledger [%s]", name)
}

// GetDBHandle gets the handle to a named database
func (p *DBProvider) GetDBHandle(name string) *DB {
	return &DB{
		levelDB:     p.leveldbProvider.GetDBHandle(name),
		name:        name,
		globalIndex: make(map[string][]byte),
	}
}

// Close closes the underlying db
func (p *DBProvider) Close() {
	p.leveldbProvider.Close()
}

// Drop drops channel-specific data from the history db
func (p *DBProvider) Drop(channelName string) error {
	return p.leveldbProvider.Drop(channelName)
}

// DB maintains and provides access to history data for a particular channel
type DB struct {
	levelDB     *leveldbhelper.DBHandle
	name        string
	globalIndex map[string][]byte
}

type HistoryValue struct {
	Prev         uint64   `json:"prev"`
	NumVersions  uint64   `json:"num_versions"`
	Transactions []uint64 `json:"transactions"`
}

// Commit implements method in HistoryDB interface
func (d *DB) Commit(block *common.Block) error {

	blockNo := block.Header.Number
	//Set the starting tranNo to 0
	var tranNo uint64

	dbBatch := d.levelDB.NewUpdateBatch()
	dataKeys := make(map[string]newIndex)

	historyMap := make(map[string]HistoryValue)

	// Read existing data
	filePath := "/var/PeerStorage/historyData.json"
	if _, err := os.Stat(filePath); err == nil {
		file, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}

		err = json.Unmarshal(file, &historyMap)
		if err != nil {
			return err
		}
	}

	logger.Debugf("Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		d.name, blockNo, len(block.Data.Data))

	// Get the invalidation byte array for the block
	txsFilter := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	// write each tran's write set to history db
	for _, envBytes := range block.Data.Data {

		// If the tran is marked as invalid, skip it
		if txsFilter.IsInvalid(int(tranNo)) {
			logger.Debugf("Channel [%s]: Skipping history write for invalid transaction number %d",
				d.name, tranNo)
			tranNo++
			continue
		}

		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return err
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			return err
		}

		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return err
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {
			// extract RWSet from transaction
			respPayload, err := protoutil.GetActionFromEnvelope(envBytes)
			if err != nil {
				return err
			}
			txRWSet := &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
				return err
			}
			// add a history record for each write
			for _, nsRWSet := range txRWSet.NsRwSets {
				ns := nsRWSet.NameSpace

				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					var (
						prev         uint64
						numVersions  uint64
						transactions []uint64
					)
					globalIndexBytes, present := d.globalIndex[kvWrite.Key]
					if present {
						indexVal, present := dataKeys[kvWrite.Key]
						if present {
							// presence in dataKeys implies presence in globalIndex
							prev, numVersions, transactions, err = decodeNewIndex(indexVal)
							if err != nil {
								return err
							}
						} else {
							prev, numVersions, err = decodeGlobalIndex(globalIndexBytes)
							if err != nil {
								return err
							}
						}
					} else {
						prev = blockNo
						// numVersions is initialized to 0
					}

					transactions = append(transactions, tranNo)
					numVersions++

					key := fmt.Sprintf("%s_%s_%d", ns, kvWrite.Key, blockNo)
					historyMap[key] = HistoryValue{
						Prev:         prev,
						NumVersions:  numVersions,
						Transactions: transactions,
					}

					d.globalIndex[kvWrite.Key] = constructGlobalIndex(prev, numVersions)

					indexVal := constructNewIndex(prev, numVersions, transactions)
					dataKeys[kvWrite.Key] = indexVal

					dataKey := constructDataKeyNew(ns, kvWrite.Key, blockNo)
					dbBatch.Put(dataKey, indexVal)
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}

	// add savepoint for recovery purpose
	height := version.NewHeight(blockNo, tranNo)
	dbBatch.Put(savePointKey, height.ToBytes())

	// write the block's history records and savepoint to LevelDB
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := d.levelDB.WriteBatch(dbBatch, true); err != nil {
		return err
	}

	file, _ := json.MarshalIndent(historyMap, "", " ")
	_ = ioutil.WriteFile(filePath, file, 0644)

	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]", d.name, blockNo)
	return nil
}

// NewQueryExecutor implements method in HistoryDB interface
func (d *DB) NewQueryExecutor(blockStore *blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return &QueryExecutor{d.levelDB, blockStore}, nil
}

// GetLastSavepoint implements returns the height till which the history is present in the db
func (d *DB) GetLastSavepoint() (*version.Height, error) {
	versionBytes, err := d.levelDB.Get(savePointKey)
	if err != nil || versionBytes == nil {
		return nil, err
	}
	height, _, err := version.NewHeightFromBytes(versionBytes)
	if err != nil {
		return nil, err
	}
	return height, nil
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (d *DB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	savepoint, err := d.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// Name returns the name of the database that manages historical states.
func (d *DB) Name() string {
	return "history"
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (d *DB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	block := blockAndPvtdata.Block

	// log every 1000th block at Info level so that history rebuild progress can be tracked in production envs.
	if block.Header.Number%1000 == 0 {
		logger.Infof("Recommitting block [%d] to history database", block.Header.Number)
	} else {
		logger.Debugf("Recommitting block [%d] to history database", block.Header.Number)
	}
	return d.Commit(block)
}
