/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"encoding/json"
	"os"
	"strconv"

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

// GetDBHandle gets the handle to a named database
func (p *DBProvider) GetDBHandle(name string) (*DB, error) {
	return &DB{
			levelDB:     p.leveldbProvider.GetDBHandle(name),
			name:        name,
			globalIndex: make(map[string][]byte),
		},
		nil
}

// Close closes the underlying db
func (p *DBProvider) Close() {
	p.leveldbProvider.Close()
}

// DB maintains and provides access to history data for a particular channel
type DB struct {
	levelDB     *leveldbhelper.DBHandle
	name        string
	globalIndex map[string][]byte
}

// Commit implements method in HistoryDB interface
func (d *DB) Commit(block *common.Block) error {

	blockNo := block.Header.Number
	//Set the starting tranNo to 0
	var tranNo uint64

	dbBatch := d.levelDB.NewUpdateBatch()
	dataKeys := make(map[string]newIndex)

	logger.Debugf("Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		d.name, blockNo, len(block.Data.Data))

	// Get the invalidation byte array for the block
	txsFilter := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	// DEBUG
	TEMPFILE, _ := os.OpenFile("/var/index.json", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	defer TEMPFILE.Close()
	// ENDDEBUG

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
							prev, numVersions, _, err = decodeNewIndex(globalIndexBytes)
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
					d.globalIndex[kvWrite.Key] = constructNewIndex(prev, numVersions, nil)
					indexVal := constructNewIndex(prev, numVersions, transactions)
					dataKeys[kvWrite.Key] = indexVal
					dataKey := constructDataKeyNew(ns, kvWrite.Key, blockNo)
					dbBatch.Put(dataKey, indexVal)

					// DEBUG
					tranBytes, _ := json.Marshal(transactions)
					outputString := "Key: " + kvWrite.Key + " Block: " + strconv.FormatUint(blockNo, 10) + " Prev: " + strconv.FormatUint(prev, 10) + " Versions: " + strconv.FormatUint(numVersions, 10) + " "
					outputString += "Transactions: " + string(tranBytes) + "\n"
					TEMPFILE.WriteString(outputString)
					// END DEBUG
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

	if err := d.Commit(block); err != nil {
		return err
	}
	return nil
}
