/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/queryresult"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	protoutil "github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// QueryExecutor is a query executor against the LevelDB history DB
type QueryExecutor struct {
	levelDB    *leveldbhelper.DBHandle
	blockStore *blkstorage.BlockStore
	globalIndex map[string][]byte
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	dbItr, err := q.levelDB.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}

	blockNum, present := q.globalIndex[key]
	if !present {
		// This scanner will return nil upon first call to Next()
		return &historyScanner{namespace, key, dbItr, q.blockStore, 0, 0, nil, -1}, nil
	}

	dataKey := constructDataKey(namespace, blockNum, key)
	scanner.dbItr.Seek(dataKey)

	indexVal := scanner.dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return nil, err
	}

	txIndex := len(transactions) - 1

	return &historyScanner{namespace, key, dbItr, q.blockStore, blockNum, prev, transactions, txIndex}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	currentBlock uint64
	previousBlock	uint64
	transactions []uint64
	txIndex      int
}

func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex <= -1 {
		if scanner.currentBlock == scanner.previousBlock {
			return nil, nil
		}
		scanner.updateBlock()
	}

	blockNum := scanner.currentBlock
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex--

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, scanner.key, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, scanner.key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *historyScanner) Close() {
	scanner.dbItr.Release()
}

func (scanner *historyScanner) updateBlock() error {
	scanner.currentBlock = scanner.previousBlock
	dataKey := constructDataKey(scanner.namespace, scanner.previousBlock, scanner.key)
	scanner.dbItr.Seek(dataKey)
	indexVal := scanner.dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return err
	}
	scanner.previousBlock = prev 
	scanner.transactions = transactions
	scanner.txIndex = len(transactions) - 1
	return nil
}

// GetHistoryForKeys implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKeys(namespace string, keys []string) (commonledger.ResultsIterator, error) {
	scanners := make(map[string]*historyScanner)
	for _, key := range keys {
		scanners := q.GetHistoryForKey(namespace, key)
	}
	scanner := &multipleHistoryScanner{namespace, keys, scanners, nil, 0, nil, 0}
	err := scanner.updateBlock()
	if err != nil {
		return nil, err
	}
	return scanner, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type multipleHistoryScanner struct {
	namespace       string
	keys            []string
	scanners        map[string]*historyScanner
	currentKeyIndex int
}

func (scanner *multipleHistoryScanner) Next() (commonledger.QueryResult, error) {
	key := scanner.keysInBlock[scanner.currentKeyIndex]

	queryResult, err := scanner.scanners[key].Next()
	if err != nil {
		return nil, err
	}

	if queryResult == nil {
		scanner.currentKeyIndex--
		if scanner.currentKeyIndex <= -1 {
			return nil, nil
		}

		key := scanner.keysInBlock[scanner.currentKeyIndex]

		queryResult, err = scanner.scanners[key].Next()
		if err != nil {
			return nil, err
		}
	}

	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, key, queryResult.(*queryresult.KeyModification).TxId)

	return queryResult, nil
}

func (scanner *multipleHistoryScanner) Close() {
	for _, key := range scanner.keys {
		scanner.scanners[key].dbItr.Release()
	}
}

// getTxIDandKeyWriteValueFromTran inspects a transaction for writes to a given key
func getKeyModificationFromTran(tranEnvelope *common.Envelope, namespace string, key string) (commonledger.QueryResult, error) {
	logger.Debugf("Entering getKeyModificationFromTran %s:%s", namespace, key)

	// extract action from the envelope
	payload, err := protoutil.UnmarshalPayload(tranEnvelope.Payload)
	if err != nil {
		return nil, err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
	if err != nil {
		return nil, err
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	txID := chdr.TxId
	timestamp := chdr.Timestamp

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return nil, err
	}

	// look for the namespace and key by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				if kvWrite.Key == key {
					return &queryresult.KeyModification{TxId: txID, Value: kvWrite.Value,
						Timestamp: timestamp, IsDelete: rwsetutil.IsKVWriteDelete(kvWrite)}, nil
				}
			} // end keys loop
			logger.Debugf("key [%s] not found in namespace [%s]'s writeset", key, namespace)
			return nil, nil
		} // end if
	} //end namespaces loop
	logger.Debugf("namespace [%s] not found in transaction's ReadWriteSets", namespace)
	return nil, nil
}

func (q *QueryExecutor) GetVersionsForKey(namespace string, key string, start uint64, end uint64) (commonledger.ResultsIterator, error) {
	return nil, nil
}

/**

type versionScanner struct {
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	numVersions  uint64
	transactions []uint64
	txIndex      int
	start        uint64
	end          uint64
}

func (q *QueryExecutor) GetVersionsForKey(namespace string, key string, start uint64, end uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	dbItr, err := q.levelDB.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}
	scanner := &versionScanner{namespace, key, dbItr, q.blockStore, nil, 0, nil, 0, start, end}
	// Find first block containing first version in range
	for {
		if !scanner.dbItr.Next() {
			// Iterator exhausted, first version in range > last actual version of key
			// This ensures first call to Next() returns nil result
			if scanner.transactions != nil {
				scanner.txIndex = len(scanner.transactions)
			}
			return scanner, nil
		}
		indexVal := scanner.dbItr.Value()
		_, scanner.numVersions, scanner.transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		if scanner.numVersions >= scanner.start {

			firstVersionInBlock := scanner.numVersions - uint64(len(scanner.transactions)) + 1
			firstIndex := scanner.start - firstVersionInBlock
			scanner.txIndex = int(firstIndex)

			if err = scanner.updateBlock(); err != nil {
				return nil, err
			}
			return scanner, nil
		}
	}
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex == len(scanner.transactions) {
		// Returned all the transactions from the block, fetch new block & update indices
		if !scanner.dbItr.Next() {
			// Iterator exhausted, last version in range already found
			return nil, nil
		}
		indexVal := scanner.dbItr.Value()
		var err error
		_, scanner.numVersions, scanner.transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		if err = scanner.updateBlock(); err != nil {
			return nil, err
		}
		scanner.txIndex = 0
	}

	firstVersionInBlock := scanner.numVersions - uint64(len(scanner.transactions)) + 1
	currentVersionNum := firstVersionInBlock + uint64(scanner.txIndex)
	if currentVersionNum > scanner.end {
		logger.Debugf("End version %d found for key: %s", scanner.end, scanner.key)
		return nil, nil
	}

	tranNum := scanner.transactions[scanner.txIndex]

	// Index into stored block & get the tranEnvelope
	txEnvelopeBytes := scanner.currentBlock.Data.Data[tranNum]
	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, scanner.key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("Version %d not found for key %s for namespace:%s", currentVersionNum, scanner.key, scanner.namespace)
		return nil, errors.Errorf("Version %d not found for key %s for namespace:%s", currentVersionNum, scanner.key, scanner.namespace)
	}

	scanner.txIndex++

	logger.Debugf("Found key version %d for namespace:%s key:%s from transaction %s", currentVersionNum, scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil

}

func (scanner *versionScanner) Close() {
	scanner.dbItr.Release()
}
**/