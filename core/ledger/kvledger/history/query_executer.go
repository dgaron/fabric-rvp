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
	"github.com/hyperledger/fabric/common/ledger/util"
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
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	dbItr, err := q.levelDB.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}

	GIkey := []byte("_" + key)
	globalIndexBytes, err := q.levelDB.Get(GIkey)
	if err != nil {
		return nil, errors.Errorf("Error reading from history database for key: %s", key)
	}
	if globalIndexBytes == nil {
		logger.Debugf("Key not present in GI. Initialized nil history scanner for key %s.", key)
		// This scanner will return nil upon first call to Next()
		return &historyScanner{namespace, key, dbItr, q.blockStore, 0, 0, nil, -1}, nil
	}
	blockNum, _, err := decodeGlobalIndex(globalIndexBytes)
	if err != nil {
		return nil, errors.Errorf("Error decoding lasts known version for key: %s", key)
	}

	dataKey := constructDataKey(namespace, blockNum, key)
	found := dbItr.Seek(dataKey)
	if !found {
		return nil, errors.Errorf("Error from dbItr.Seek() for key: %s, block: %d", key, blockNum)
	}

	indexVal := dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return nil, err
	}

	txIndex := len(transactions) - 1

	logger.Debugf("Initialized history scanner for key %s, currentBlock: %d, previousBlock: %d", key, blockNum, prev)

	return &historyScanner{namespace, key, dbItr, q.blockStore, blockNum, prev, transactions, txIndex}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	namespace     string
	key           string
	dbItr         iterator.Iterator
	blockStore    *blkstorage.BlockStore
	currentBlock  uint64
	previousBlock uint64
	transactions  []uint64
	txIndex       int
}

func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex == -1 {
		oldBlockNum := scanner.currentBlock
		err := scanner.updateBlock()
		if err != nil {
			return nil, err
		}
		if scanner.currentBlock == oldBlockNum {
			// Iterator exhausted
			return nil, nil
		}
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
	if scanner.transactions == nil {
		return nil
	}
	logger.Debugf("Updating block for key %s. currentBlock %d, previousBlock %d", scanner.key, scanner.currentBlock, scanner.previousBlock)
	scanner.currentBlock = scanner.previousBlock
	dataKey := constructDataKey(scanner.namespace, scanner.previousBlock, scanner.key)
	found := scanner.dbItr.Seek(dataKey)
	if !found {
		return errors.Errorf("Error from dbItr.Seek() for key: %s, block: %d", scanner.key, scanner.previousBlock)
	}
	indexVal := scanner.dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return err
	}
	scanner.previousBlock = prev
	scanner.transactions = transactions
	scanner.txIndex = len(transactions) - 1
	logger.Debugf("Fished updating block for key %s. currentBlock %d, previousBlock %d", scanner.key, scanner.currentBlock, scanner.previousBlock)
	return nil
}

// GetHistoryForKeys implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKeys(namespace string, keys []string) (commonledger.ResultsIterator, error) {
	scanners := make(map[string]*historyScanner)
	for _, key := range keys {
		scanner, err := q.GetHistoryForKey(namespace, key)
		if err != nil {
			return nil, err
		}
		var ok bool
		scanners[key], ok = scanner.(*historyScanner)
		if !ok {
			return nil, errors.Errorf("Error converting commonledger.ResultsIterator to historyScanner")
		}
	}
	scanner := &multipleHistoryScanner{namespace, keys, scanners, 0}
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
	key := scanner.keys[scanner.currentKeyIndex]

	queryResult, err := scanner.scanners[key].Next()
	if err != nil {
		return nil, err
	}

	for queryResult == nil {
		scanner.currentKeyIndex++
		if scanner.currentKeyIndex >= len(scanner.keys) {
			return nil, nil
		}

		key := scanner.keys[scanner.currentKeyIndex]

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
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	if end <= 0 || start <= 0 {
		return nil, errors.Errorf("Start: %d, end: %d cannot be less than 1", start, end)
	}

	dbItr, err := q.levelDB.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}

	GIkey := []byte("_" + key)
	globalIndexBytes, err := q.levelDB.Get(GIkey)
	if err != nil {
		return nil, errors.Errorf("Error reading from history database for key: %s", key)
	}
	if globalIndexBytes == nil {
		logger.Debugf("Key not present in GI. Initialized nil version scanner for key %s.", key)
		// This scanner will return nil upon first call to Next()
		return &versionScanner{namespace, key, dbItr, q.blockStore, start, nil, -1, start, end}, nil
	}
	blockNum, _, err := decodeGlobalIndex(globalIndexBytes)
	if err != nil {
		return nil, errors.Errorf("Error decoding lasts known version for key: %s", key)
	}

	dataKey := constructDataKey(namespace, blockNum, key)
	found := dbItr.Seek(dataKey)
	if !found {
		return nil, errors.Errorf("Error from dbItr.Seek() for key: %s, block: %d", key, blockNum)
	}

	indexVal := dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return nil, err
	}

	txIndex := len(transactions) - 1

	logger.Debugf("Initialized version scanner for key %s, currentBlock: %d, previousBlock: %d", key, blockNum, prev)

	scanner := &versionScanner{namespace, key, dbItr, q.blockStore, blockNum, indexVal, txIndex, start, end}
	// Find first block containing end version in range
	for {
		_, numVersions, transactions, err := decodeNewIndex(scanner.indexVal)
		if err != nil {
			return nil, err
		}
		firstVersionInBlock := numVersions - uint64(len(transactions)) + 1
		if firstVersionInBlock <= scanner.end {
			if numVersions >= scanner.end {
				scanner.txIndex = int(scanner.end - firstVersionInBlock)
			} else {
				scanner.txIndex = len(transactions) - 1
			}
			return scanner, nil
		}

		oldBlockNum := scanner.currentBlock
		err = scanner.updateBlock()
		if err != nil {
			return nil, err
		}
		if scanner.currentBlock == oldBlockNum {
			// Iterator exhausted
			scanner.txIndex = -1
			return scanner, nil
		}
	}
}

type versionScanner struct {
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	currentBlock uint64
	indexVal     newIndex
	txIndex      int
	start        uint64
	end          uint64
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	if scanner.indexVal == nil {
		return nil, nil
	}
	_, numVersions, transactions, err := decodeNewIndex(scanner.indexVal)
	if err != nil {
		return nil, err
	}
	firstVersionInBlock := numVersions - uint64(len(transactions)) + 1
	currentVersionNum := firstVersionInBlock + uint64(scanner.txIndex)
	if currentVersionNum < scanner.start {
		logger.Debugf("First requested version %d found for key: %s", scanner.start, scanner.key)
		return nil, nil
	}
	if scanner.txIndex == -1 {
		oldBlockNum := scanner.currentBlock
		err := scanner.updateBlock()
		if err != nil {
			return nil, err
		}
		if scanner.currentBlock == oldBlockNum {
			// Iterator exhausted
			return nil, nil
		}
		_, _, transactions, err = decodeNewIndex(scanner.indexVal)
		if err != nil {
			return nil, err
		}
	}

	blockNum := scanner.currentBlock
	tranNum := transactions[scanner.txIndex]
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
	logger.Debugf("Found key version %d for namespace:%s key:%s from transaction %s", currentVersionNum, scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *versionScanner) Close() {
	scanner.dbItr.Release()
}

func (scanner *versionScanner) updateBlock() error {
	prev, _, _, err := decodeNewIndex(scanner.indexVal)
	if err != nil {
		return err
	}
	logger.Debugf("Updating block for key %s. currentBlock %d, previousBlock %d", scanner.key, scanner.currentBlock, prev)
	scanner.currentBlock = prev
	dataKey := constructDataKey(scanner.namespace, prev, scanner.key)
	found := scanner.dbItr.Seek(dataKey)
	if !found {
		return errors.Errorf("Error from dbItr.Seek() for key: %s, block: %d", scanner.key, prev)
	}
	scanner.indexVal = scanner.dbItr.Value()
	_, _, transactions, err := decodeNewIndex(scanner.indexVal)
	if err != nil {
		return err
	}
	scanner.txIndex = len(transactions) - 1
	logger.Debugf("Fished updating block for key %s. currentBlock %d, previousBlock %d", scanner.key, scanner.currentBlock, prev)
	return nil
}

// GetUpdatesByBlockRange implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetUpdatesByBlockRange(namespace string, start uint64, end uint64, updates uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	if end <= 0 || start <= 0 {
		return nil, errors.Errorf("Start: %d, end: %d cannot be less than 1", start, end)
	}

	// Create iterator over range from ns~startBlock to ns~endBlock
	startKey := append([]byte(namespace), compositeKeySep...)
	startKey = append(startKey, util.EncodeOrderPreservingVarUint64(start)...)

	// End key is exclusive, end+1 results in range including all entries from 'start' to 'end'
	endKey := append([]byte(namespace), compositeKeySep...)
	endKey = append(endKey, util.EncodeOrderPreservingVarUint64(end+1)...)

	dbItr, err := q.levelDB.GetIterator(startKey, endKey)
	if err != nil {
		return nil, err
	}

	logger.Debugf("Initialized block scanner over range from start: %d to end: %d seeking results with at least %d updates", start, end, updates)

	keys := make(map[string]bool)
	scanner := &blockRangeScanner{namespace, dbItr, q.blockStore, keys, start, "", nil, 0}

	err = scanner.countKeyUpdates(updates)
	if err != nil {
		return nil, err
	}

	return scanner, nil
}

// blockRangeScanner implements ResultsIterator for iterating through history results
type blockRangeScanner struct {
	namespace    string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	keys         map[string]bool
	currentBlock uint64
	currentKey   string
	transactions []uint64
	txIndex      int
}

func (scanner *blockRangeScanner) Next() (commonledger.QueryResult, error) {

	if scanner.txIndex >= len(scanner.transactions) {
		hasNext, key, err := scanner.nextKey()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			// Iterator exhausted
			return nil, nil
		}
		scanner.currentKey = key
	}

	blockNum := scanner.currentBlock
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex++

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, scanner.currentKey, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, scanner.currentKey)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.currentKey, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.currentKey, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, scanner.currentKey, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *blockRangeScanner) Close() {
	scanner.dbItr.Release()
}

func (scanner *blockRangeScanner) countKeyUpdates(updates uint64) error {
	keyCounts := make(map[string]int)
	for scanner.dbItr.Next() {
		_, key, err := decodeDataKey(scanner.namespace, scanner.dbItr.Key())
		if err != nil {
			return err
		}
		_, _, transactions, err := decodeNewIndex(scanner.dbItr.Value())
		if err != nil {
			return err
		}
		keyCounts[key] += len(transactions)
	}
	for key, count := range keyCounts {
		logger.Debugf("Key: %s updated %d times\n", key, count)
		if count >= int(updates) {
			scanner.keys[key] = true
		}
	}
	scanner.dbItr.First()
	scanner.dbItr.Prev()
	return nil
}

func (scanner *blockRangeScanner) nextKey() (bool, string, error) {
	for {
		if !scanner.dbItr.Next() {
			return false, "", nil
		}
		dataKey := scanner.dbItr.Key()
		blockNum, key, err := decodeDataKey(scanner.namespace, dataKey)
		if err != nil {
			return false, "", err
		}
		scanner.currentBlock = blockNum
		if scanner.keys[key] {
			indexVal := scanner.dbItr.Value()
			_, _, transactions, err := decodeNewIndex(indexVal)
			if err != nil {
				return false, "", err
			}
			scanner.transactions = transactions
			scanner.txIndex = 0
			logger.Debugf("Next key: %s, appears in block %d transactions: %v\n", key, scanner.currentBlock, scanner.transactions)
			return true, key, nil
		}
	}
}
