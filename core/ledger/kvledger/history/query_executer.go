/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"sort"

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
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	rangeScan := constructRangeScan(namespace, key)
	dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
	if err != nil {
		return nil, err
	}

	// By default, dbItr is in the orderer of oldest to newest and its cursor is at the beginning of the entries.
	// Need to call Last() and Next() to move the cursor to the end of the entries so that we can iterate
	// the entries in the order of newest to oldest.
	if dbItr.Last() {
		dbItr.Next()
	}
	return &historyScanner{rangeScan, namespace, key, dbItr, q.blockStore}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	rangeScan  *rangeScan
	namespace  string
	key        string
	dbItr      iterator.Iterator
	blockStore *blkstorage.BlockStore
}

// Next iterates to the next key, in the order of newest to oldest, from history scanner.
// It decodes blockNumTranNumBytes to get blockNum and tranNum,
// loads the block:tran from block storage, finds the key and returns the result.
func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	// call Prev because history query result is returned from newest to oldest
	if !scanner.dbItr.Prev() {
		return nil, nil
	}
	historyKey := scanner.dbItr.Key()
	blockNum, tranNum, err := scanner.rangeScan.decodeBlockNumTranNum(historyKey)
	if err != nil {
		return nil, err
	}
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

// GetHistoryForKeys implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKeys(namespace string, keys []string) (commonledger.ResultsIterator, error) {
	keyMap := make(map[string]keyData)
	validKeys := []string{}
	for _, key := range keys {
		rangeScan := constructRangeScan(namespace, key)
		dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
		if err == nil && dbItr.Last() {
			dbItr.Next()
			keyMap[key] = keyData{rangeScan, dbItr}
			validKeys = append(validKeys, key)
		}
	}
	scanner := &multipleHistoryScanner{namespace, validKeys, keyMap, q.blockStore, 0}
	return scanner, nil
}

type keyData struct {
	rangeScan *rangeScan
	dbItr     iterator.Iterator
}

// historyScanner implements ResultsIterator for iterating through history results
type multipleHistoryScanner struct {
	namespace  string
	keys       []string
	keyMap     map[string]keyData
	blockStore *blkstorage.BlockStore
	keyIndex   int
}

func (scanner *multipleHistoryScanner) Next() (commonledger.QueryResult, error) {

	key := scanner.keys[scanner.keyIndex]
	dbItr := scanner.keyMap[key].dbItr
	rangeScan := scanner.keyMap[key].rangeScan

	if !dbItr.Prev() {
		scanner.keyIndex++
		// Indicates we've exhausted the iterators for every key
		if scanner.keyIndex == len(scanner.keys) {
			return nil, nil
		}
		key = scanner.keys[scanner.keyIndex]
		dbItr = scanner.keyMap[key].dbItr
		rangeScan = scanner.keyMap[key].rangeScan
		dbItr.Prev()
	}

	historyKey := dbItr.Key()

	// Retrieval of key modification proceeds exactly as originally implemented
	blockNum, tranNum, err := rangeScan.decodeBlockNumTranNum(historyKey)
	if err != nil {
		return nil, err
	}
	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, key, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, key, queryResult.(*queryresult.KeyModification).TxId)

	return queryResult, nil
}

func (scanner *multipleHistoryScanner) Close() {
	for _, key := range scanner.keys {
		scanner.keyMap[key].dbItr.Release()
	}
}

type versionScanner struct {
	rangeScan  *rangeScan
	namespace  string
	key        string
	dbItr      iterator.Iterator
	blockStore *blkstorage.BlockStore
	current    uint64
	start      uint64
	end        uint64
}

func (q *QueryExecutor) GetVersionsForKey(namespace string, key string, start uint64, end uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}
	rangeScan := constructRangeScan(namespace, key)
	dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
	if err != nil {
		return nil, err
	}
	scanner := &versionScanner{rangeScan, namespace, key, dbItr, q.blockStore, 0, start, end}
	for {
		scanner.current++
		if !scanner.dbItr.Next() || scanner.current >= scanner.start {
			scanner.dbItr.Prev()
			scanner.current--
			return scanner, nil
		}
	}
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	if !scanner.dbItr.Next() {
		return nil, nil
	}
	scanner.current++
	if scanner.current > scanner.end {
		return nil, nil
	}

	historyKey := scanner.dbItr.Key()
	blockNum, tranNum, err := scanner.rangeScan.decodeBlockNumTranNum(historyKey)
	if err != nil {
		return nil, err
	}
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

func (scanner *versionScanner) Close() {
	scanner.dbItr.Release()
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

// ---------------------------------------------

// GetUpdatesByBlockRange implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetUpdatesByBlockRange(namespace string, start uint64, end uint64, updates uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	if end <= 0 || start <= 0 {
		return nil, errors.Errorf("Start: %d, end: %d cannot be less than 1", start, end)
	}

	scanner := &blockRangeScanner{namespace, q.levelDB, q.blockStore, start, end, nil, -1, nil, nil}

	err := scanner.countKeyUpdates(updates)
	if err != nil {
		return nil, err
	}

	_, err = scanner.nextKey()
	if err != nil {
		return nil, err
	}

	logger.Debugf("Initialized block scanner over range from start: %d to end: %d seeking results with at least %d updates", start, end, updates)

	return scanner, nil
}

// blockRangeScanner implements ResultsIterator for iterating through history results
type blockRangeScanner struct {
	namespace        string
	levelDB          *leveldbhelper.DBHandle
	blockStore       *blkstorage.BlockStore
	start            uint64
	end              uint64
	keys             []string
	keyIndex         int
	currentKeyItr    iterator.Iterator
	currentRangeScan *rangeScan
}

func (scanner *blockRangeScanner) Next() (commonledger.QueryResult, error) {

	blockNum, tranNum, exhausted, err := scanner.getNextBlockTran()
	if err != nil {
		return nil, err
	}
	if exhausted {
		return nil, nil
	}

	key := scanner.keys[scanner.keyIndex]

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, key, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *blockRangeScanner) Close() {
	if scanner.currentKeyItr != nil {
		scanner.currentKeyItr.Release()
	}
}

func (scanner *blockRangeScanner) countKeyUpdates(updates uint64) error {
	keyCounts := make(map[string]int)
	for i := scanner.start; i <= scanner.end; i++ {
		nextBlockBytes, err := scanner.blockStore.RetrieveBlockByNumber(i)
		if err != nil {
			return err
		}
		for _, txEnvelopeBytes := range nextBlockBytes.Data.Data {
			tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
			if err != nil {
				return err
			}
			err = countKeyUpdatesForTran(tranEnvelope, scanner.namespace, keyCounts)
			if err != nil {
				return err
			}
		}
	}
	for key, count := range keyCounts {
		logger.Debugf("Key: %s updated %d times\n", key, count)
		if count >= int(updates) {
			scanner.keys = append(scanner.keys, key)
		}
	}
	sort.Strings(scanner.keys)
	logger.Debugf("%d keys found meeting the update threshold in block range", len(scanner.keys))
	return nil
}

func (scanner *blockRangeScanner) nextKey() (bool, error) {
	logger.Debugf("Entering nextKey")
	if scanner.currentKeyItr != nil {
		scanner.currentKeyItr.Release()
	}
	scanner.keyIndex++
	if scanner.keyIndex >= len(scanner.keys) {
		return false, nil
	}
	key := scanner.keys[scanner.keyIndex]
	scanner.currentRangeScan = constructRangeScan(scanner.namespace, key)
	var err error
	scanner.currentKeyItr, err = scanner.levelDB.GetIterator(scanner.currentRangeScan.startKey, scanner.currentRangeScan.endKey)
	if err != nil {
		return false, err
	}
	for scanner.currentKeyItr.Next() {
		historyKey := scanner.currentKeyItr.Key()
		blockNum, _, err := scanner.currentRangeScan.decodeBlockNumTranNum(historyKey)
		if err != nil {
			return false, err
		}
		if blockNum >= scanner.start {
			// Move the iterator back so first call to Next() retrieves correct value
			scanner.currentKeyItr.Prev()
			return true, nil
		}
	}
	// Shouldn't ever reach this line as we precheck that all keys are within block range
	return scanner.nextKey()
}

// getTxIDandKeyWriteValueFromTran inspects a transaction for writes to a given key
func countKeyUpdatesForTran(tranEnvelope *common.Envelope, namespace string, keys map[string]int) error {
	logger.Debugf("Entering getKeysFromTran %s", namespace)

	// extract action from the envelope
	payload, err := protoutil.UnmarshalPayload(tranEnvelope.Payload)
	if err != nil {
		return err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return err
	}

	_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
	if err != nil {
		return err
	}

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return err
	}

	// Read the keys by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				keys[kvWrite.Key] += 1
			} // end keys loop
			return nil
		} // end if
	} //end namespaces loop
	logger.Debugf("namespace [%s] not found in transaction's ReadWriteSets", namespace)
	return nil
}

func (scanner *blockRangeScanner) getNextBlockTran() (uint64, uint64, bool, error) {
	if scanner.currentKeyItr == nil {
		// Indicates no keys meet criteria
		return 0, 0, true, nil
	}
	hasNextIndex := scanner.currentKeyItr.Next()
	if hasNextIndex {
		historyKey := scanner.currentKeyItr.Key()
		blockNum, tranNum, err := scanner.currentRangeScan.decodeBlockNumTranNum(historyKey)
		if err != nil {
			return 0, 0, false, err
		}
		if blockNum <= scanner.end {
			return blockNum, tranNum, false, nil
		}
	}
	// Current key iterator exhausted
	hasNextKey, err := scanner.nextKey()
	if err != nil {
		return 0, 0, false, err
	}
	if !hasNextKey {
		return 0, 0, true, nil
	}
	return scanner.getNextBlockTran()
}
