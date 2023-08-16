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
	levelDB     *leveldbhelper.DBHandle
	blockStore  *blkstorage.BlockStore
	globalIndex map[string][]byte
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	dbItr, err := q.levelDB.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}

	globalIndexBytes, present := q.globalIndex[key]
	if !present {
		logger.Debugf("Key not present in GI. Initialized nil history scanner for key %s.", key)
		// This scanner will return nil upon first call to Next()
		return &historyScanner{namespace, key, dbItr, q.blockStore, 0, 0, nil, -1}, nil
	}

	blockNum, _, err := decodeGlobalIndex(globalIndexBytes)
	if err != nil {
		return nil, err
	}

	historyKey := constructDataKey(namespace, blockNum, key)
	found := dbItr.Seek(historyKey)
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
		scanner.updateBlock()
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
	logger.Debugf("Updating block for key %s. currentBlock %d, previousBlock %d", scanner.key, scanner.currentBlock, scanner.previousBlock)
	scanner.currentBlock = scanner.previousBlock
	historyKey := constructDataKey(scanner.namespace, scanner.previousBlock, scanner.key)
	found := scanner.dbItr.Seek(historyKey)
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
	keyMap := make(map[string]keyData)
	validKeys := []string{}
	for _, key := range keys {
		dbItr, err := q.levelDB.GetIterator(nil, nil)

		globalIndexBytes, present := q.globalIndex[key]
		if !present {
			continue
		} // Else key is valid & we add it to the collection
		validKeys = append(validKeys, key)
	
		blockNum, _, err := decodeGlobalIndex(globalIndexBytes)
		if err != nil {
			return nil, err
		}
	
		historyKey := constructDataKey(namespace, blockNum, key)
		dbItr.Seek(historyKey)

		indexVal := dbItr.Value()
		prev, _, transactions, err := decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		txIndex := len(transactions) - 1

		keyMap[key] = keyData{rangeScan, dbItr, blockNum, prev, transactions, txIndex}

	}
	scanner := &parallelHistoryScanner{namespace, validKeys, keyMap, q.blockStore, nil, 0, nil, 0}
	err := scanner.nextBlock()
	if err != nil {
		return nil, err
	}
	return scanner, nil
}

type keyData struct {
	dbItr        iterator.Iterator
	currentBlock  uint64
	previousBlock uint64
	transactions []uint64
	txIndex      int
	isExhausted		bool
}

// historyScanner implements ResultsIterator for iterating through history results
type parallelHistoryScanner struct {
	namespace       string
	keys            []string
	keyMap          map[string]keyData
	blockStore      *blkstorage.BlockStore
	currentBlockContents    *common.Block
	currentBlockNum uint64
	keysInBlock     []string
	currentKeyIndex int
}

func (scanner *parallelHistoryScanner) Next() (commonledger.QueryResult, error) {
	// No keys in next block indicates we've exhausted the iterators
	if len(scanner.keysInBlock) == 0 {
		return nil, nil
	}

	key := scanner.keysInBlock[scanner.currentKeyIndex]
	blockNum := scanner.currentBlockNum
	tranNum := scanner.keyMap[key].transactions[scanner.keyMap[key].txIndex]

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, key, blockNum, tranNum)

	// Index into stored block & get the tranEnvelope
	txEnvelopeBytes := scanner.currentBlock.Data.Data[tranNum]
	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
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

	logger.Debugf("Completed scanner.Next(), updating position trackers: tranNum: %v, txIndex: %v, keyIndex: %v", scanner.keyMap[key].transactions[scanner.keyMap[key].txIndex], scanner.keyMap[key].txIndex, scanner.currentKeyIndex)

	keyData := scanner.keyMap[key]
	keyData.txIndex--
	scanner.keyMap[key] = keyData
	// Update position trackers
	if scanner.keyMap[key].txIndex <= -1 {
		err := scanner.updateKeyData(key)
		if err != nil {
			return nil, nil
		}
		scanner.currentKeyIndex++
		if scanner.currentKeyIndex >= len(scanner.keysInBlock) {
			err := scanner.nextBlock()
			if err != nil {
				return nil, nil
			}
		}
	}

	return queryResult, nil
}

func (scanner *parallelHistoryScanner) Close() {
	for _, key := range scanner.keys {
		scanner.keyMap[key].dbItr.Release()
	}
}

func (scanner *parallelHistoryScanner) nextBlock() error {
	scanner.currentBlock = 0
	scanner.keysInBlock = []string{}
	for _, key := range scanner.keys {
		keyData := scanner.keyMap[key]
		if keyData.isExhausted {	
			continue
		}
		blockNum := keyData.currentBlock
		if blockNum > scanner.currentBlockNum {
			scanner.currentBlockNum = blockNum
			scanner.keysInBlock = append([]string{}, key)
		} else if blockNum == scanner.currentBlockNum {
			scanner.keysInBlock = append(scanner.keysInBlock, key)
		}
	}
	if len(scanner.keysInBlock) > 0 {
		block, err := scanner.blockStore.RetrieveBlockByNumber(scanner.currentBlockNum)
		scanner.currentBlock = block
		if err != nil {
			return err
		}
	}
	scanner.currentKeyIndex = 0
	logger.Debugf("Completed scanner.nextBlock: currentBlock: %v, keyIndex: %v, keysInBlock: %v", scanner.currentBlockNum, scanner.currentKeyIndex, scanner.keysInBlock)
	return nil
}

func (scanner *parallelHistoryScanner) updateKeyData(keyData keyData) error {
	if (keyData.currentBlock == keyData.previousBlock) {
		keyData.isExhausted = true
		scanner.keyMap[key] = keyData
		return nil
	}
	nextBlockNum := keyData.previousBlock
	historyKey := constructDataKey(scanner.namespace, nextBlockNum, scanner.key)
	keyData.dbItr.Seek(historyKey)
	indexVal := scanner.dbItr.Value()
	prev, _, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return err
	}
	keyData.currentBlock = nextBlockNum
	keyData.previousBlock = prev
	keyData.transactions = transactions
	keyData.txIndex = len(transactions) - 1
	scanner.keyMap[key] = keyData
	return nil
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

	globalIndexBytes, present := q.globalIndex[key]
	if !present {
		logger.Debugf("Key not present in GI. Initialized nil version scanner for key %s.", key)
		// This scanner will return nil upon first call to Next()
		return &versionScanner{namespace, key, dbItr, q.blockStore, 0, nil, -1, start, end}, nil
	}

	blockNum, _, err := decodeGlobalIndex(globalIndexBytes)
	if err != nil {
		return nil, err
	}

	historyKey := constructDataKey(namespace, blockNum, key)
	found := dbItr.Seek(historyKey)
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
		scanner.updateBlock()
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
		scanner.updateBlock()
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
	historyKey := constructDataKey(scanner.namespace, prev, scanner.key)
	found := scanner.dbItr.Seek(historyKey)
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
