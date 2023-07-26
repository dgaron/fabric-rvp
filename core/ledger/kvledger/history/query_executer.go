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
	return &historyScanner{rangeScan, namespace, key, dbItr, q.blockStore, nil, nil, -1}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	rangeScan    *rangeScan
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	currentBlock *common.Block
	transactions []uint64
	txIndex      int
}

// Next iterates to the next key, in the order of newest to oldest, from history scanner.
// It decodes blockNumTranNumBytes to get blockNum and tranNum,
// loads the block:tran from block storage, finds the key and returns the result.
func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	// call Prev because history query result is returned from newest to oldest
	if scanner.txIndex <= -1 && !scanner.dbItr.Prev() {
		return nil, nil
	}

	historyKey := scanner.dbItr.Key()
	blockNum, err := scanner.rangeScan.decodeBlockNum(historyKey)
	if err != nil {
		return nil, err
	}
	if scanner.txIndex <= -1 {
		// Retrieve new block
		scanner.currentBlock, err = scanner.blockStore.RetrieveBlockByNumber(blockNum)
		if err != nil {
			return nil, err
		}
		indexVal := scanner.dbItr.Value()
		_, _, scanner.transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		scanner.txIndex = len(scanner.transactions) - 1
	}
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex--

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, scanner.key, blockNum, tranNum)

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
		if err == nil {
			dbItr.Last()
			keyMap[key] = keyData{rangeScan, dbItr, nil, -1}
			validKeys = append(validKeys, key)
		}
	}
	scanner := &parallelHistoryScanner{namespace, validKeys, keyMap, q.blockStore, nil, 0, nil, 0}
	err := scanner.nextBlock()
	if err != nil {
		return nil, err
	}
	return scanner, nil
}

type keyData struct {
	rangeScan    *rangeScan
	dbItr        iterator.Iterator
	transactions []uint64
	txIndex      int
}

// historyScanner implements ResultsIterator for iterating through history results
type parallelHistoryScanner struct {
	namespace       string
	keys            []string
	keyMap          map[string]keyData
	blockStore      *blkstorage.BlockStore
	currentBlock    *common.Block
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
		scanner.keyMap[key].dbItr.Prev()
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
	scanner.currentBlockNum = 0
	scanner.keysInBlock = []string{}

	for _, key := range scanner.keys {
		currentIndexVal := scanner.keyMap[key].dbItr.Value()
		if currentIndexVal != nil {

			historyKey := scanner.keyMap[key].dbItr.Key()
			blockNum, err := scanner.keyMap[key].rangeScan.decodeBlockNum(historyKey)
			if err != nil {
				return err
			}

			_, _, transactions, err := decodeNewIndex(currentIndexVal)
			if err != nil {
				return err
			}

			keyData := scanner.keyMap[key]
			keyData.transactions = transactions
			keyData.txIndex = len(transactions) - 1
			scanner.keyMap[key] = keyData

			if blockNum > scanner.currentBlockNum {
				scanner.currentBlockNum = blockNum
				scanner.keysInBlock = append([]string{}, key)
			} else if blockNum == scanner.currentBlockNum {
				scanner.keysInBlock = append(scanner.keysInBlock, key)
			}
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

type versionScanner struct {
	rangeScan     *rangeScan
	namespace     string
	key           string
	dbItr         iterator.Iterator
	blockStore    *blkstorage.BlockStore
	numVersions   uint64
	targetVersion uint64
}

func (q *QueryExecutor) GetVersionForKey(namespace string, key string, version uint64) (commonledger.ResultsIterator, error) {
	rangeScan := constructRangeScan(namespace, key)
	dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
	if err != nil {
		return nil, err
	}
	return &versionScanner{rangeScan, namespace, key, dbItr, q.blockStore, 0, version}, nil
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	for {
		if !scanner.dbItr.Next() {
			return nil, nil
		}
		indexVal := scanner.dbItr.Value()
		var (
			err          error
			transactions []uint64
		)
		_, scanner.numVersions, transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		if scanner.numVersions >= scanner.targetVersion {
			firstVersionInBlock := scanner.numVersions - uint64(len(transactions))
			txIndex := scanner.targetVersion - firstVersionInBlock
			tranNum := transactions[txIndex]

			historyKey := scanner.dbItr.Key()
			blockNum, err := scanner.rangeScan.decodeBlockNum(historyKey)
			if err != nil {
				return nil, err
			}
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
				logger.Errorf("Version %d not found for key %s for namespace:%s", scanner.targetVersion, scanner.key, scanner.namespace)
				return nil, errors.Errorf("Version %d not found for key %s for namespace:%s", scanner.targetVersion, scanner.key, scanner.namespace)
			}

			logger.Debugf("Found key version %d for namespace:%s key:%s from transaction %s", scanner.targetVersion, scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
			return queryResult, nil
		}
	}
}

func (scanner *versionScanner) Close() {
	scanner.dbItr.Release()
}
