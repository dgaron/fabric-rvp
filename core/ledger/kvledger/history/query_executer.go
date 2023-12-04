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
)

// QueryExecutor is a query executor against the LevelDB history DB
type QueryExecutor struct {
	levelDB    *leveldbhelper.DBHandle
	blockStore *blkstorage.BlockStore
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	rangeScan := constructRangeScan(namespace, key)
	encodedBlockList, err := q.levelDB.Get(rangeScan.blockKey)
	if err != nil {
		return nil, err
	}
	if encodedBlockList == nil {
		return &historyScanner{rangeScan, namespace, key, nil, nil, -1, -1, q.blockStore}, nil
	}
	encodedTxList, err := q.levelDB.Get(rangeScan.txKey)
	if err != nil {
		return nil, err
	}
	blockList, err := decodeBlockList(encodedBlockList)
	if err != nil {
		return nil, err
	}
	txList, err := decodeTxList(encodedTxList)
	if err != nil {
		return nil, err
	}
	blockIndex := len(blockList) - 1
	txIndex := len(txList[blockIndex]) - 1

	return &historyScanner{rangeScan, namespace, key, blockList, txList, blockIndex, txIndex, q.blockStore}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	rangeScan  *rangeScan
	namespace  string
	key        string
	blockList  []uint64
	txList     [][]uint64
	blockIndex int
	txIndex    int
	blockStore *blkstorage.BlockStore
}

func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex <= -1 {
		scanner.blockIndex--
		if scanner.blockIndex <= -1 {
			return nil, nil
		}
		scanner.txIndex = len(scanner.txList[scanner.blockIndex]) - 1
	}

	blockNum := scanner.blockList[scanner.blockIndex]
	tranNum := scanner.txList[scanner.blockIndex][scanner.txIndex]

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
	// Intentionally empty
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
	// Intentionally empty
}

type versionScanner struct {
	rangeScan  *rangeScan
	namespace  string
	key        string
	blockList  []uint64
	txList     [][]uint64
	blockIndex int
	txIndex    int
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
	encodedBlockList, err := q.levelDB.Get(rangeScan.blockKey)
	if err != nil {
		return nil, err
	}
	encodedTxList, err := q.levelDB.Get(rangeScan.txKey)
	if err != nil {
		return nil, err
	}
	blockList, err := decodeBlockList(encodedBlockList)
	if err != nil {
		return nil, err
	}
	txList, err := decodeTxList(encodedTxList)
	if err != nil {
		return nil, err
	}
	var (
		current    uint64
		blockIndex int
		txIndex    int
	)

	transactions := txList[blockIndex]

	for current < start {
		if start > current+uint64(len(transactions)) {
			current += uint64(len(transactions))
			blockIndex++
			transactions = txList[blockIndex]
		} else {
			txIndex = int(start - current - 1)
			current = start
		}
	}

	return &versionScanner{rangeScan, namespace, key, blockList, txList, blockIndex, txIndex, q.blockStore, current, start, end}, nil
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	if scanner.current > scanner.end {
		return nil, nil
	}
	if scanner.blockIndex >= len(scanner.blockList) {
		return nil, nil
	}

	blockNum := scanner.blockList[scanner.blockIndex]
	tranNum := scanner.txList[scanner.blockIndex][scanner.txIndex]

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

	scanner.current++
	scanner.txIndex++
	if scanner.txIndex >= len(scanner.txList[scanner.blockIndex]) {
		scanner.blockIndex++
		scanner.txIndex = 0
	}
	return queryResult, nil
}

func (scanner *versionScanner) Close() {
	// Intentionally empty
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
