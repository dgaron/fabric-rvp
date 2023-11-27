/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric/common/ledger/util"
)

type dataKey []byte
type blockList []byte
type txList []byte
type rangeScan struct {
	blockKey, txKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
	emptyValue      = []byte{}     // used to store as value for keys where only key needs to be stored (e.g., dataKeys)
)

func constructBlockKey(ns string, key string) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)
	k = append(k, []byte("b")...)
	return dataKey(k)
}

func constructTxKey(ns string, key string) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)
	k = append(k, []byte("t")...)
	return dataKey(k)
}

// blockKey = namespace~len(key)~key~b
// txKey = namespace~len(key)~key~t
func constructRangeScan(ns string, key string) *rangeScan {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)

	return &rangeScan{
		blockKey: append(k, []byte("b")...),
		txKey:    append(k, []byte("t")...),
	}
}

func constructBlockList(blocks []uint64) blockList {
	var bl []byte
	for _, block := range blocks {
		bl = append(bl, util.EncodeOrderPreservingVarUint64(block)...)
	}
	return blockList(bl)
}

func decodeBlockList(encodedBlockList blockList) ([]uint64, error) {
	var blockList []uint64
	var totalBytesConsumed int
	for i := 0; i < len(encodedBlockList); i += totalBytesConsumed {
		blockNum, bytesConsumed, err := util.DecodeOrderPreservingVarUint64(encodedBlockList[totalBytesConsumed:])
		totalBytesConsumed += bytesConsumed
		if err != nil {
			return nil, err
		}
		blockList = append(blockList, blockNum)
	}
	return blockList, nil
}

func constructTxList(transactionLists [][]uint64) txList {
	var tl []byte
	var sep []byte
	for _, transactions := range transactionLists {
		tl = append(tl, sep...)
		for _, tx := range transactions {
			tl = append(tl, util.EncodeOrderPreservingVarUint64(tx)...)
		}
		sep = compositeKeySep
	}
	return txList(tl)
}

func decodeTxList(encodedTxList txList) ([][]uint64, error) {
	var (
		txList             [][]uint64
		totalBytesConsumed int
		transactions       []uint64
	)
	for i := 0; i < len(encodedTxList); i += totalBytesConsumed {
		// Check for separator, indicating next transactions list
		// Using a literal because compositeKeySep is a []byte and compositeKeySep[0] wasn't super clear
		if encodedTxList[totalBytesConsumed] == 0x00 {
			txList = append(txList, transactions)
			transactions = []uint64{}
		}
		tx, bytesConsumed, err := util.DecodeOrderPreservingVarUint64(encodedTxList[totalBytesConsumed:])
		totalBytesConsumed += bytesConsumed
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}
	return txList, nil
}
