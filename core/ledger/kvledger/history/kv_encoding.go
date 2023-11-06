/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric/common/ledger/util"
)

type dataKey []byte
type newIndex []byte
type globalIndex []byte

type rangeScan struct {
	startKey, endKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
)

func constructNewIndex(prev uint64, numVersions uint64, transactions []uint64) newIndex {
	var k []byte
	k = append(k, util.EncodeOrderPreservingVarUint64(prev)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(numVersions)...)
	for _, tx := range transactions {
		k = append(k, util.EncodeOrderPreservingVarUint64(tx)...)
	}
	return newIndex(k)
}

func decodeNewIndex(newIndex newIndex) (uint64, uint64, []uint64, error) {
	prev, prevBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndex)
	if err != nil {
		return 0, 0, nil, err
	}
	numVersions, versionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndex[prevBytesConsumed:])
	if err != nil {
		return 0, 0, nil, err
	}
	var transactions []uint64
	currentTxStart := prevBytesConsumed + versionBytesConsumed
	var lastTxBytesConsumed int
	for i := currentTxStart; i < len(newIndex); i += lastTxBytesConsumed {
		tx, bytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndex[currentTxStart:])
		lastTxBytesConsumed = bytesConsumed
		currentTxStart += bytesConsumed
		if err != nil {
			return 0, 0, nil, err
		}
		transactions = append(transactions, tx)
	}
	return prev, numVersions, transactions, nil
}

func constructDataKey(ns string, blocknum uint64, key string) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(blocknum)...)
	k = append(k, compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	return dataKey(k)
}
