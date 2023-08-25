/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"bytes"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pkg/errors"
)

type dataKey []byte
type rangeScan struct {
	startKey, endKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
	emptyValue      = []byte{}     // used to store as value for keys where only key needs to be stored (e.g., dataKeys)
)

// constructDataKey builds the key of the format namespace~len(key)~key~minVersion
// using an order preserving encoding so that history query results are ordered by height
// Note: this key format is different than the format in pre-v2.0 releases and requires
//
//	a historydb rebuild when upgrading an older version to v2.0.
func constructDataKey(ns string, key string, minVersion uint64) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(minVersion)...)
	return dataKey(k)
}

func constructNewIndex(blockNum uint64, transactions []uint64) newIndex {
	var ni []byte
	ni = append(ni, util.EncodeOrderPreservingVarUint64(blocniNum)...)
	for _, tx := range transactions {
		ni = append(ni, util.EncodeOrderPreservingVarUint64(tx)...)
	}
	return newIndex(ni)
}

func decodeNewIndex(newIndex newIndex) (uint64, []uint64, error) {
	blockNum, blockNumBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndex)
	if err != nil {
		return 0, nil, err
	}
	var transactions []uint64
	currentTxStart := blockNumBytesConsumed
	var lastTxBytesConsumed int
	for i := currentTxStart; i < len(newIndex); i += lastTxBytesConsumed {
		tx, bytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndex[currentTxStart:])
		lastTxBytesConsumed = bytesConsumed
		currentTxStart += bytesConsumed
		if err != nil {
			return 0, nil, err
		}
		transactions = append(transactions, tx)
	}
	return blockNum, transactions, nil
}


// constructRangescanKeys returns start and endKey for performing a range scan
// that covers all the keys for <ns, key>.
// startKey = namespace~len(key)~key~
// endKey = namespace~len(key)~key~0xff
func constructRangeScan(ns string, key string) *rangeScan {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)

	return &rangeScan{
		startKey: k,
		endKey:   append(k, 0xff),
	}
}

func (r *rangeScan) decodeMinVersion(dataKey dataKey) (uint64, error) {
	minVersionBytes := bytes.TrimPrefix(dataKey, r.startKey)
	minVersion, minVersionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(minVersionBytes)
	if err != nil {
		return 0, err
	}

	return minVersion, nil
}