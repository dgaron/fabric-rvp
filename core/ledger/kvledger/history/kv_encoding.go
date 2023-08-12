/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pkg/errors"
)

type dataKey []byte
type globalIndex []byte

type rangeScan struct {
	startKey, endKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
	emptyValue		= []byte{}
)

// DataKey: ns~blockNum~len(key)~key~prev~numVersions~[]transactions
func constructDataKey(ns string, blocknum uint64, key string, prev uint64, numVersions uint64, transactions []uint64) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(blocknum)...)
	k = append(k, compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(prev)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(numVersions)...)
	for _, tx := range transactions {
		k = append(k, util.EncodeOrderPreservingVarUint64(tx)...)
	}
	return dataKey(k)
}

func constructRangeScan(ns string) *rangeScan {
	k := append([]byte(ns), compositeKeySep...)

	return &rangeScan{
		startKey: k,
		endKey:   append(k, 0xff),
	}
}

// Returns blockNum, prev, numVersions, []transactions
func (r *rangeScan) decodeNewIndex(dataKey dataKey) (uint64, uint64, uint64, []uint64, error) {
	var totalBytesConsumed int
	newIndexBytes := bytes.TrimPrefix(dataKey, r.startKey)
	blockNum, blockNumBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndexBytes)
	if err != nil {
		return 0, 0, nil, err
	}
	totalBytesConsumed += blockNumBytesConsumed

	keyLen, keyLenBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndexBytes[totalBytesConsumed:])
	if err != nil {
		return 0, 0, nil, err
	}
	totalBytesConsumed += keyLenBytesConsumed

	// Increment placeholder by number of bytes used for key
	totalBytesConsumed += int(keyLen)

	prev, prevBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndexBytes[totalBytesConsumed:])
	if err != nil {
		return 0, 0, nil, err
	}
	totalBytesConsumed += prevBytesConsumed

	numVersions, versionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndexBytes[totalBytesConsumed:])
	if err != nil {
		return 0, 0, nil, err
	}
	totalBytesConsumed += versionBytesConsumed

	var transactions []uint64
	currentTxStart := totalBytesConsumed
	var lastTxBytesConsumed int
	for i := currentTxStart; i < len(newIndexBytes); i += lastTxBytesConsumed {
		tx, bytesConsumed, err := util.DecodeOrderPreservingVarUint64(newIndexBytes[currentTxStart:])
		lastTxBytesConsumed = bytesConsumed
		currentTxStart += bytesConsumed
		if err != nil {
			return 0, 0, nil, err
		}
		transactions = append(transactions, tx)
	}
	return blockNum, prev, numVersions, transactions, nil
}

func constructGlobalIndex(prev uint64, numVersions uint64) globalIndex {
	var k []byte
	k = append(k, util.EncodeOrderPreservingVarUint64(prev)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(numVersions)...)
	return globalIndex(k)
}

func decodeGlobalIndex(globalIndex globalIndex) (uint64, uint64, error) {
	prev, prevBytesConsumed, err := util.DecodeOrderPreservingVarUint64(globalIndex)
	if err != nil {
		return 0, 0, err
	}
	numVersions, versionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(globalIndex[prevBytesConsumed:])
	if err != nil {
		return 0, 0, err
	}
	// The following error should never happen. Keep the check just in case there is some unknown bug.
	if prevBytesConsumed+versionBytesConsumed != len(globalIndex) {
		return 0, 0, errors.Errorf("number of decoded bytes (%d) is not equal to the length of blockNumTranNumBytes (%d)",
			prevBytesConsumed+versionBytesConsumed, len(globalIndex))
	}
	return prev, numVersions, nil
}
