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
type newIndex []byte
type globalIndex []byte

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

// namespace~SEP~block~SEP~len(key)~key
func decodeDataKey(namespace string, dataKey dataKey) (uint64, string, error) {
	var bytesConsumed int
	startKey := append([]byte(namespace), compositeKeySep...)
	blockNumKeyBytes := bytes.TrimPrefix(dataKey, startKey)

	blockNum, blockNumBytesConsumed, err := util.DecodeOrderPreservingVarUint64(blockNumKeyBytes)
	if err != nil {
		return 0, "", err
	}
	// Extra 1 is added for separator byte
	bytesConsumed += blockNumBytesConsumed + 1

	keyLen, keyLenBytesConsumed, err := util.DecodeOrderPreservingVarUint64(blockNumKeyBytes[bytesConsumed:])
	if err != nil {
		return 0, "", err
	}
	bytesConsumed += keyLenBytesConsumed

	key := string(blockNumKeyBytes[bytesConsumed : bytesConsumed+int(keyLen)])
	// keyLen bytes are used to store the key
	bytesConsumed += int(keyLen)

	if bytesConsumed != len(blockNumKeyBytes) {
		return 0, "", errors.Errorf("number of decoded bytes (%d) is not equal to the length of blockNumKeyBytes (%d)",
			bytesConsumed, len(blockNumKeyBytes))
	}

	return blockNum, key, nil
}
