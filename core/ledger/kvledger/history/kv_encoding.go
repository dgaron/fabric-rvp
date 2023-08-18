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
type versionScan struct {
	prefix, startKey, endKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
	emptyValue      = []byte{}     // used to store as value for keys where only key needs to be stored (e.g., dataKeys)
)

// constructDataKey builds the key of the format namespace~len(key)~key~versionnum
// using an order preserving encoding so that history query results are ordered by height
// Note: this key format is different than the format in pre-v2.0 releases and requires
//
//	a historydb rebuild when upgrading an older version to v2.0.
func constructDataKey(ns string, key string, versionnum uint64, blocknum uint64, trannum uint64) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(versionnum)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(blocknum)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(trannum)...)
	return dataKey(k)
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

func constructVersionScan(ns string, key string, start uint64, end uint64) *versionScan {
	p := append([]byte(ns), compositeKeySep...)
	p = append(p, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	p = append(p, []byte(key)...)
	p = append(p, compositeKeySep...)

	s := append(p, util.EncodeOrderPreservingVarUint64(start)...)

	e := append(p, util.EncodeOrderPreservingVarUint64(end+1)...)

	return &versionScan{
		prefix:	  p,
		startKey: s,
		endKey:   e,
	}
}

func (r *rangeScan) decodeVersionBlockTran(dataKey dataKey) (uint64, uint64, uint64, error) {
	versionBlockTranBytes := bytes.TrimPrefix(dataKey, r.startKey)
	versionNum, versionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes)
	if err != nil {
		return 0, 0, 0, err
	}
	blockNum, blockBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes[versionBytesConsumed:])
	if err != nil {
		return 0, 0, 0, err
	}
	tranNum, tranBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes[blockBytesConsumed+versionBytesConsumed:])
	if err != nil {
		return 0, 0, 0, err
	}
	// The following error should never happen. Keep the check just in case there is some unknown bug.
	if versionBytesConsumed+blockBytesConsumed+tranBytesConsumed != len(versionBlockTranBytes) {
		return 0, 0, 0, errors.Errorf("number of decoded bytes (%d) is not equal to the length of blockNumTranNumBytes (%d)",
			blockBytesConsumed+tranBytesConsumed, len(versionBlockTranBytes))
	}

	return versionNum, blockNum, tranNum, nil
}

func (v *versionScan) decodeVersionBlockTran(dataKey dataKey) (uint64, uint64, uint64, error) {
	versionBlockTranBytes := bytes.TrimPrefix(dataKey, v.prefix)
	versionNum, versionBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes)
	if err != nil {
		return 0, 0, 0, err
	}
	blockNum, blockBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes[versionBytesConsumed:])
	if err != nil {
		return 0, 0, 0, err
	}
	tranNum, tranBytesConsumed, err := util.DecodeOrderPreservingVarUint64(versionBlockTranBytes[blockBytesConsumed+versionBytesConsumed:])
	if err != nil {
		return 0, 0, 0, err
	}
	// The following error should never happen. Keep the check just in case there is some unknown bug.
	if versionBytesConsumed+blockBytesConsumed+tranBytesConsumed != len(versionBlockTranBytes) {
		return 0, 0, 0, errors.Errorf("number of decoded bytes (%d) is not equal to the length of blockNumTranNumBytes (%d)",
			blockBytesConsumed+tranBytesConsumed, len(versionBlockTranBytes))
	}

	return versionNum, blockNum, tranNum, nil
}
