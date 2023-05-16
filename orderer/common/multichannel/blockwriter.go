/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	newchannelconfig "github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/willf/bloom"
	"log"
	"sync"
)

type blockWriterSupport interface {
	identity.SignerSerializer
	blockledger.ReadWriter
	configtx.Validator
	Update(*newchannelconfig.Bundle)
	CreateBundle(channelID string, config *cb.Config) (*newchannelconfig.Bundle, error)
	SharedConfig() newchannelconfig.Orderer
}

// BlockWriter efficiently writes the blockchain to disk.
// To safely use BlockWriter, only one thread should interact with it.
// BlockWriter will spawn additional committing go routines and handle locking
// so that these other go routines safely interact with the calling one.
type BlockWriter struct {
	support            blockWriterSupport
	registrar          *Registrar
	lastConfigBlockNum uint64
	lastConfigSeq      uint64
	lastBlock          *cb.Block
	committingBlock    sync.Mutex
}

func newBlockWriter(lastBlock *cb.Block, r *Registrar, support blockWriterSupport) *BlockWriter {
	bw := &BlockWriter{
		support:       support,
		lastConfigSeq: support.Sequence(),
		lastBlock:     lastBlock,
		registrar:     r,
	}

	// If this is the genesis block, the lastconfig field may be empty, and, the last config block is necessarily block 0
	// so no need to initialize lastConfig
	if lastBlock.Header.Number != 0 {
		var err error
		bw.lastConfigBlockNum, err = protoutil.GetLastConfigIndexFromBlock(lastBlock)
		if err != nil {
			logger.Panicf("[channel: %s] Error extracting last config block from block metadata: %s", support.ChannelID(), err)
		}
	}

	logger.Debugf("[channel: %s] Creating block writer for tip of chain (blockNumber=%d, lastConfigBlockNum=%d, lastConfigSeq=%d)", support.ChannelID(), lastBlock.Header.Number, bw.lastConfigBlockNum, bw.lastConfigSeq)
	return bw
}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (bw *BlockWriter) CreateNextBlock(messages []*cb.Envelope) *cb.Block {
	previousBlockHash := protoutil.BlockHeaderHash(bw.lastBlock.Header)

	data := &cb.BlockData{
		Data: make([][]byte, len(messages)),
	}

	var err error
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		if err != nil {
			logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	block := protoutil.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data

	// ==================================================
	// New Part Starts Here
	// ==================================================

	// bloomFilter := make([]byte, 16*1024) // 16Kb of zeros

	bloomFilter := bloom.New(16*8*1024, 25)

	// Iterate through the messages in the block
	for _, msg := range messages {
		payload, err := protoutil.UnmarshalPayload(msg.Payload)
		if err != nil {
			logger.Panicf("Could not unmarshal payload: %s", err)
		}

		// Extract the keys from the transaction
		keys, _ := extractKeysFromTransaction(payload.Data)

		// Hash Functions
		// numHashFuncs := 22
		// numBits := len(bloomFilter) * 8
		// hashFuncs := generateHashFuncs(numHashFuncs)

		for _, key := range keys {
			// keyBytes := []byte(key)
			// for _, hashFunc := range hashFuncs {
			// index := hashFunc(keyBytes) % uint32(numBits)
			// byteIndex := index / 8
			// bitIndex := index % 8
			// bloomFilter[byteIndex] |= 1 << bitIndex
			// }
			bloomFilter.Add([]byte(key))

		}
	}
	// Convert the bloom filter to a byte array
	var buf bytes.Buffer
	_, err = bloomFilter.WriteTo(&buf)
	if err != nil {
		log.Fatalf("Error while serializing bloom filter: %v", err)
	}
	serializedBloomFilter := buf.Bytes()

	block.Metadata.Metadata = append(block.Metadata.Metadata, serializedBloomFilter)

	return block
}

// Custom Function for Extracting Keys from Transaction
func extractKeysFromTransaction(txPayloadData []byte) ([]string, error) {
	var keys []string

	payload, err := protoutil.UnmarshalPayload(txPayloadData)
	if err != nil {
		return nil, err
	}

	txEnvelope, err := protoutil.UnmarshalEnvelope(payload.Data)
	if err != nil {
		return nil, err
	}

	tx, err := protoutil.UnmarshalTransaction(txEnvelope.Payload)
	if err != nil {
		return nil, err
	}

	actions := tx.GetActions()

	for _, action := range actions {
		ccActionPayload, err := protoutil.UnmarshalChaincodeActionPayload(action.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal chaincode action payload: %v", err)
		}

		propRespPayload, err := protoutil.UnmarshalProposalResponsePayload(ccActionPayload.Action.ProposalResponsePayload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal proposal response payload: %v", err)
		}

		caPayload, err := protoutil.UnmarshalChaincodeAction(propRespPayload.Extension)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal chaincode actions: %v", err)
		}

		txRWSet := &rwsetutil.TxRwSet{}
		if err = txRWSet.FromProtoBytes(caPayload.Results); err != nil {
			return nil, fmt.Errorf("failed to unmarshal read-write set: %v", err)
		}

		for _, nsRWSet := range txRWSet.NsRwSets {
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				keys = append(keys, kvWrite.Key)
			}
		}
	}

	return keys, nil
}

// Custom function to produce hash functions
/*
func generateHashFuncs(numHashFuncs int) []func([]byte) uint32 {
	hashFuncs := make([]func([]byte) uint32, numHashFuncs)

	for i := 0; i < numHashFuncs; i++ {
		seed1 := generateRandomUint32()
		seed2 := generateRandomUint32()

		hasher1 := fnv.New32a()
		hasher2 := fnv.New32a()

		hashFuncs[i] = func(data []byte) uint32 {
			hasher1.Reset()
			hasher2.Reset()

			hasher1.Write(data)
			hasher2.Write(data)

			h1 := hasher1.Sum32() ^ seed1
			h2 := hasher2.Sum32() ^ seed2

			return h1 ^ h2
		}
	}

	return hashFuncs
}

func generateRandomUint32() uint32 {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint32(buf[:])
}
*/

// WriteConfigBlock should be invoked for blocks which contain a config transaction.
// This call will block until the new config has taken effect, then will return
// while the block is written asynchronously to disk.
func (bw *BlockWriter) WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte) {
	ctx, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		logger.Panicf("Told to write a config block, but could not get configtx: %s", err)
	}

	payload, err := protoutil.UnmarshalPayload(ctx.Payload)
	if err != nil {
		logger.Panicf("Told to write a config block, but configtx payload is invalid: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Told to write a config block, but configtx payload header is missing")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Told to write a config block with an invalid channel header: %s", err)
	}

	switch chdr.Type {
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		newChannelConfig, err := protoutil.UnmarshalEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config update embedded: %s", err)
		}
		bw.registrar.newChain(newChannelConfig)

	case int32(cb.HeaderType_CONFIG):
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config envelope encoded: %s", err)
		}

		err = bw.support.Validate(configEnvelope)
		if err != nil {
			logger.Panicf("Told to write a config block with new config, but could not apply it: %s", err)
		}

		bundle, err := bw.support.CreateBundle(chdr.ChannelId, configEnvelope.Config)
		if err != nil {
			logger.Panicf("Told to write a config block with a new config, but could not convert it to a bundle: %s", err)
		}

		oc, ok := bundle.OrdererConfig()
		if !ok {
			logger.Panicf("[channel: %s] OrdererConfig missing from bundle", bw.support.ChannelID())
		}

		currentType := bw.support.SharedConfig().ConsensusType()
		nextType := oc.ConsensusType()
		if currentType != nextType {
			encodedMetadataValue = nil
			logger.Debugf("[channel: %s] Consensus-type migration: maintenance mode, change from %s to %s, setting metadata to nil",
				bw.support.ChannelID(), currentType, nextType)
		}

		// Avoid Bundle update before the go-routine in WriteBlock() finished writing the previous block.
		// We do this (in particular) to prevent bw.support.Sequence() from advancing before the go-routine reads it.
		// In general, this prevents the StableBundle from changing before the go-routine in WriteBlock() finishes.
		bw.committingBlock.Lock()
		bw.committingBlock.Unlock()
		bw.support.Update(bundle)
	default:
		logger.Panicf("Told to write a config block with unknown header type: %v", chdr.Type)
	}

	bw.WriteBlock(block, encodedMetadataValue)
}

// WriteBlock should be invoked for blocks which contain normal transactions.
// It sets the target block as the pending next block, and returns before it is committed.
// Before returning, it acquires the committing lock, and spawns a go routine which will
// annotate the block with metadata and signatures, and write the block to the ledger
// then release the lock.  This allows the calling thread to begin assembling the next block
// before the commit phase is complete.
func (bw *BlockWriter) WriteBlock(block *cb.Block, encodedMetadataValue []byte) {
	bw.committingBlock.Lock()
	bw.lastBlock = block

	go func() {
		defer bw.committingBlock.Unlock()
		bw.commitBlock(encodedMetadataValue)
	}()
}

// commitBlock should only ever be invoked with the bw.committingBlock held
// this ensures that the encoded config sequence numbers stay in sync
func (bw *BlockWriter) commitBlock(encodedMetadataValue []byte) {
	bw.addLastConfig(bw.lastBlock)
	bw.addBlockSignature(bw.lastBlock, encodedMetadataValue)

	err := bw.support.Append(bw.lastBlock)
	if err != nil {
		logger.Panicf("[channel: %s] Could not append block: %s", bw.support.ChannelID(), err)
	}
	logger.Debugf("[channel: %s] Wrote block [%d]", bw.support.ChannelID(), bw.lastBlock.GetHeader().Number)
}

func (bw *BlockWriter) addBlockSignature(block *cb.Block, consenterMetadata []byte) {
	blockSignature := &cb.MetadataSignature{
		SignatureHeader: protoutil.MarshalOrPanic(protoutil.NewSignatureHeaderOrPanic(bw.support)),
	}

	blockSignatureValue := protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
		LastConfig:        &cb.LastConfig{Index: bw.lastConfigBlockNum},
		ConsenterMetadata: protoutil.MarshalOrPanic(&cb.Metadata{Value: consenterMetadata}),
	})

	blockSignature.Signature = protoutil.SignOrPanic(
		bw.support,
		util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, protoutil.BlockHeaderBytes(block.Header)),
	)

	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: blockSignatureValue,
		Signatures: []*cb.MetadataSignature{
			blockSignature,
		},
	})
}

func (bw *BlockWriter) addLastConfig(block *cb.Block) {
	configSeq := bw.support.Sequence()
	if configSeq > bw.lastConfigSeq {
		logger.Debugf("[channel: %s] Detected lastConfigSeq transitioning from %d to %d, setting lastConfigBlockNum from %d to %d", bw.support.ChannelID(), bw.lastConfigSeq, configSeq, bw.lastConfigBlockNum, block.Header.Number)
		bw.lastConfigBlockNum = block.Header.Number
		bw.lastConfigSeq = configSeq
	}

	lastConfigValue := protoutil.MarshalOrPanic(&cb.LastConfig{Index: bw.lastConfigBlockNum})
	logger.Debugf("[channel: %s] About to write block, setting its LAST_CONFIG to %d", bw.support.ChannelID(), bw.lastConfigBlockNum)

	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: lastConfigValue,
	})
}
