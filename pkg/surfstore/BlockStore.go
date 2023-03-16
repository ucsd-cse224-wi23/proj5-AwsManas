package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	val, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return val, fmt.Errorf("Some error happened while fetching the hash from hashmap, mostly not found")
	} else {
		return val, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashed_key := sha256.Sum256(block.BlockData)
	if block.BlockSize == 0 {
		return &Success{Flag: true}, nil
	}
	hash_str := hex.EncodeToString(hashed_key[:])
	bs.BlockMap[hash_str] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var chash []string
	for _, element := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[element]
		if !ok {
			continue
		} else {
			chash = append(chash, element)
		}
	}
	return &BlockHashes{Hashes: chash}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var chash []string
	for key := range bs.BlockMap {
		chash = append(chash, key)
	}
	return &BlockHashes{Hashes: chash}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
