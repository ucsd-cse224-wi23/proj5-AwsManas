package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {

	for fname, fileMdata := range m.FileMetaMap {
		if fname == fileMetaData.Filename {
			// file name match , update if version is correct
			if fileMetaData.Version != fileMdata.Version+1 {
				// out of sync for client
				return &Version{Version: -1}, nil
			} else {
				// update the file
				m.FileMetaMap[fileMetaData.Filename] = fileMetaData
				return &Version{Version: fileMetaData.Version}, nil
			}
		}
	}
	// if we are here then it is a new file that needs to be inserted
	if fileMetaData.Version == 1 {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		return &Version{Version: fileMetaData.Version}, fmt.Errorf("New file should have version = 1")
	}

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var bsm BlockStoreMap
	bsm.BlockStoreMap = make(map[string]*BlockHashes)
	temp_map := make(map[string][]string)
	for _, hash := range blockHashesIn.Hashes {
		val := m.ConsistentHashRing.GetResponsibleServer(hash)
		temp_map[val] = append(temp_map[val], hash)
	}
	for k, v := range temp_map {
		bsm.BlockStoreMap[k] = &BlockHashes{Hashes: v}
	}
	return &bsm, nil
}
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
