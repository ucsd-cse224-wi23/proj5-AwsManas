package surfstore

import (
	context "context"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()
	succc, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}

	*succ = succc.Flag
	return conn.Close()

}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())

	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()
	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = blockHashes.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())

	if err != nil {
		return err
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()
	fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

	if err != nil {
		conn.Close()
		return err
	}

	*serverFileInfoMap = fileInfoMap.FileInfoMap
	return conn.Close()

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())

	if err != nil {
		return err
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	verr, err := c.UpdateFile(ctx, fileMetaData)

	if err != nil {
		conn.Close()
		return err
	}

	*latestVersion = verr.Version

	return conn.Close()

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())

	if err != nil {
		return err
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	block_store_mp, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

	if err != nil {
		conn.Close()
		return err
	}

	tmp := block_store_mp.BlockStoreMap

	tmp2 := make(map[string][]string)

	for k, v := range tmp {
		tmp2[k] = v.Hashes
	}

	*blockStoreMap = tmp2

	return conn.Close()

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {

	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())

	if err != nil {
		return err
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	block_store_addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

	if err != nil {
		conn.Close()
		return err
	}

	*blockStoreAddrs = block_store_addrs.BlockStoreAddrs

	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())

	if err != nil {
		return err
	}

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	block_hashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = block_hashes.Hashes

	return conn.Close()

}
