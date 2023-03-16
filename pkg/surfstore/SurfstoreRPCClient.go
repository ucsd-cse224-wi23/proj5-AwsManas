package surfstore

import (
	context "context"
	"fmt"
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
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())

		if err != nil {
			return err
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil && err != ERR_NOT_LEADER && err != ERR_SERVER_CRASHED {
			conn.Close()
			return err
		}

		if err == nil {
			*serverFileInfoMap = fileInfoMap.FileInfoMap
		}

		conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())

		if err != nil {
			return err
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		defer cancel()

		verr, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil && err != ERR_NOT_LEADER && err != ERR_SERVER_CRASHED {
			conn.Close()
			return err
		}
		if err == nil {
			*latestVersion = verr.Version
		}

		conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	fmt.Println("Total lines : ", len(surfClient.MetaStoreAddrs))
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())

		if err != nil {
			return err
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		block_store_mp, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

		if err != nil && err != ERR_NOT_LEADER && err != ERR_SERVER_CRASHED {
			fmt.Println(err.Error())
			conn.Close()
			continue
		}

		if err == nil {
			print("here - no error", block_store_mp.BlockStoreMap)
			tmp := block_store_mp.BlockStoreMap

			tmp2 := make(map[string][]string)

			for k, v := range tmp {
				tmp2[k] = v.Hashes
			}

			*blockStoreMap = tmp2
		}

		conn.Close()
		cancel()
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())

		if err != nil {
			return err
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		defer cancel()

		block_store_addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		if err != nil && err != ERR_NOT_LEADER && err != ERR_SERVER_CRASHED {
			conn.Close()
			return err
		}

		if err == nil {
			*blockStoreAddrs = block_store_addrs.BlockStoreAddrs
		}

		conn.Close()
	}
	return nil
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
