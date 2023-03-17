package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	fmt.Println("Client sync started - Scanning the base directory ")
	all_files := scanBaseDir(client.BaseDir)
	fmt.Println("All files scanned , got : ", all_files)
	client_sync_files := make(map[string]FileMetaData)
	client_sync_files_data := make(map[string][]Block)
	var block_store_addr []string
	client.GetBlockStoreAddrs(&block_store_addr)
	fmt.Println("Block Store Adress in client : ", block_store_addr)

	for _, file := range all_files {
		if file != "index.db" {
			//fmt.Println("Generating hashlist and blocklist for ", file)
			var fmd FileMetaData
			fmd.BlockHashList = ReadAndGetBlocks(file, client.BaseDir, client.BlockSize)
			//fmt.Println("Generated hashlist ", file)
			fmd.Filename = file
			client_sync_files[file] = fmd
			//fmt.Println("Generating blocks ")
			client_sync_files_data[file] = ReadDataAndGetBlocks(file, client.BaseDir, client.BlockSize)
			//fmt.Println("Generated blocks ")
		}
	}

	localFileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println("Error loading metafile in local client")
	}

	var new_files_in_local []string
	var changed_files_idx_local []string
	var deleted_file_list []string

	for file_name, file_meta_data := range client_sync_files {
		val, ok := localFileMetaMap[file_name]
		if ok {
			if !compareBothHashes(file_meta_data.BlockHashList, val.BlockHashList) {
				changed_files_idx_local = append(changed_files_idx_local, file_name)
			}
		} else {
			new_files_in_local = append(new_files_in_local, file_name)
		}
	}

	for file_name, val := range localFileMetaMap {
		_, ok := client_sync_files[file_name]
		if ok {
			continue
		} else {
			if len(val.BlockHashList) == 1 && val.BlockHashList[0] == "0" {
				continue
			} else {
				deleted_file_list = append(deleted_file_list, file_name)
			}
		}
	}
	fmt.Println("New files in local : ", new_files_in_local)
	fmt.Println("Changed files in local : ", changed_files_idx_local)
	fmt.Println("Deleted file list : ", deleted_file_list)
	var remote_index_file_map map[string]*FileMetaData
	client.GetFileInfoMap(&remote_index_file_map)
	//PrintMetaMap(remote_index_file_map)
	// Download files present in remote but not in localindex (not base dir)
	//+ // Download all the files whose version is more that what was in local_index

	// Tested till here
	for file_nm, val := range remote_index_file_map {
		t, ok := localFileMetaMap[file_nm]
		if ok {
			// check version, if outdated download the file
			if val.Version > t.Version {
				fmt.Println("Downloading file , updated version present in remote  : ", file_nm)
				downloadFileFromRemote(client.BaseDir, file_nm, block_store_addr, client, val)
				localFileMetaMap[file_nm] = val
				deleted_file_list = deleteIfExists(deleted_file_list, file_nm)
				changed_files_idx_local = deleteIfExists(changed_files_idx_local, file_nm)
			}
		} else {
			fmt.Println("Downloading file : ", file_nm, " from remote")
			downloadFileFromRemote(client.BaseDir, file_nm, block_store_addr, client, val)
			localFileMetaMap[file_nm] = val
		}
	}

	// Upload file that are not in localindex and remoteIndex

	var all_files_best_local []string
	all_files_best_local = append(all_files_best_local, new_files_in_local...)
	all_files_best_local = append(all_files_best_local, changed_files_idx_local...)

	for _, file_nm := range all_files_best_local {
		var block_addr_map map[string][]string
		client.GetBlockStoreMap(client_sync_files[file_nm].BlockHashList, &block_addr_map)
		fmt.Println("Get block addr map : ", block_addr_map)
		for i := 0; i < len(client_sync_files[file_nm].BlockHashList); i++ {
			var suc bool
			bsa := getBlockStoreAddr(client_sync_files[file_nm].BlockHashList[i], block_addr_map)
			client.PutBlock(&client_sync_files_data[file_nm][i], bsa, &suc)
			if !suc {
				fmt.Println("Error while putting the block, something went wrong")
			}
		}

		// Update the server with File Info
		var filemtdata FileMetaData
		filemtdata.Filename = file_nm
		if find(new_files_in_local, file_nm) {
			filemtdata.Version = 1
		} else {
			filemtdata.Version = localFileMetaMap[file_nm].Version + 1
		}
		filemtdata.BlockHashList = client_sync_files[file_nm].BlockHashList
		var ver Version
		client.UpdateFile(&filemtdata, &ver.Version)
		if ver.Version != -1 {
			localFileMetaMap[file_nm] = &filemtdata
		} else {
			// Failure , download the latest file
			var new_remote_index_file_map map[string]*FileMetaData
			client.GetFileInfoMap(&new_remote_index_file_map)
			val := new_remote_index_file_map[file_nm]
			downloadFileFromRemote(client.BaseDir, file_nm, block_store_addr, client, val)
			localFileMetaMap[file_nm] = val
		}

	}

	// Sync all the deleted file

	for _, file_ := range deleted_file_list {
		// Update the server with File Info
		var filemtdata FileMetaData
		filemtdata.Filename = file_
		filemtdata.Version = localFileMetaMap[file_].Version + 1
		filemtdata.BlockHashList = []string{"0"}
		var ver Version
		client.UpdateFile(&filemtdata, &ver.Version)
		if ver.Version != -1 {
			localFileMetaMap[file_] = &filemtdata
		} else {
			// Failure , download the latest file
			var new_remote_index_file_map map[string]*FileMetaData
			client.GetFileInfoMap(&new_remote_index_file_map)
			val := new_remote_index_file_map[file_]
			downloadFileFromRemote(client.BaseDir, file_, block_store_addr, client, val)
			localFileMetaMap[file_] = val
		}
	}

	WriteMetaFile(localFileMetaMap, client.BaseDir)

	fmt.Println("Hereee-----")
}

func downloadFileFromRemote(dir_nm string, file_nm string, block_store_addrs []string, client RPCClient, val *FileMetaData) {
	var reconstruct_file []byte
	var block_addr_map map[string][]string
	client.GetBlockStoreMap(val.BlockHashList, &block_addr_map)

	if len(val.BlockHashList) == 1 && val.BlockHashList[0] == "0" {
		// delete the file
		if err := os.Remove(ConcatPath(dir_nm, file_nm)); err != nil {
			fmt.Println("File didnt existed that needed to be removed")
		}
		return
	}

	if len(val.BlockHashList) == 1 && val.BlockHashList[0] == GetBlockHashString([]byte{}) {
		ioutil.WriteFile(ConcatPath(dir_nm, file_nm), reconstruct_file, 0644)
	}
	for _, tmp := range val.BlockHashList {
		var blck Block
		var block_store_addr string
		block_store_addr = getBlockStoreAddr(tmp, block_addr_map)
		client.GetBlock(tmp, block_store_addr, &blck)
		reconstruct_file = append(reconstruct_file, blck.BlockData...)
	}
	// fmt.Println("Writing file : ", file_nm, reconstruct_file)
	ioutil.WriteFile(ConcatPath(dir_nm, file_nm), reconstruct_file, 0644)
}

func getBlockStoreAddr(search string, add_map map[string][]string) string {
	for k, val := range add_map {
		for _, iter_val := range val {
			if iter_val == search {
				fmt.Println("Returning this as address : ", k)
				return k
			}
		}
	}
	fmt.Println("Something went wrong, didnt find block")
	return "404"
}

func compareBothHashes(s1 []string, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func ReadAndGetBlocks(filedir string, baseDir string, blocksize int) []string {
	var string_for_file []string
	all_byte, err := ioutil.ReadFile(ConcatPath(baseDir, filedir))
	if err != nil {
		fmt.Println("There was error in reading a file")
	}
	// fmt.Println("All files : ", all_byte)
	i := 0
	for i+blocksize < len(all_byte) {
		// fmt.Println("Befo : ", all_byte[i:i+blocksize])
		// fmt.Println("Here : ", GetBlockHashString(all_byte[i:i+blocksize]))
		string_for_file = append(string_for_file, GetBlockHashString(all_byte[i:i+blocksize]))

		i = i + blocksize

	}
	// fmt.Println("Late : ", GetBlockHashString(all_byte[i:]))
	string_for_file = append(string_for_file, GetBlockHashString(all_byte[i:]))
	// fmt.Println("Late's Late : ", string_for_file)
	return string_for_file
}

func ReadDataAndGetBlocks(filedir string, baseDir string, blocksize int) []Block {
	var string_for_file []Block
	all_byte, err := ioutil.ReadFile(ConcatPath(baseDir, filedir))
	if err != nil {
		fmt.Println("There was error in reading a file")
	}
	i := 0
	for i+blocksize < len(all_byte) {
		var blk Block
		blk.BlockData = all_byte[i : i+blocksize]
		blk.BlockSize = int32(blocksize)
		string_for_file = append(string_for_file, blk)
		i = i + blocksize
	}
	var blk2 Block
	blk2.BlockData = all_byte[i:]
	blk2.BlockSize = int32(len(all_byte[i:]))
	string_for_file = append(string_for_file, blk2)
	return string_for_file
}

func scanBaseDir(basedir string) []string {
	var fileList []string
	files, err := ioutil.ReadDir(basedir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fileList = append(fileList, file.Name())
	}
	return fileList
}

func find(arr []string, ele string) bool {
	for _, a := range arr {
		if ele == a {
			return true
		}
	}
	return false
}

func deleteIfExists(arr []string, fname string) []string {
	var ret []string
	for _, a := range arr {
		if fname == a {
			continue
		} else {
			ret = append(ret, a)
		}
	}
	return ret
}
