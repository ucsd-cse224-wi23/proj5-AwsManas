package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
)

func Hash(mystring string) int {
	h := fnv.New32a()
	h.Write([]byte(mystring))
	return int(h.Sum32())
}

func Hash_to_string(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func moduloHashing(servers []string, data []string) {
	for _, dat := range data {
		blockHash := Hash(dat) % len(servers)
		responsibleServer := servers[blockHash]
		fmt.Println(dat, responsibleServer)
	}
}

func consistentHashing(servers []string, data []string) {
	// hash servers on a hash ring
	consistentHashRing := make(map[string]string) // hash: serverName
	for _, serverName := range servers {
		serverHash := Hash_to_string(serverName)
		consistentHashRing[serverHash] = serverName
	}
	// find where each block belongs to
	// 1. sort hash values (key in hash ring)
	hashes := []string{}
	for h := range consistentHashRing {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// 2. find the first server with larger hash value than blockHash
	for _, dat := range data {
		blockHash := Hash_to_string(dat)
		responsibleServer := ""
		for i := 0; i < len(hashes); i++ {
			if hashes[i] > blockHash {
				responsibleServer = consistentHashRing[hashes[i]]
				break
			}
		}
		if responsibleServer == "" {
			responsibleServer = consistentHashRing[hashes[0]]
		}
		fmt.Println(dat, responsibleServer)
	}

}

func main() {
	servers := []string{"server00", "server01", "server02", "Server03", "Server04"}
	data := []string{"blockABC", "blockDEF", "blockAAA", "blockZZZ", "block..."}

	// moduloHashing(servers, data)
	// fmt.Println("-----")
	// moduloHashing(servers[1:], data)
	// fmt.Println(" ")

	consistentHashing(servers, data)
	fmt.Println("-----")
	consistentHashing(servers[1:], data)

}
