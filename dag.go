package merkledag

import "hash"

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, hp HashPool) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	return nil
}
