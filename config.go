package lsmgo

import "lsmgo/memtable"

type TreeConfig struct {
	Dir               string
	MaxLevel          int
	MemTableConstruct memtable.MemTableConstruct
	SSTSize           uint64 // SSTable 文件大小，也是 MemTable 落盘阈值
}
