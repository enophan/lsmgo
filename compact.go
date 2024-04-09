package lsmgo

import (
	"lsmgo/memtable"
)

type ROnlyMemTable struct {
	walFile  string
	memTable memtable.MemTable
}
