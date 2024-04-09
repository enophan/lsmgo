package wal

import (
	"bufio"
	"lsmgo/memtable"
	"os"
)

type WalReader struct {
	fileName string
	src      *os.File
	reader   *bufio.Reader
}

// 传入完整路径
func NewWalReader(fileName string) (*WalReader, error) {
	return &WalReader{
		fileName: fileName,
	}, nil
}

func (wr *WalReader) RestoreMemTable(memtable memtable.MemTable) error {
	return nil
}

func (wr *WalReader) Close() {
	wr.reader.Reset(wr.src)
	wr.src.Close()
}
