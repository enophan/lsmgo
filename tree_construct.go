package lsmgo

import (
	"io/fs"
	"lsmgo/wal"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// 读取指定目录下的 sst 文件，构建 lsm tree
func (t *Tree) constructTree() error {
	// 1. 读取目录下 sst 文件
	// 2. 将每一个 sst 文件构造成 node 加入 Tree.nodes

	sstEntries, err := t.getStoredSSTEntries()
	if err != nil {
		return err
	}

	for _, entry := range sstEntries {
		if err := t.loadNode(entry); err != nil {
			return err
		}
	}
	return nil
}

// 从给定的目录中获取所有的 sst 文件，结果按照 level 与 seq 从小到大排序
func (t *Tree) getStoredSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.config.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".sst") {
			sstEntries = append(sstEntries, entry)
		}
	}

	sort.Slice(sstEntries, func(i, j int) bool {
		iLevel, iSeq := getLevelAndSeqFromSST(sstEntries[i].Name())
		jLevel, jSeq := getLevelAndSeqFromSST(sstEntries[j].Name())
		if iLevel == jLevel {
			return iSeq < jSeq
		}
		return iLevel < jLevel
	})
	return sstEntries, nil
}

// 将 sst 作为 node 载入 lsm tree
func (t *Tree) loadNode(sstEntry fs.DirEntry) error {

	return nil
}

// level_seq.sst => level, seq
func getLevelAndSeqFromSST(fileName string) (int, int32) {
	parts := strings.Split(fileName, "_")
	// if len(parts) != 2 {
	// 	return 0, 0, ErrFileNameFormat
	// }
	strLevel := parts[0]
	strSeq := strings.Split(parts[1], ".")[0]
	level, _ := strconv.Atoi(strLevel)
	seq, _ := strconv.Atoi(strSeq)
	return level, int32(seq)
}

func (t *Tree) constructMemTables() error {
	// 从 wal 文件中寻找，找不到则新建，找得到则用 Tree.restoreMemTable() 还原
	allEntries, err := os.ReadDir(filepath.Join(t.config.Dir, "walfile"))
	if err != nil {
		return err
	}
	var walEnties []fs.DirEntry
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".wal") {
			walEnties = append(walEnties, entry)
		}
	}
	if len(walEnties) == 0 {
		t.newMemTable()
		return nil
	}
	return t.restoreMemtable(walEnties)
}

// 从 wal 中恢复所有 MemTable ，其中编号最大的为可读写 MemTable ，其他的为只读 MemTable
func (t *Tree) restoreMemtable(wals []fs.DirEntry) error {
	// 1. 给 wals 按照 index 从小到大排序
	// 2. 新建 WalReader 读取 wal 文件内容至新建的 MemTable
	// 3. 最后一个 MemTable 作为可读写
	sort.Slice(wals, func(i, j int) bool {
		iWal := getMemIndexFromWal(wals[i].Name())
		jWal := getMemIndexFromWal(wals[j].Name())
		return iWal < jWal
	})

	for i := 0; i < len(wals); i++ {
		// 新建 walreader
		fileName := filepath.Join(t.config.Dir, "walfile", wals[i].Name())
		walReader, err := wal.NewWalReader(fileName)
		if err != nil {
			return err
		}
		defer walReader.Close()

		// 新建 memtable
		memTable := t.config.MemTableConstruct()

		// 还原 memtable
		if err := walReader.RestoreMemTable(memTable); err != nil {
			return err
		}

		if i == len(wals)-1 { // 序号最大的 wal 要作为可读写 MemTable
			t.memTable = memTable
			t.memTableIndex = getMemIndexFromWal(wals[i].Name())
			t.walWriter, err = wal.NewWalWriter(fileName)
			if err != nil {
				return nil
			}
		} else { // 其他都是只读 MemTable
			rOnlyMemTable := &ROnlyMemTable{
				walFile:  fileName,
				memTable: memTable,
			}
			t.rOnlyMemTables = append(t.rOnlyMemTables, rOnlyMemTable)
			// 加入落盘队列
			t.memCompactCh <- rOnlyMemTable
		}
	}
	return nil
}

// 传入文件名
func getMemIndexFromWal(filename string) int32 {
	parts := strings.Split(filename, ".")
	index, _ := strconv.Atoi(parts[0])
	return int32(index)
}
