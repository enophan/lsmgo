package lsmgo

import (
	"lsmgo/memtable"
	"lsmgo/wal"
	"sync"
)

type Tree struct {
	config         *TreeConfig
	mu             sync.RWMutex
	levelLocks     []sync.RWMutex
	memTable       memtable.MemTable   // 可读写 MemTable
	rOnlyMemTables []*ROnlyMemTable    // 待 compact 的只读 MemTable 集合
	walWriter      *wal.WalWiter       // 预写日志写入器
	memTableIndex  int32               // 当前可读写 MemTable 序号（要与 WalWriter 序号保持一致）
	nodes          [][]*Node           // 某 level 层的某 SSTable 文件地址
	closech        chan struct{}       // 通知 compact 协程停止
	memCompactCh   chan *ROnlyMemTable // 告知 compact 协程需要落盘操作的只读 MemTable
	levelCompactCh chan int            // 告知 compact 协程需要合并的 level 层
}

// lsm tree ，启动！
func NewTree(config *TreeConfig) (*Tree, error) {
	// 1. 初始化 tree
	// 2. 读取 SST 构造 tree 中的 node 节点
	// 3. 异步启动 compact 协程
	// 4. 通过 wal 还原 memtable

	t := &Tree{
		config:         config,
		levelLocks:     make([]sync.RWMutex, config.MaxLevel),
		nodes:          make([][]*Node, config.MaxLevel),
		closech:        make(chan struct{}),
		memCompactCh:   make(chan *ROnlyMemTable),
		levelCompactCh: make(chan int),
	}

	if err := t.constructTree(); err != nil {
		return nil, err
	}

	go t.compact()

	if err := t.constructMemTables(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Tree) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 首先，写入预写日志文件
	if err := t.walWriter.Write(key, value); err != nil {
		return err
	}

	// 其次，写入 MemTable
	if err := t.memTable.Put(key, value); err != nil {
		return err
	}

	// 最后，检查 MemTable 是否达到落盘阈值
	// 考虑到溢写成 sstable 后，需要有一些辅助的元数据，预估容量放大为 5/4 倍
	if t.memTable.Size()*5/4 <= t.config.SSTSize {
		return nil
	}

	// 达到阈值就将当前 MemTable 切换为只读 MemTable
	t.switchMemTable()
	return nil
}

func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	// 1. 从所有 MemTable 找
	t.mu.Lock()
	value, ok := t.memTable.Get(key)
	if ok {
		t.mu.Lock()
		return value, true, nil
	}

	for i := len(t.rOnlyMemTables) - 1; i >= 0; i-- {
		value, ok := t.rOnlyMemTables[i].memTable.Get(key)
		if ok {
			t.mu.Unlock()
			return value, true, nil
		}
	}
	t.mu.Unlock()

	// 2. 从 level0 层找，每一个 sst 都要检索
	t.levelLocks[0].Lock()
	for i := len(t.nodes[0]) - 1; i >= 0; i-- {
		value, ok, err := t.nodes[0][i].Get(key)
		if err != nil {
			t.levelLocks[0].Unlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[0].Unlock()
			return value, true, nil
		}
	}
	t.levelLocks[0].Unlock()

	// 3. 从 level1~k 层找
	for level := 1; level < len(t.nodes); level++ {
		t.levelLocks[level].RLock()
		node, ok := t.levelBinarySearch(level, key, 0, len(t.nodes[level])-1)
		if !ok {
			t.levelLocks[level].RUnlock()
			continue
		}
		value, ok, err := node.Get(key)
		if err != nil {
			t.levelLocks[level].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[level].RUnlock()
			return value, true, nil
		}
		t.levelLocks[level].RUnlock()
	}

	return nil, false, nil
}

// TODO DELETE

func (t *Tree) Close() {
	close(t.closech)
	for i := 0; i < len(t.nodes); i++ {
		for j := 0; j < len(t.nodes[i]); j++ {
			t.nodes[i][j].Close()
		}
	}
}

// 切换读写 MemTable ，通知 compact 启动 MemTable 落盘操作
func (t *Tree) switchMemTable() error {
	// 1. 添加成只读 MemTable
	// 2. 通知 compact 执行落盘
	// 3. 新建可读写 MemTable

	rOnlyMemTable := &ROnlyMemTable{
		walFile:  t.walFilePath(),
		memTable: t.memTable,
	}
	t.rOnlyMemTables = append(t.rOnlyMemTables, rOnlyMemTable)
	t.walWriter.Close()

	go func() {
		t.memCompactCh <- rOnlyMemTable
	}()

	t.newMemTable()
	return nil
}

// 新建一个可读写 MemTable， 同时也要创建对应新 WalWriter
func (t *Tree) newMemTable() {
	t.walWriter, _ = wal.NewWalWriter(t.walFilePath())
	t.memTable = t.config.MemTableConstruct()
}

// 检索指定 level 层
func (t *Tree) levelBinarySearch(level int, key []byte, start, end int) (*Node, bool) {
	return nil, false
}
