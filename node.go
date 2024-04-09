package lsmgo

type Node struct {
	conf          *TreeConfig
	fileName      string // 不含路径，纯文件名
	level         int
	seq           int32
	size          uint64
	blockToFilter map[uint64][]byte // 各 block 对应的 filter bitmap
	index         []*Index          // 各 block 对应的 index
	endKey        []byte            // node 中最大的 key
	startKey      []byte            // node 中最小的 key
	sstReader     *SSTReader
}

func NewNode() {

}

func (n *Node) Get(key []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (n *Node) Close() {

}
