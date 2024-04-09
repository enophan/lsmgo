package memtable

type MemTableConstruct func() MemTable

type MemTable interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	Close()
	All()
	Size() uint64
}
