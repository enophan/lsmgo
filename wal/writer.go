package wal

type WalWiter struct{}

// 需要传入完整路径
func NewWalWriter(filename string) (*WalWiter, error) {
	return nil, nil
}

func (ww *WalWiter) Write(key, value []byte) error {
	return nil
}

func (ww *WalWiter) Close() {
}
