package lsmgo

import (
	"fmt"
	"path/filepath"
)

func (t *Tree) compact() {
	for {
		select {
		case <-t.closech: // 关闭 compact
			return
		case memCompactItem := <-t.memCompactCh: // 可读写 MemTable 落盘
			t.compactMemTable(memCompactItem)
		case level := <-t.levelCompactCh: // 压缩合并某一 level 层
			t.compactLevel(level)
		}
	}
}

func (t *Tree) compactMemTable(item *ROnlyMemTable) {
	// 一、溢写落盘
	// 二、删除 ROnlyMemTable
}

func (t *Tree) compactLevel(level int) {

}

func (t *Tree) walFilePath() string {
	return filepath.Join(t.config.Dir, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}
