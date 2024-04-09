package lsmgo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTree_getStoredSSTEntries(t *testing.T) {
	if err := os.Mkdir("test", os.ModePerm); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		if err := os.Remove("./test"); err != nil {
			t.Error(err)
		}
	}()

	files := []string{"1_1.sst", "1_2.ab", "10_0.sst", "2_3.sst", "1_5.sst", "10_10.sst", "10_5.sst"}
	for _, file := range files {
		file := file
		fd, err := os.Create(filepath.Join("test", file))
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			fd.Close()
			if err := os.Remove(filepath.Join("test", file)); err != nil {
				t.Error(err)
			}
		}()
	}

	tree := &Tree{
		config: &TreeConfig{
			Dir: "./test",
		},
	}

	expectEntries := []string{"1_1.sst", "1_5.sst", "2_3.sst", "10_0.sst", "10_5.sst", "10_10.sst"}
	allEntries, err := tree.getStoredSSTEntries()
	if err != nil {
		t.Error(err)
		return
	}
	var actualEntries []string
	for _, entry := range allEntries {
		actualEntries = append(actualEntries, entry.Name())
	}

	assert.Equal(t, expectEntries, actualEntries)

}

func TestGetLevelAndSeqFromSST(t *testing.T) {
	s := "1_562.sst"
	l, seq := getLevelAndSeqFromSST(s)
	t.Log(l)
	t.Log(seq)
}

func TestGetMemIndexFromWal(t *testing.T) {
	s := "1562.sst"
	l := getMemIndexFromWal(s)
	t.Log(l)
}
