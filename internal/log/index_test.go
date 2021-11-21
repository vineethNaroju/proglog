package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	temp, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)

	defer os.Remove(temp.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(temp, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, temp.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// fail to read beyond index size
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// FAILS HERE TODO
	err = idx.Close()
	require.NoError(t, err)

	// build from existing file
	temp, _ = os.OpenFile(temp.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(temp, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
