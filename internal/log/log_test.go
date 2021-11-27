package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/vineethNaroju/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for testCase, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record": testAppendRead,
		"test out of range offset": testOutOfRangeErr,
		"test reader":              testReader,
		"test truncate":            testTruncate,
		"test re-init":             testReInit,
	} {
		t.Run(testCase, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32

			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("Hello World !"),
	}

	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	pew, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, rec.Value, pew.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	rec, err := log.Read(1)
	require.Error(t, err)
	require.Nil(t, rec)
}

func testReader(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("Hello World !"),
	}

	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, rec.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("Hello World !"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(1)
	require.Error(t, err)
}

func testReInit(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("Hello world !"),
	}

	for i := 0; i < 3; i++ {
		off, err := log.Append(rec)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}

	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	dlog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = dlog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = dlog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}
