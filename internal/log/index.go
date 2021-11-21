package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// The file to store Index entries in
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func (i *index) Name() string {
	return i.file.Name()
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// to get file details
	fileInfo, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	// to store file size ( may not be empty)
	idx.size = uint64(fileInfo.Size())

	// limiting file size to our config, to remove remaining space
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))

	if err != nil {
		return nil, err
	}

	// transfer data from file to mmap
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Flushes and Closes index
func (i *index) Close() error {
	//synchronosly flush mmap left out data to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// synchronosly flush file buffer to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	// we grow files by appending empty space, which messes up the bytes arrangement and
	// hence we need to truncate extra space at end of file
	// limiting file size to our config, to remove last remaining space
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Returns record's position in the store at an offset in
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// we use relative offset wrt segment otherwise we need to use 8 bytes instead of 4 bytes
	// companies make billions or trillions or records every day
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Append offset and position to index
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += uint64(entWidth)

	return nil
}
