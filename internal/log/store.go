package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// encoding that we persist record sizes and index entries in
	enc = binary.BigEndian
)

const (
	// number of bytes used to store the record's length
	lenWidth = 8
)

// The file to store Records in
type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

// creates store for a given file
func newStore(f *os.File) (*store, error) {
	// to get file's current size
	fileInfo, err := os.Stat(f.Name()) 

	if err != nil {
		return nil, err
	}
	
	size := uint64(fileInfo.Size())
	
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

// Persists bytes to store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	
	// writing length of p
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0,0,err
	}

	// writing p
	w, err := s.buf.Write(p)

	if err != nil {
		return 0,0,err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Returns bytes stored at given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err:= s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	//fethcing length of our data at pos
	if _, err  := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	//allocating data size bytes
	b := make([]byte, enc.Uint64(size))

	//reading from pos+lenWidth into b
	if _, err := s.File.ReadAt(b, int64(pos + lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// Reads len(p) bytes into p beginning at the off offset from store's file
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

//Closes and persists any buffered data
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}