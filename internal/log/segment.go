package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/vineethNaroju/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// New Segment at dir path with baseOffset and c Config
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, s.config); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = s.baseOffset
	} else {
		s.nextOffset = s.baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Marshal record and append to Segment
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	curr := s.nextOffset
	record.Offset = curr

	var prot []byte

	if prot, err = proto.Marshal(record); err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(prot)

	if err != nil {
		return 0, err
	}

	if err = s.index.Write(uint32(curr-uint64(s.baseOffset)), pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return curr, nil
}

// Reads from off in the Segment
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))

	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)

	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// Returns if Segment is full
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}
