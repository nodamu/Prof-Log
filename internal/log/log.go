package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a new store
func newStore(f *os.File) (*store, error) {

	// Retreives file current size
	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to a store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	pos = s.size

	// Write binary representation of data into the buffer writer s.buf
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the contents of p into the buffer
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// +lenWidth because each byte write take 8 bits
	// So starting from the position of the byte to the 8th bit
	w += lenWidth

	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) (p []byte, err error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))

	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()

	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
