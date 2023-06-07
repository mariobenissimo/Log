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
	mu   sync.Mutex    // mutua esclusione con lock e unlock sem
	buf  *bufio.Writer // buffer di scrittura
	size uint64
}

func newStrore(f *os.File) (*store, error) {
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
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil { // Scrive la lunghezza dei dati p nel buffer utilizzando binary.Write
		return 0, 0, err
	}
	w, err := s.buf.Write(p) // Scrive i dati p nel buffer e restituisce il numero di byte scritti
	if err != nil {
		return
	}
	w += lenWidth
	s.size += uint64(w) // Aggiorna la dimensione totale del buffer
	return uint64(w), pos, nil
}
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil { // Svuota il buffer s.buf e scrive tutti i dati nel file s.File
		return nil, err
	}
	size := make([]byte, lenWidth) // Crea un slice di byte per contenere la lunghezza dei dati
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))                              // Crea un slice di byte con la dimensione specificata dalla lunghezza dei dati
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil { // Legge i dati dal file s.File a partire dalla posizione pos+lenWidth
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
