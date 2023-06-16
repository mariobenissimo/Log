// START: begin
package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// ogni voce dell'index mantiene l'offset del record e la sua posizione nel file
var ( // numero di byte per ogni voce dell'index
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth // per saltare all posizione di una voce data dal suo offest
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // dimi file e dove scrivere la succesiva voce
}

func newIndex(f *os.File, c Config) (*index, error) { // crea il file e lo mappa in memoria
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) { // in = offset e restuisce la posizione del record nello store
	// 4 byte offset e 8 byte position
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	// mi sposto nel record che cero
	if i.size < pos+entWidth { // se l'offset eccede l'index
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])          // leggo da pos a pos+OffWidt ovver leggo l'offset
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth]) // leggo da pos+OffWidt a pos+EntWidth ovver leggo la pos nel file
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
