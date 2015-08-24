package smartfile

import "os"

type item struct {
	bytes []byte
	dirty bool
}

/*
File ...
*/
type File struct {
	id    string
	file  *os.File
	queue *Cache
	size  uint
	ops   uint
}

/*
NewFile ...
*/
func NewFile(id string, size uint) (*File, error) {
	file, err := os.OpenFile(id, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	fq := &File{id: id, file: file, size: size,
		ops: 0, queue: NewLRU(int(size))}
	fq.queue.OnEvicted = func(k int64, v *item) {
		fq.file.WriteAt(v.bytes, k)
	}
	return fq, nil
}

/*
Read ...
*/
func (fq *File) Read(data []byte, offset int64) error {
	v, ok := fq.queue.get(offset)
	if ok {
		cdata := v.bytes
		if len(cdata) >= len(data) {
			for i, d := range v.bytes[:len(data)] {
				data[i] = d
			}
		}
		return nil
	}
	if _, err := fq.file.ReadAt(data, offset); err == nil {
		return err
	}

	fq.queue.Add(offset, item{data, false})
	return nil
}

/*
Write ...
*/
func (fq *File) Write(data []byte, offset int64) {
	fq.ops++
	fq.queue.Add(offset, item{data, true})
	if fq.ops%fq.size == 0 {
		fq.Flush()
	}
}

/*
Flush ...
*/
func (fq *File) Flush() error {
	var err error
	for _, k := range fq.queue.keys() {
		item, _ := fq.queue.peek(k)
		if item.dirty {
			if _, err = fq.file.WriteAt(item.bytes, k); err != nil {
				return err
			}
		}
		item.dirty = false
	}
	return err
}

/*
Clear ...
*/
func (fq *File) Clear() {
	fq.Flush()
	fq.queue.clear()
	fq.ops = 0
}

/*
Purge ...
*/
func (fq *File) Purge() {
	fq.file.Close()
	fq.queue.clear()
	fq.ops = 0
}

/*
GetSize ...
*/
func (fq *File) GetSize() int64 {
	stat, _ := fq.file.Stat()
	return stat.Size()
}
