package storage

import (
	"os"
	"path/filepath"

	"github.com/seiflotfy/skizze/storage/smartfile"
)

/*
Create storage
*/
func (m *ManagerStruct) Create(ID string) error {
	sf, err := smartfile.NewFile(filepath.Join(dataPath, ID), 100000)
	if err != nil {
		return err
	}
	m.cache.Add(ID, sf)
	return nil
}

/*
SaveData ...
*/
func (m *ManagerStruct) SaveData(ID string, data []byte, offset int64) error {
	f, err := m.getFileFromCache(ID)
	f.Write(data, offset)
	return err
}

/*
DeleteData ...
*/
func (m *ManagerStruct) DeleteData(ID string) error {
	v, ok := m.cache.Get(ID)
	if ok {
		v.(*smartfile.File).Purge()
	}
	path := filepath.Join(dataPath, ID)
	return os.Remove(path)
}

/*
FlushData ...
*/
func (m *ManagerStruct) FlushData(ID string) error {
	f, _ := m.getFileFromCache(ID)
	f.Flush()
	return nil
}

/*
LoadData ...
*/
func (m *ManagerStruct) LoadData(ID string, offset int64, length int64) ([]byte, error) {
	sf, err := m.getFileFromCache(ID)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		length = sf.GetSize()
	}

	data := make([]byte, length)
	if err = sf.Read(data, offset); err != nil {
		return nil, err
	}
	return data, nil
}

func (m *ManagerStruct) getFileFromCache(ID string) (*smartfile.File, error) {
	v, ok := m.cache.Get(ID)
	if ok {
		return v.(*smartfile.File), nil
	}
	sf, err := smartfile.NewFile(filepath.Join(dataPath, ID), 100000)
	if err == nil {
		m.cache.Add(ID, sf)
	}
	return sf, err
}
