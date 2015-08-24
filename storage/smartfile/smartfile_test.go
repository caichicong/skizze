package smartfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seiflotfy/skizze/config"
	"github.com/seiflotfy/skizze/utils"
)

func setupFileManagerTests() {
	os.Setenv("SKZ_DATA_DIR", "/tmp/skizze_storage_filemanager_data")
	os.Setenv("SKZ_INFO_DIR", "/tmp/skizze_storage_filemanager_info")
	path, err := os.Getwd()
	utils.PanicOnError(err)
	path = filepath.Dir(path)
	configPath := filepath.Join(path, "../config/default.toml")
	os.Setenv("SKZ_CONFIG", configPath)
	tearDownFileManagerTests()
}

func tearDownFileManagerTests() {
	os.RemoveAll(config.GetConfig().GetDataDir())
	os.RemoveAll(config.GetConfig().GetInfoDir())
	os.Mkdir(config.GetConfig().GetDataDir(), 0777)
	os.Mkdir(config.GetConfig().GetInfoDir(), 0777)
}

func TestFileManager(t *testing.T) {
	setupFileManagerTests()
	//defer tearDownFileManagerTests()
	path := filepath.Join(config.GetConfig().GetDataDir(), "x-force")
	fq, _ := NewFile(path, 1)
	fq.Write([]byte("beast"), 4)
	fq.Write([]byte("storm"), 14)
	fq.Write([]byte("kiss"), 4)
}
