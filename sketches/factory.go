package sketches

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seiflotfy/skizze/config"
	"github.com/seiflotfy/skizze/sketches/abstract"
	"github.com/seiflotfy/skizze/sketches/wrappers/bloom"
	"github.com/seiflotfy/skizze/sketches/wrappers/count-min-log"
	"github.com/seiflotfy/skizze/sketches/wrappers/dict"
	"github.com/seiflotfy/skizze/sketches/wrappers/hllpp"
	"github.com/seiflotfy/skizze/sketches/wrappers/topk"
	"github.com/seiflotfy/skizze/storage"
)

/*
SketchProxy ...
*/
type SketchProxy struct {
	*abstract.Info
	sketch abstract.Sketch
	lock   sync.RWMutex
	ops    uint
	dirty  bool
}

/*
Add ...
*/
func (sp *SketchProxy) Add(values [][]byte) (bool, error) {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	sp.ops++
	sp.Properties["adds"]++
	sp.dirty = true
	defer sp.save(false)
	return sp.sketch.AddMultiple(values)
}

/*
Remove ...
*/
func (sp *SketchProxy) Remove(values [][]byte) (bool, error) {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	sp.Properties["remove"]++
	sp.ops++
	sp.dirty = true
	defer sp.save(false)
	return sp.sketch.RemoveMultiple(values)
}

/*
Count ...
*/
func (sp *SketchProxy) Count(values []string) map[string]interface{} {
	result := make(map[string]interface{})
	result["info"] = sp.Info.Properties
	if sp.Type == abstract.CML {
		bvalues := make([][]byte, len(values), len(values))
		for i, value := range values {
			bvalues[i] = []byte(value)
		}
		result["result"] = sp.sketch.GetFrequency(bvalues)
		return result
	} else if sp.Type == abstract.TopK {
		result["result"] = sp.sketch.GetFrequency(nil)
		return result
	} else if sp.Type == abstract.Bloom {
		bvalues := make([][]byte, len(values), len(values))
		for i, value := range values {
			bvalues[i] = []byte(value)
		}
		result["result"] = sp.sketch.GetFrequency(bvalues)
		return result
	}
	result["result"] = sp.sketch.GetCount()
	return result
}

func (sp *SketchProxy) autosave() {
	for {
		time.Sleep(time.Duration(config.GetConfig().SaveThresholdSeconds) * time.Second)
		if sp.dirty {
			sp.save(true)
			sp.dirty = false
		}
	}
}

/*
save ...
*/
func (sp *SketchProxy) save(force bool) {
	if !sp.dirty {
		return
	}

	if sp.ops%config.GetConfig().SaveThresholdOps == 0 || force {
		sp.ops++
		sp.dirty = false
		manager := storage.Manager()
		serialized, err := sp.sketch.Marshal()
		if err != nil {
			logger.Error.Println(err)
		}
		err = manager.SaveData(sp.Info.ID, serialized, 0)
		if err != nil {
			logger.Error.Println(err)
		}
		info, _ := json.Marshal(sp.Info)
		err = manager.SaveInfo(sp.Info.ID, info)
		if err != nil {
			logger.Error.Println(err)
		}
	}
}

func createSketch(info *abstract.Info) (*SketchProxy, error) {
	var sketch abstract.Sketch
	var err error
	manager := storage.Manager()
	err = manager.Create(info.ID)
	if err != nil {
		return nil, errors.New("Error creating new sketch")
	}

	switch info.Type {
	case abstract.HLLPP:
		sketch, err = hllpp.NewSketch(info)
	case abstract.TopK:
		sketch, err = topk.NewSketch(info)
	case abstract.CML:
		sketch, err = cml.NewSketch(info)
	case abstract.Dict:
		sketch, err = dict.NewSketch(info)
	case abstract.Bloom:
		sketch, err = bloom.NewSketch(info)
	default:
		return nil, errors.New("Invalid sketch type: " + info.Type)
	}
	if err != nil {
		return nil, errors.New("Error creating new sketch")
	}

	sp := SketchProxy{info, sketch, sync.RWMutex{}, 0, true}
	err = storage.Manager().Create(info.ID)
	if err != nil {
		return nil, err
	}

	sp.save(true)
	go sp.autosave()
	return &sp, nil
}

func loadSketch(info *abstract.Info) (*SketchProxy, error) {
	var sketch abstract.Sketch

	data, err := storage.Manager().LoadData(info.ID, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("Error loading data for sketch: %s", info.ID)
	}

	switch info.Type {
	case abstract.HLLPP:
		sketch, err = hllpp.Unmarshal(info, data)
	case abstract.TopK:
		sketch, err = topk.Unmarshal(info, data)
	case abstract.CML:
		sketch, err = cml.Unmarshal(info, data)
	case abstract.Dict:
		sketch, err = dict.Unmarshal(info, data)
	case abstract.Bloom:
		sketch, err = bloom.Unmarshal(info, data)
	default:
		logger.Info.Println("Invalid sketch type", info.Type)
	}
	sp := SketchProxy{info, sketch, sync.RWMutex{}, 0, false}

	if err != nil {
		return nil, fmt.Errorf("Error loading data for sketch: %s", info.ID)
	}

	go sp.autosave()
	return &sp, nil
}
