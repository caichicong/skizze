package cml

import (
	"errors"

	"github.com/seiflotfy/skizze/sketches/abstract"
	"github.com/seiflotfy/skizze/sketches/wrappers/count-min-log/count-min-log"
	"github.com/seiflotfy/skizze/utils"
)

var logger = utils.GetLogger()

const defaultCapacity = 1000000.0

/*
Sketch is the toplevel Sketch to control the count-min-log implementation
*/
type Sketch struct {
	*abstract.Info
	impl *cml.Sketch
}

/*
NewSketch ...
*/
func NewSketch(info *abstract.Info) (*Sketch, error) {
	if info.Properties["capacity"] == 0 {
		info.Properties["capacity"] = defaultCapacity
	}
	sketch, err := cml.NewForCapacity16(uint64(info.Properties["capacity"]), 0.01)
	d := Sketch{info, sketch}
	if err != nil {
		logger.Error.Printf("an error has occurred while saving Sketch: %s", err.Error())
	}
	return &d, nil
}

/*
Add ...
*/
func (d *Sketch) Add(value []byte) (bool, error) {
	d.impl.IncreaseCount(value)
	return true, nil
}

/*
AddMultiple ...
*/
func (d *Sketch) AddMultiple(values [][]byte) (bool, error) {
	for _, value := range values {
		d.impl.IncreaseCount(value)
	}
	return true, nil
}

/*
Remove ...
*/
func (d *Sketch) Remove(value []byte) (bool, error) {
	logger.Error.Println("This Sketch type does not support deletion")
	return false, errors.New("This Sketch type does not support deletion")
}

/*
RemoveMultiple ...
*/
func (d *Sketch) RemoveMultiple(values [][]byte) (bool, error) {
	logger.Error.Println("This Sketch type does not support deletion")
	return false, errors.New("This Sketch type does not support deletion")
}

/*
GetCount ...
*/
func (d *Sketch) GetCount() uint {
	return 0
}

/*
Clear ...
*/
func (d *Sketch) Clear() (bool, error) {
	d.impl.Reset()
	return true, nil
}

/*
Marshal ...
*/
func (d *Sketch) Marshal() ([]byte, error) {
	return d.impl.Marshal()
}

/*
GetFrequency ...
*/
func (d *Sketch) GetFrequency(values [][]byte) interface{} {
	res := make(map[string]uint)
	for _, value := range values {
		count := d.impl.Frequency(value)
		res[string(value)] = uint(count)
	}
	return res
}

/*
Unmarshal ...
*/
func Unmarshal(info *abstract.Info, data []byte) (*Sketch, error) {
	sketch, err := cml.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &Sketch{info, sketch}, nil
}
