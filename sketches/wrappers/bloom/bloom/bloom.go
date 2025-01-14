/*
Package bloom provides data structures and methods for creating Bloom filters.

A Bloom filter is a representation of a set of _n_ items, where the main
requirement is to make membership queries; _i.e._, whether an item is a
member of a set.

A Bloom filter has two parameters: _m_, a maximum size (typically a reasonably large
multiple of the cardinality of the set to represent) and _k_, the number of hashing
functions on elements of the set. (The actual hashing functions are important, too,
but this is not a parameter for this implementation). A Bloom filter is backed by
a BitSet; a key is represented in the filter by setting the bits at each value of the
hashing functions (modulo _m_). Set membership is done by _testing_ whether the
bits at each value of the hashing functions (again, modulo _m_) are set. If so,
the item is in the set. If the item is actually in the set, a Bloom filter will
never fail (the true positive rate is 1.0); but it is susceptible to false
positives. The art is to choose _k_ and _m_ correctly.

In this implementation, the hashing functions used is a local version of FNV,
a non-cryptographic hashing function, seeded with the index number of
the kth hashing function.

This implementation accepts keys for setting as testing as []byte. Thus, to
add a string item, "Love":

    uint n = 1000
    filter := bloom.New(20*n, 5) // load of 20, 5 keys
    filter.Add([]byte("Love"))

Similarly, to test if "Love" is in bloom:

    if filter.Test([]byte("Love"))

For numeric data, I recommend that you look into the binary/encoding library. But,
for example, to add a uint32 to the filter:

    i := uint32(100)
    n1 := make([]byte,4)
    binary.BigEndian.PutUint32(n1,i)
    f.Add(n1)

Finally, there is a method to estimate the false positive rate of a particular
bloom filter for a set of size _n_:

    if filter.EstimateFalsePositiveRate(1000) > 0.001

Given the particular hashing scheme, it's best to be empirical about this. Note
that estimating the FP rate will clear the Bloom filter.
*/
package bloom

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"

	"github.com/willf/bitset"
)

// A Filter is a representation of a set of _n_ items, where the main
// requirement is to make membership queries; _i.e._, whether an item is a
// member of a set.
type Filter struct {
	m uint
	k uint
	b *bitset.BitSet
}

// New creates a new Bloom filter with _m_ bits and _k_ hashing functions
func New(m uint, k uint) *Filter {
	return &Filter{m, k, bitset.New(m)}
}

func fnv64Hash(index uint, data []byte) uint64 {
	hash := uint64(index) + 14695981039346656037
	for _, c := range data {
		hash ^= uint64(c)
		hash *= 1099511628211
	}
	return hash
}

// baseHashes returns the four hash values of data that are used to create k
// hashes
func baseHashes(data []byte) []uint64 {
	return []uint64{
		fnv64Hash(0, data),
		fnv64Hash(1, data),
		fnv64Hash(2, data),
		fnv64Hash(3, data),
	}
}

// location returns the ith hashed location using the four base hash values
func (f *Filter) location(h []uint64, i uint) (location uint) {
	ii := uint64(i)
	location = uint((h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]) % uint64(f.m))
	return
}

// EstimateParameters estimates requirements for m and k.
// Based on https://bitbucket.org/ww/bloom/src/829aa19d01d9/bloom.go
// used with permission.
func EstimateParameters(n uint, p float64) (m uint, k uint) {
	m = uint(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return
}

// NewWithEstimates creates a new Bloom filter for about n items with fp
// false positive rate
func NewWithEstimates(n uint, fp float64) *Filter {
	m, k := EstimateParameters(n, fp)
	return New(m, k)
}

// Cap returns the capacity, _m_, of a Bloom filter
func (f *Filter) Cap() uint {
	return f.m
}

// K returns the number of hash functions used in the Filter
func (f *Filter) K() uint {
	return f.k
}

// Add data to the Bloom Filter. Returns the filter (allows chaining)
func (f *Filter) Add(data []byte) *Filter {
	h := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		f.b.Set(f.location(h, i))
	}
	return f
}

// AddString to the Bloom Filter. Returns the filter (allows chaining)
func (f *Filter) AddString(data string) *Filter {
	return f.Add([]byte(data))
}

// Test returns true if the data is in the Filter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *Filter) Test(data []byte) bool {
	h := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		if !f.b.Test(f.location(h, i)) {
			return false
		}
	}
	return true
}

// TestString returns true if the string is in the Filter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *Filter) TestString(data string) bool {
	return f.Test([]byte(data))
}

// TestAndAdd is the equivalent to calling Test(data) then Add(data).
// Returns the result of Test.
func (f *Filter) TestAndAdd(data []byte) bool {
	present := true
	h := baseHashes(data)
	for i := uint(0); i < f.k; i++ {
		l := f.location(h, i)
		if !f.b.Test(l) {
			present = false
		}
		f.b.Set(l)
	}
	return present
}

// TestAndAddString is the equivalent to calling Test(string) then Add(string).
// Returns the result of Test.
func (f *Filter) TestAndAddString(data string) bool {
	return f.TestAndAdd([]byte(data))
}

// ClearAll clears all the data in a Bloom filter, removing all keys
func (f *Filter) ClearAll() *Filter {
	f.b.ClearAll()
	return f
}

// EstimateFalsePositiveRate returns, for a Filter with a estimate of m bits
// and k hash functions, what the false positive rate will be
// while storing n entries; runs 100,000 tests. This is an empirical
// test using integers as keys. As a side-effect, it clears the Filter.
func (f *Filter) EstimateFalsePositiveRate(n uint) (fpRate float64) {
	rounds := uint32(100000)
	f.ClearAll()
	n1 := make([]byte, 4)
	for i := uint32(0); i < uint32(n); i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	fp := 0
	// test for number of rounds
	for i := uint32(0); i < rounds; i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Test(n1) {
			//fmt.Printf("%v failed.\n", i+uint32(n)+1)
			fp++
		}
	}
	fpRate = float64(fp) / (float64(rounds))
	f.ClearAll()
	return
}

// FilterJSON is an unexported type for marshaling/unmarshaling Filter struct.
type FilterJSON struct {
	M uint           `json:"m"`
	K uint           `json:"k"`
	B *bitset.BitSet `json:"b"`
}

// MarshalJSON implements json.Marshaler interface.
func (f *Filter) MarshalJSON() ([]byte, error) {
	return json.Marshal(FilterJSON{f.m, f.k, f.b})
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (f *Filter) UnmarshalJSON(data []byte) error {
	var j FilterJSON
	err := json.Unmarshal(data, &j)
	if err != nil {
		return err
	}
	f.m = j.M
	f.k = j.K
	f.b = j.B
	return nil
}

// WriteTo writes a binary representation of the Filter to an i/o stream.
// It returns the number of bytes written.
func (f *Filter) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(f.m))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(f.k))
	if err != nil {
		return 0, err
	}
	numBytes, err := f.b.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads a binary representation of the Filter (such as might
// have been written by WriteTo()) from an i/o stream. It returns the number
// of bytes read.
func (f *Filter) ReadFrom(stream io.Reader) (int64, error) {
	var m, k uint64
	err := binary.Read(stream, binary.BigEndian, &m)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &k)
	if err != nil {
		return 0, err
	}
	b := &bitset.BitSet{}
	numBytes, err := b.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	f.m = uint(m)
	f.k = uint(k)
	f.b = b
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}

// GobEncode implements gob.GobEncoder interface.
func (f *Filter) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	_, err := f.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GobDecode implements gob.GobDecoder interface.
func (f *Filter) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	_, err := f.ReadFrom(buf)

	return err
}
