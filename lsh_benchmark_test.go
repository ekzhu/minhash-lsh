package minhashlsh

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var (
	minhashSeed              = 42
	minhashSize              = 128
	canadianOpenDataFilename = os.Getenv("BENCHMARK_SET_FILE")
	threshold                = 0.9
	ignoreSelfPair           = true
)

func Benchmark_Insert10000(b *testing.B) {
	sigs := make([]Signature, 10000)
	for i := range sigs {
		sigs[i] = randomSignature(64, int64(i))
	}
	b.ResetTimer()
	f := NewMinhashLSH16(64, 0.5)
	for i := range sigs {
		f.Add(strconv.Itoa(i), sigs[i])
	}
	f.Index()
}

type valueCountPair struct {
	value string
	count int
}

var valueCountRegex = regexp.MustCompile(`^(?P<value>.*)____(?P<count>[0-9]+)$`)

func (p *valueCountPair) Parse(str string) error {
	indexes := valueCountRegex.FindStringSubmatchIndex(str)
	if indexes == nil || len(indexes) != 6 {
		return errors.New("Incorrect value count pair detected: " + str)
	}
	p.value = str[indexes[2]:indexes[3]]
	var err error
	p.count, err = strconv.Atoi(str[indexes[4]:indexes[5]])
	if err != nil {
		panic(str + "\n" + err.Error())
	}
	return nil
}

type set struct {
	ID     string
	values []string
}

// readSets takes a set file having the following format:
// 1. One set per line
// 2. Each set, all items are separated by whitespaces
// 3. If the parameter firstItemIsID is set to true,
//    the first itme is the unique ID of the set.
// 4. The rest of the items with the following format:
//    <value>____<frequency>
//    * value is an unique element of the set
//    * frequency is an integer count of the occurance of value
//    * ____ (4 underscores) is the separator
func readSets(setFilename string, firstItemIsID bool) <-chan set {
	sets := make(chan set)
	go func() {
		defer close(sets)
		file, err := os.Open(setFilename)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(nil, 1024*1024*1024)
		var count int
		for scanner.Scan() {
			items := strings.Split(scanner.Text(), " ")
			var ID string
			if firstItemIsID {
				ID = items[0]
				items = items[1:]
			} else {
				ID = strconv.Itoa(count)
			}
			values := make([]string, len(items))
			for i, item := range items {
				var pair valueCountPair
				if err := pair.Parse(item); err != nil {
					fmt.Println(items)
					panic(err)
				}
				values[i] = pair.value
			}
			sets <- set{ID, values}
			count++
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}()
	return sets
}

type setSig struct {
	ID        string
	size      int
	signature []uint64
}

func createSigantures(sets <-chan set) <-chan setSig {
	out := make(chan setSig)
	go func() {
		defer close(out)
		for set := range sets {
			mh := NewMinhash(minhashSeed, minhashSize)
			for _, v := range set.values {
				mh.Push([]byte(v))
			}
			out <- setSig{set.ID, len(set.values), mh.Signature()}
		}
	}()
	return out
}

type pair struct {
	ID1 string
	ID2 string
}

func (p *pair) String() string {
	if p.ID1 <= p.ID2 {
		return fmt.Sprintf("%s, %s", p.ID1, p.ID2)
	}
	return fmt.Sprintf("%s, %s", p.ID2, p.ID1)
}

func Benchmark_AllPair(b *testing.B) {
	sets := readSets(canadianOpenDataFilename, true)
	setSigs := make([]setSig, 0)
	for setSig := range createSigantures(sets) {
		setSigs = append(setSigs, setSig)
	}
	b.ResetTimer()

	// Indexing
	lsh := NewMinhashLSH(minhashSize, threshold)
	for _, s := range setSigs {
		lsh.Add(s.ID, s.signature)
	}
	lsh.Index()

	// Querying and output results
	pairs := make(chan pair)
	go func() {
		defer close(pairs)
		for _, s := range setSigs {
			for _, candidateID := range lsh.Query(s.signature) {
				if ignoreSelfPair && candidateID == s.ID {
					continue
				}
				pairs <- pair{s.ID, candidateID}
			}
		}
	}()

	// Output results
	w := bufio.NewWriter(os.Stdout)
	for pair := range pairs {
		w.WriteString(pair.String() + "\n")
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
}
