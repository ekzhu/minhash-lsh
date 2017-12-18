package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	minhashlsh "github.com/ekzhu/minhash-lsh"
)

var (
	setFilename    string
	minhashSeed    int64
	minhashSize    int
	threshold      float64
	outputSelfPair bool
	hasID          bool
)

func main() {
	flag.StringVar(&setFilename, "input", "", "The set file as input")
	flag.Int64Var(&minhashSeed, "seed", 42, "The Minhash seed")
	flag.IntVar(&minhashSize, "sigsize", 128,
		"The Minhash signature size in number of hash functions")
	flag.Float64Var(&threshold, "threshold", 0.9, "The Jaccard similarity threshold")
	flag.BoolVar(&outputSelfPair, "selfpair", false, "Allow self-pair in results")
	flag.BoolVar(&hasID, "hasIDfield", true, "The input set file has ID field in the beginning of each line")
	flag.Parse()

	// Create Minhash signatures
	start := time.Now()
	sets := readSets(setFilename, hasID)
	setSigs := make([]setSig, 0)
	for setSig := range createSigantures(sets) {
		setSigs = append(setSigs, setSig)
	}
	signatureCreationTime := time.Now().Sub(start)
	fmt.Fprintf(os.Stderr, "Creating Minhash signature time: %.2f seconds\n", signatureCreationTime.Seconds())

	// Indexing
	start = time.Now()
	lsh := minhashlsh.NewMinhashLSH(minhashSize, threshold)
	for _, s := range setSigs {
		lsh.Add(s.ID, s.signature)
	}
	lsh.Index()
	indexingTime := time.Now().Sub(start)
	fmt.Fprintf(os.Stderr, "Indexing time: %.2f seconds\n", indexingTime.Seconds())

	// Querying and output results
	start = time.Now()
	pairs := make(chan pair)
	go func() {
		defer close(pairs)
		for _, s := range setSigs {
			for _, candidateID := range lsh.Query(s.signature) {
				if !outputSelfPair && candidateID == s.ID {
					continue
				}
				pairs <- pair{s.ID, candidateID.(string)}
			}
		}
	}()
	w := bufio.NewWriter(os.Stdout)
	for pair := range pairs {
		w.WriteString(pair.String() + "\n")
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
	searchTime := time.Now().Sub(start)
	fmt.Fprintf(os.Stderr, "All pair search time: %.2f seconds\n", searchTime.Seconds())
}

func pointquery() {
	panic("Not implemented")
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
		scanner.Buffer(nil, 4096*1024*1024*8)
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
			mh := minhashlsh.NewMinhash(minhashSeed, minhashSize)
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
