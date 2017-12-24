package minhashlsh

import (
	"strconv"
	"testing"
)

func Benchmark_Insert10000(b *testing.B) {
	sigs := make([][]uint64, 10000)
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
