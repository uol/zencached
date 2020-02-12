package zencached_test

import (
	"testing"

	"github.com/uol/zencached"
)

func Benchmark(b *testing.B) {

	z := createZencached(nil)
	key := "benchmark"
	value := "benchmark"
	route := []byte{0}

	for n := 0; n < b.N; n++ {
		_, err := z.Storage(zencached.Set, route, key, value, 60)
		if err != nil {
			panic(err)
		}
		_, _, err = z.Get(route, key)
		if err != nil {
			panic(err)
		}
	}
}
