package client

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
	"testing"
	"sync/atomic"
	"net/http"

	"github.com/JohnDing1995/pentadb/client"


)

var (
	numKeys    = flag.Int("keys_mil", 1, "How many million keys to write.")
	valueSize  = flag.Int("valsz", 0, "Value size in bytes.")
	mil        = 1000000
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
	memprofile = flag.String("memprofile", "", "write memory profile to `file`")
	flagValueSize = flag.Int("valsz", 128, "Size of each value.")
)

const Mi int = 1000000
const Mf int = 1000000

type entry struct {
	Key   []byte
	Value []byte
	//Meta  byte
}


type hitCounter struct {
	found    uint64
	notFound uint64
	errored  uint64
}

func (h *hitCounter) Reset() {
	h.found, h.notFound, h.errored = 0, 0, 0
}

func (h *hitCounter) Update(c *hitCounter) {
	atomic.AddUint64(&h.found, c.found)
	atomic.AddUint64(&h.notFound, c.notFound)
	atomic.AddUint64(&h.errored, c.errored)
}

func (h *hitCounter) Print(storeName string, b *testing.B) {
	b.Logf("%s: %d keys had valid values.", storeName, h.found)
	b.Logf("%s: %d keys had no values", storeName, h.notFound)
	b.Logf("%s: %d keys had errors", storeName, h.errored)
	b.Logf("%s: %d total keys looked at", storeName, h.found+h.notFound+h.errored)
	b.Logf("%s: hit rate : %.2f", storeName, float64(h.found)/float64(h.found+h.notFound+h.errored))
}

func fillEntry(e *entry) {
	k := rand.Intn(*numKeys * mil * 10)
	key := fmt.Sprintf("vsz=%05d-k=%010d", *valueSize, k) // 22 bytes.
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	rand.Read(e.Value)
	//e.Meta = 0
}

func newKey() []byte {
	k := rand.Int() % int(*numKeys*Mf)
	key := fmt.Sprintf("vsz=%05d-k=%010d", *flagValueSize, k) // 22 bytes.
	return []byte(key)
}

func getPentaDB() *client.Client{
	var nodes = []string{
		"127.0.0.1:4567",
	}
	penta, _ := client.NewClient(nodes, nil, 1)
	fmt.Printf("PentaDB opened! Start testing")
	return penta
}



func runRandomReadBenchmark(b *testing.B, storeName string, doBench func(*hitCounter, *testing.PB)) {
	counter := &hitCounter{}
	b.Run("read-random"+storeName, func(b *testing.B) {
		counter.Reset()
		b.RunParallel(func(pb *testing.PB) {
			c := &hitCounter{}
			doBench(c, pb)
			counter.Update(c)
		})
	})
	counter.Print(storeName, b)
}

func BenchmarkReadRandomPenta(b *testing.B){
	penta := getPentaDB()
	defer penta.Close()

	runRandomReadBenchmark(b, "pentadb", func(c *hitCounter, pb *testing.PB){
		for pb.Next(){
			key := newKey()
			value := penta.Get(string(key))
			if []byte(value) == nil {
				c.notFound ++
				continue
			}
			c.found ++
		}
	})
}


func BenchmarkWriteRandomPenta(b *testing.B){
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	rand.Seed(time.Now().Unix())

	b.Error(os.RemoveAll("tmp/penta"))
	os.MkdirAll("tmp/penta", 0777)
	penta := getPentaDB()
	defer penta.Close()
	entries := make([]*entry, *numKeys*1000000)
	wstart := time.Now()
	for i := 0; i < len(entries); i++ {
		e := new(entry)
		e.Key = make([]byte, 22)
		e.Value = make([]byte, *valueSize)
		entries[i] = e
	}

	for _, e := range entries {
		fillEntry(e)
		penta.Put(string(e.Key), string(e.Value))
	}
	fmt.Println("Value size:", *valueSize)
	fmt.Println("Total Write Time: ", time.Since(wstart))

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	fmt.Println("Starting read test")

}

func TestMain(m *testing.M) {
	flag.Parse()
	// call flag.Parse() here if TestMain uses flags
	go http.ListenAndServe(":8080", nil)
	os.Exit(m.Run())
}
