package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
)

type config struct {
	printTime    time.Duration
	generateTime time.Duration
	workerCount  int
}

var (
	batchSize      = 10
	accumulatedSum int64
)

func gen(batchSize int) []int {
	packet := make([]int, batchSize)

	for i := 0; i < batchSize; i++ {
		packet[i] = rand.Intn(100)
	}
	return packet
}

func worker(data []int, resultChannel chan<- []int) {
	result := findTopKBySorting(data, 3)
	accumulator(result)
	resultChannel <- result
}

func accumulator(data []int) {
	sum := 0
	for _, value := range data {
		sum += value
	}
	atomic.AddInt64(&accumulatedSum, int64(sum))

}

func findTopKBySorting(slice []int, k int) []int {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] > slice[j]
	})
	return slice[:k]
}

func readConfig(path string) *config {
	cfg := config{}
	err := godotenv.Load(path)
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	N, err := strconv.Atoi(os.Getenv("N"))
	if err != nil || N <= 0 {
		log.Fatalf("Invalid value for N: %v", os.Getenv("N"))
	}
	cfg.generateTime = time.Duration(N) * time.Millisecond

	M, err := strconv.Atoi(os.Getenv("M"))
	if err != nil || M <= 0 {
		log.Fatalf("Invalid value for M: %v", os.Getenv("M"))
	}
	cfg.workerCount = M

	K, err := strconv.Atoi(os.Getenv("K"))
	if err != nil || K <= 0 {
		log.Fatalf("Invalid value for K: %v", os.Getenv("K"))
	}
	cfg.printTime = time.Duration(K) * time.Second

	return &cfg
}

func main() {
	cfg := readConfig("C:/Users/USER/go/src/github.com/zakether/marketlab-test/.env")

	dataChannel := make(chan []int)
	resultChannel := make(chan []int, cfg.workerCount)

	go func(d time.Duration) {
		for range time.Tick(d) {
			dataChannel <- gen(batchSize)
		}
	}(cfg.generateTime)

	go func() {
		for data := range dataChannel {
			worker(data, resultChannel)
		}
	}()

	go func(d time.Duration) {
		for range time.Tick(d) {
			fmt.Println(atomic.LoadInt64(&accumulatedSum))
		}
	}(cfg.printTime)

	for r := range resultChannel {
		_ = r
	}
}
