package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

type wordCount struct{}

func (w wordCount) Map(key, value string, emitter corral.Emitter) {
	re := regexp.MustCompile("[^a-zA-Z0-9\\s]+")

	sanitized := strings.ToLower(re.ReplaceAllString(value, " "))
	//sanitized := value
	for _, word := range strings.Fields(sanitized) {
		if len(word) == 0 {
			continue
		}
		err := emitter.Emit(word, strconv.Itoa(1))
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (w wordCount) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	count := 0
	for val := range values.Iter() {
		c, _ := strconv.Atoi(val)
		count += c
	}
	emitter.Emit(key, strconv.Itoa(count))
}

func main() {
	job := corral.NewJob(wordCount{}, wordCount{})

	options := []corral.Option{
		//corral.WithSplitSize(50 * 1024 * 1024),
		//corral.WithMapBinSize(50 * 1024 * 1024),
		corral.WithSplitSize(50 * 1024 * 1024),
		corral.WithMapBinSize(50 * 1024 * 1024),
		corral.WithLambdaFuncName("WordCountFunc"),
		corral.WithNumReduce(500),
	}

	driver := corral.NewDriver(job, options...)
	driver.Main()
}
