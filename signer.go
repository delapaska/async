package async

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ExecutePipeline
func ExecutePipeline(jobs ...job) {

	var wg sync.WaitGroup
	in := make(chan any)

	for _, jobFunc := range jobs {
		wg.Add(1)
		out := make(chan any)

		go func(in, out chan any) {
			defer wg.Done()
			defer close(out)

			jobFunc(in, out)
		}(in, out)

		in = out
	}

	wg.Wait()
}

// SingleHash
/*
SingleHash считает значение crc32(data)+"~"+crc32(md5(data))
( конкатенация двух строк через ~),
где data - то что пришло на вход (по сути - числа из первой функции);
*/
func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for data := range in {
		wg.Add(1)
		dataString := fmt.Sprintf("%v", data)
		md5Data := DataSignerMd5(dataString)
		go calculateSignleHash(&wg, dataString, md5Data, out)

	}
	wg.Wait()
}

func calculateSignleHash(wg *sync.WaitGroup, data string, dataMd5 string, out chan any) {
	defer wg.Done()

	chCrc32 := make(chan string)
	chCrc32Md5 := make(chan string)
	go calculateCRC32(chCrc32, data)
	go calculateCRC32(chCrc32Md5, dataMd5)
	data = <-chCrc32
	dataMd5 = <-chCrc32Md5
	out <- data + "~" + dataMd5

}

func calculateCRC32(ch chan string, data string) {
	ch <- DataSignerCrc32(data)
}

// MultiHash
func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for data := range in {
		wg.Add(1)
		singleHashData := fmt.Sprintf("%v", data)
		go calculateMultiHash(&wg, singleHashData, out)
	}
	wg.Wait()
}
func calculateMultiHash(wg *sync.WaitGroup, data string, out chan any) {
	defer wg.Done()
	results := make([]string, 6)
	var wgTh sync.WaitGroup
	wgTh.Add(6)
	for th := 0; th < 6; th++ {
		go calculateWithTh(&wgTh, th, data, results)
	}
	wgTh.Wait()
	final := strings.Join(results, "")
	out <- final
	fmt.Println("MultiHash result ", final)

}
func calculateWithTh(wg *sync.WaitGroup, th int, data string, results []string) {
	defer wg.Done()
	results[th] = DataSignerCrc32(fmt.Sprintf("%d%v", th, data))
}

// CombineResults
func CombineResults(in, out chan interface{}) {
	var results []string
	for data := range in {
		results = append(results, fmt.Sprintf("%v", data))
	}

	sort.Strings(results)
	finalResult := strings.Join(results, "_")
	out <- finalResult
	fmt.Println("CombineResults result ", finalResult)

}
