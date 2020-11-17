/*
	Author: myhat123
	改编自golangbot.com中buffer channel中的示例
	对Job, Result数据结构，以及allocate作业分配进行了调整，采用接口的方式以适应多类型数据的处理
	增加了InitChan，是为了大数据量的分批导入处理
*/
package tasks

import (
	//"github.com/ClickHouse/clickhouse-go"
	//"github.com/gocql/gocql"
	"math"
	"reflect"
	"sync"
)

type Job struct {
	id int
	//session *gocql.Session
	data interface{}
}

type Result struct {
	job    Job
	status bool
}

var jobs chan Job
var results chan Result

func worker(wg *sync.WaitGroup) {
	for job := range jobs {
		output := Result{job, writeClickHouseDB(job.data)}
		//fmt.Println("job.id========", job.id)
		results <- output
	}
	wg.Done()
}

func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg)
	}
	wg.Wait()
	close(results)
}

func allocate(data interface{}, page int) {

	v := reflect.ValueOf(data)

	if v.Kind() == reflect.Slice {
		total := v.Len()

		if total <= page {
			job := Job{0, v.Slice(0, total).Interface()}
			jobs <- job
		} else {
			i := 0
			for ; i < total/page; i++ {
				job := Job{i, v.Slice(i*page, (i+1)*page).Interface()}
				jobs <- job
			}

			k := float64(total) / float64(page)
			noOfJobs := int(math.Ceil(k))
			if noOfJobs > total/page {
				job := Job{i, v.Slice((i * page), total).Interface()}
				jobs <- job
			}
		}

		close(jobs)
	}
}

func finish(done chan bool) {
	/*
		for result := range results {
			fmt.Printf("Job %d\n", result.job.id)
		}
		done <- true
	*/

	for _ = range results {
	}
	done <- true
}

func InitChan() {
	jobs = make(chan Job, 200)
	results = make(chan Result, 200)
}

func Start(data interface{}) {
	go allocate(data, 1000)
	done := make(chan bool)
	go finish(done)
	noOfWorkers := 100
	createWorkerPool(noOfWorkers)
	<-done
}
