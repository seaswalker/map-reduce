package mapreduce

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sort"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	reduced := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		intermediateReduceFileName := reduceName(jobName, i, reduceTask)

		err := handleOneIntermediateReduceFile(intermediateReduceFileName, reduced)
		if err != nil {
			log.Printf("Reduce中间文件打开失败，文件名: %s，error: %s.\n", intermediateReduceFileName, err)
		}
	}

	sortAllValues(reduced)

	result := callDoReduce(reduced, reduceF)

	writeToReducedFile(result, outFile)

	log.Printf("写入到reduce文件完成，文件名: %s.\n", outFile)
}

// 读取一个临时文件
func handleOneIntermediateReduceFile(fileName string, reduced map[string][]string) (error) {
	fi, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer fi.Close()

	scanner := bufio.NewScanner(fi)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.Split(line, "=")
		values, ok := reduced[parts[0]]

		if !ok {
			values := []string{parts[1]}
			reduced[parts[0]] = values
		} else {
			values = append(values, parts[1])
		}
	}

	return nil
}

// 对每个key的所有value进行排序
func sortAllValues(reduced map[string][]string) {
	for _, value := range reduced {
		sort.Slice(value, func(i, j int) bool {
			return strings.Compare(value[i], value[j]) > 0
		})
	}
}

func callDoReduce(reduced map[string][]string, reduceF func(key string, values []string) string) (map[string]string) {
	result := make(map[string]string)

	for key, value := range reduced {
		userReduced := reduceF(key, value)
		result[key] = userReduced
	}

	return result
}

func writeToReducedFile(result map[string]string, outFile string) {
	fi, err := os.Create(outFile)

	if err != nil {
		log.Panicf("创建reduce文件失败，文件名: %s，错误码: %s\n.", outFile, err)
	}

	defer fi.Close()

	enc := json.NewEncoder(fi)

	for key, value := range result {
		enc.Encode(KeyValue{key, value})
	}
}