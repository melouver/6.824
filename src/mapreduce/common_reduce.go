package mapreduce

import (
	"encoding/json"
	"bufio"
	"os"
	"fmt"
	"sort"
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

	var kvs[]KeyValue
	// 从所有map的结果中提取出所有kv对
	for i := 0; i < nMap; i++ {
		fs := reduceName(jobName, i, reduceTask)
		f, e := os.Open(fs)
		if e != nil {
			fmt.Printf("error in open map file" + e.Error())
			panic(e)
			return
		}
		r := bufio.NewReader(f)
		dec := json.NewDecoder(r)

		for {
			var kv KeyValue
			e = dec.Decode(&kv)
			if e != nil {
				break;
			}
			kvs = append(kvs, kv)
		}
	}

	//排序，相同的key会排在一起
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	var values[]string
	var retkvs[]KeyValue
	last := kvs[0].Key
	//把相同的key的value组在一起，传给reduceF
	for _, kv := range kvs {
		if kv.Key == last {
			values = append(values, kv.Value)
		} else {
			retkvs = append(retkvs,
				KeyValue{last, reduceF(last, values)})
			last = kv.Key
			values = nil
			values = append(values, kv.Value)
		}
	}
	// 为了末尾的key
	retkvs = append(retkvs,
		KeyValue{last, reduceF(last, values)})

	f, e := os.Create(outFile)
	if e != nil {
		panic(e)
	}
	enc := json.NewEncoder(f)
	for _, p := range retkvs {
		enc.Encode(p)
	}
	f.Close()

}
