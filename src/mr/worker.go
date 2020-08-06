package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := DispatchTaskReq{}
	args.PID = os.Getpid()
	isDone := false
	intermediateDir := "."

	for !isDone {
		reply := CallTask(&args)
		if reply == nil {
			time.Sleep(time.Second)
			continue
		}

		switch reply.Task.TaskType {
		case MAP_TASK:
			filename := reply.Task.FileName
			nReduce := reply.Task.NReduce
			err := processMapTask(reply.Task.TaskName, filename, reply.Task.ID, nReduce, mapf, intermediateDir)
			if err == nil {
				completeReq := CompleteTaskReq{
					TaskType: MAP_TASK,
					TaskID:   reply.Task.ID,
				}
				CompleteTaskReport(&completeReq)
			}
		case REDUCE_TASK:
			reduceNO := reply.Task.ID
			err := processReduceTask(reply.Task.TaskName, reduceNO, reply.Task.ReduceTaskFiles, intermediateDir, reducef)
			if err == nil {
				completeReq := CompleteTaskReq{
					TaskType: REDUCE_TASK,
					TaskID:   reply.Task.ID,
				}
				CompleteTaskReport(&completeReq)
			}
		case EXIST_TASK:
			isDone = true
		case WATIT_TASK:
			time.Sleep(time.Second)
		default:
			println("wrong task type: ", reply.Task.TaskType)
		} // switch

	} // for

	//if isDone {
	//	names, err := filepath.Glob("mr-*-*-*")
	//	if err == nil {
	//		for _, name := range names {
	//			os.Remove(name)
	//		}
	//	}
	//
	//	names, err = filepath.Glob("mr-output-*_*")
	//	if err == nil {
	//		for _, name := range names {
	//			os.Remove(name)
	//		}
	//	}
	//}

}

func processMapTask(taskName string, filename string, mapNO int, nReduce int, mapf func(string, string) []KeyValue,
	intermediateDir string) (err error) {

	fileMap := map[string]*os.File{}
	intermediateMap := map[int][]KeyValue{}
	reduceTaskFilesMap := map[int][]string{}

	defer func() {
		if err != nil {
			for _, file := range fileMap {
				os.Remove(file.Name())
			}
		}
	}()

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s, %s", filename, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, val := range kva {
		reduceNO := ihash(val.Key) % nReduce
		intermediateMap[reduceNO] = append(intermediateMap[reduceNO], val)
	}

	for reduceNO, val := range intermediateMap { // create each intermediate file
		oname := fmt.Sprintf("%s-%d-%d", taskName, mapNO, reduceNO)
		os.Remove(oname)
		ofile, errI := ioutil.TempFile(intermediateDir, oname + "_")
		if errI != nil {
			log.Fatalf("cannot create temp file for %v", oname)
			err = errI
			return
		}
		fileMap[oname] = ofile

		enc := json.NewEncoder(ofile)
		for i := 0; i < len(val); i++ {
			kv := val[i]
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode the file for %v", oname)
				return
			}
		}
		reduceTaskFilesMap[reduceNO] = append(reduceTaskFilesMap[reduceNO], oname)
	}

	for nameKey, file := range fileMap {
		os.Rename(file.Name(), fmt.Sprintf("%s/%s", intermediateDir, nameKey))
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close the file for %v", file.Name())
			return
		}
	}
	args := CollectInterFilesReq{SubMapReduceTaskMap: reduceTaskFilesMap}
	SendInterFiles(&args)

	return nil
}

func processReduceTask(taskName string, reduceNO int, files []string, intermediateDir string, reducef func(string, []string) string) (err error) {

	// read files for reduce task reduceNO
	names := files

	intermediate := []KeyValue{}
	for _, fileName := range names {	// read each target file for reduce task reduceNO
		file, errI := os.Open(fileName)
		if errI != nil {
			log.Fatalf("cannot open the file: %v", fileName)
			err = errI
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// sort the kvs
	sort.Sort(ByKey(intermediate))

	// write to the output file
	oname := fmt.Sprintf("mr-output-%d", reduceNO)
	os.Remove(oname)
	ofile, err := ioutil.TempFile(intermediateDir, oname + "_")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), fmt.Sprintf("%s/%s", intermediateDir, oname))
	for _, fileName := range files {
		os.Remove(fileName)
	}

	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		println("dialing:", err)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		rep, _ := json.Marshal(reply)
		println("rpc reply: ", string(rep))
		return true
	}
	fmt.Println(err)
	return false
}
