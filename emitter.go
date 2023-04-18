package corral

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/corfs"
)

// Emitter enables mappers and reducers to yield key-value pairs.
type Emitter interface {
	Emit(key, value string) error
	close() error
	bytesWritten() int64
	setPartitionFunc(partitionFunc PartitionFunc)
	getLengths() []int
}

// reducerEmitter is a threadsafe emitter.
type reducerEmitter struct {
	writer       io.WriteCloser
	mut          *sync.Mutex
	writtenBytes int64
}

// newReducerEmitter initializes and returns a new reducerEmitter
func newReducerEmitter(writer io.WriteCloser) *reducerEmitter {
	return &reducerEmitter{
		writer: writer,
		mut:    &sync.Mutex{},
	}
}

// Emit yields a key-value pair to the framework.
func (e *reducerEmitter) Emit(key, value string) error {
	e.mut.Lock()
	defer e.mut.Unlock()

	n, err := e.writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
	e.writtenBytes += int64(n)
	return err
}

// close terminates the reducerEmitter. close must not be called more than once
func (e *reducerEmitter) close() error {
	return e.writer.Close()
}

func (e *reducerEmitter) bytesWritten() int64 {
	return e.writtenBytes
}

func (e *reducerEmitter) setPartitionFunc(partitionFunc PartitionFunc) {
	// skip
}

func (e *reducerEmitter) getLengths() []int {
	return nil
}

// mapperEmitter is an emitter that partitions keys written to it.
// mapperEmitter maintains a map of writers. Keys are partitioned into one of numBins
// intermediate "shuffle" bins. Each bin is written as a separate file.
type mapperEmitter struct {
	numBins       uint                    // number of intermediate shuffle bins
	writers       map[uint]io.WriteCloser // maps a parition number to an open writer
	fs            corfs.FileSystem        // filesystem to use when opening writers
	mapperID      uint                    // numeric identifier of the mapper using this emitter
	outDir        string                  // folder to save map output to
	partitionFunc PartitionFunc           // PartitionFunc to use when partitioning map output keys into intermediate bins
	writtenBytes  int64                   // counter for number of bytes written from emitted key/val pairs
}

func (me *mapperEmitter) setPartitionFunc(partitionFunc PartitionFunc) {
	//TODO implement me
	me.partitionFunc = partitionFunc
}

// Initializes a new mapperEmitter
func newMapperEmitter(numBins uint, mapperID uint, outDir string, fs corfs.FileSystem) mapperEmitter {
	return mapperEmitter{
		numBins:       numBins,
		writers:       make(map[uint]io.WriteCloser, numBins),
		fs:            fs,
		mapperID:      mapperID,
		outDir:        outDir,
		partitionFunc: hashPartition,
	}
}

// hashPartition partitions a key to one of numBins shuffle bins
func hashPartition(key string, numBins uint) uint {
	h := fnv.New64()
	h.Write([]byte(key))
	return uint(h.Sum64() % uint64(numBins))
}

// Emit yields a key-value pair to the framework.
func (me *mapperEmitter) Emit(key, value string) error {
	bin := me.partitionFunc(key, me.numBins)

	// Open writer for the bin, if necessary
	writer, exists := me.writers[bin]
	if !exists {
		var err error
		path := me.fs.Join(me.outDir, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_map_%d_reduce_%d.data", defaultShuffleID, me.mapperID, bin))

		writer, err = me.fs.OpenWriter(path)
		if err != nil {
			return err
		}
		me.writers[bin] = writer
	}

	var err error
	var n int
	if shuffleOutType == "json" {
		// JSON格式的序列化
		kv := keyValue{
			Key:   key,
			Value: value,
		}

		data, err := json.Marshal(kv)
		if err != nil {
			log.Error(err)
			return err
		}

		data = append(data, '\n')
		n, err = writer.Write(data)
		me.writtenBytes += int64(n)
	} else {
		n, err = writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
		me.writtenBytes += int64(n)
	}
	return err
}

// close terminates the mapperEmitter. Must not be called more than once
func (me *mapperEmitter) close() error {
	errs := make([]string, 0)
	for _, writer := range me.writers {
		err := writer.Close()
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}

	return nil
}

func (me *mapperEmitter) bytesWritten() int64 {
	return me.writtenBytes
}

func (me *mapperEmitter) getLengths() []int {
	return nil
}

// SingleFileMapperEmitter
// 每个Map生成一个Shuffle数据文件ShuffleID_MapID.data，一个索引文件ShuffleID_MapID.index （指向每个分区的起始位置和终止位置)
type SingleFileMapperEmitter struct {
	numPartitions uint              // number of partitions
	writer        io.WriteCloser    // maps a parition number to an open writer
	fs            corfs.FileSystem  // filesystem to use when opening writers
	mapperID      uint              // numeric identifier of the mapper using this emitter
	outDir        string            // folder to save map output to
	partitionFunc PartitionFunc     // PartitionFunc to use when partitioning map output keys into intermediate bins
	writtenBytes  int64             // counter for number of bytes written from emitted key/val pairs
	dataBuffer    map[uint][][]byte // 所有需要写出的数据，key是partitionID，value是待写出的Record
	lengths       []int             // index文件，每个分区数据块的起始偏移量和结束偏移量
}

func (me *SingleFileMapperEmitter) setPartitionFunc(partitionFunc PartitionFunc) {
	me.partitionFunc = partitionFunc
}

// Initializes a new SingleFileMapperEmitter
func newSingleFileMapperEmitter(numPartitions uint, mapperID uint, outDir string, fs corfs.FileSystem) SingleFileMapperEmitter {
	return SingleFileMapperEmitter{
		numPartitions: numPartitions,
		fs:            fs,
		mapperID:      mapperID,
		outDir:        outDir,
		partitionFunc: hashPartition,
		dataBuffer:    make(map[uint][][]byte, numPartitions),
		lengths:       make([]int, 0),
	}
}

// Emit yields a key-value pair to the framework.
func (me *SingleFileMapperEmitter) Emit(key, value string) error {
	partitionID := me.partitionFunc(key, me.numPartitions)

	var err error
	var data []byte
	if shuffleOutType == "json" {
		// JSON格式的序列化
		kv := keyValue{
			Key:   key,
			Value: value,
		}

		data, err = json.Marshal(kv)
		if err != nil {
			log.Error(err)
			return err
		}
		data = append(data, '\n')
	} else {
		data = []byte(fmt.Sprintf("%s\t%s\n", key, value))
	}

	if _, ok := me.dataBuffer[partitionID]; !ok {
		me.dataBuffer[partitionID] = make([][]byte, 0)
	}

	me.dataBuffer[partitionID] = append(me.dataBuffer[partitionID], data)

	return err
}

// 写出Shuffle Data文件和index文件
func (me *SingleFileMapperEmitter) close() error {
	// Open writer for the bin, if necessary
	if me.writer == nil {
		var err error
		path := me.fs.Join(me.outDir, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.data", defaultShuffleID, me.mapperID))

		me.writer, err = me.fs.OpenWriter(path)
		if err != nil {
			return err
		}
	}

	errs := make([]string, 0)

	var currentOffset = 0
	me.lengths = append(me.lengths, currentOffset)
	var partitionID uint = 0
	for ; partitionID < me.numPartitions; partitionID++ {
		datas, ok := me.dataBuffer[partitionID]
		if !ok {
			me.lengths = append(me.lengths, currentOffset)
			continue
		}
		for _, data := range datas {
			n, err := me.writer.Write(data)
			if err != nil {
				errs = append(errs, err.Error())
			}
			me.writtenBytes += int64(n)
			currentOffset += n
		}
		me.lengths = append(me.lengths, currentOffset)
	}

	//// 如果是在本地运行，index数据直接写出到本地的文件
	//// 如果是在lambda运行，index数据传回driver，由driver写出到本地
	//if runningLocal() {
	//	err := me.writeToIndexFile(me.lengths)
	//	if err != nil {
	//		errs = append(errs, err.Error())
	//	}
	//}

	err := me.writeToIndexFile(me.lengths)
	if err != nil {
		errs = append(errs, err.Error())
	}

	err = me.writer.Close()
	if err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		log.Warn(errors.New(strings.Join(errs, "\n")))
	}

	return nil
}

func (me *SingleFileMapperEmitter) bytesWritten() int64 {
	return me.writtenBytes
}

func (me *SingleFileMapperEmitter) writeToIndexFile(lengths []int) error {
	indexPath := me.fs.Join(me.outDir, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.index", defaultShuffleID, me.mapperID))
	indexWriter, err := me.fs.OpenWriter(indexPath)
	if err != nil {
		return err
	}

	for _, offset := range lengths {
		indexWriter.Write([]byte(fmt.Sprintf("%s\n", strconv.Itoa(offset))))
	}

	err = indexWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func (me *SingleFileMapperEmitter) getLengths() []int {
	return me.lengths
}
