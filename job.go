package corral

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	pathlib "path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// Job is the logical container for a MapReduce job
type Job struct {
	Map           Mapper
	Reduce        Reducer
	PartitionFunc PartitionFunc

	fileSystem       corfs.FileSystem
	config           *config
	intermediateBins uint
	outputPath       string

	mapBytesRead        int64
	mapBytesWritten     int64
	mergeBytesRead      int64
	mergeBytesWritten   int64
	reduceBytesRead     int64
	reduceBytesWritten  int64
	combineBytesRead    int64
	combineBytesWritten int64

	cloudFuncRunningTime int64
	cloudFuncNum         int

	mapperCompletedChan chan string
}

// Logic for running a single map task
func (j *Job) runMapper(mapperID uint, splits []inputSplit) ([]int, error) {
	var emitter Emitter
	if jobShuffleMode == WriteCombine || jobShuffleMode == Merge || jobShuffleMode == MergeAndDivide || jobShuffleMode == LSMCombine {
		singleFileMapperEmitter := newSingleFileMapperEmitter(j.intermediateBins, mapperID, j.outputPath, j.fileSystem)
		emitter = &singleFileMapperEmitter
	} else {
		generalMapperEmitter := newMapperEmitter(j.intermediateBins, mapperID, j.outputPath, j.fileSystem)
		emitter = &generalMapperEmitter
	}

	if j.PartitionFunc != nil {
		emitter.setPartitionFunc(j.PartitionFunc)
	}

	for _, split := range splits {
		err := j.runMapperSplit(split, emitter)
		if err != nil {
			return nil, err
		}
	}

	err := emitter.close()
	atomic.AddInt64(&j.mapBytesWritten, emitter.bytesWritten())

	return emitter.getLengths(), err
}

func splitInputRecord(record string) *keyValue {
	fields := strings.Split(record, "\t")
	if len(fields) == 2 {
		return &keyValue{
			Key:   fields[0],
			Value: fields[1],
		}
	}
	return &keyValue{
		Value: record,
	}
}

// runMapperSplit runs the mapper on a single inputSplit
func (j *Job) runMapperSplit(split inputSplit, emitter Emitter) error {
	offset := split.StartOffset
	if split.StartOffset != 0 {
		offset--
	}

	inputSource, err := j.fileSystem.OpenReader(split.Filename, split.StartOffset)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputSource)
	var bytesRead int64
	splitter := countingSplitFunc(bufio.ScanLines, &bytesRead)
	scanner.Split(splitter)

	if split.StartOffset != 0 {
		scanner.Scan()
	}

	for scanner.Scan() {
		record := scanner.Text()
		kv := splitInputRecord(record)
		j.Map.Map(kv.Key, kv.Value, emitter)

		// Stop reading when end of inputSplit is reached
		pos := bytesRead
		if split.Size() > 0 && pos > split.Size() {
			break
		}
	}

	atomic.AddInt64(&j.mapBytesRead, bytesRead)

	return nil
}

// Logic for running a single reduce task
func (j *Job) runReducer(binID uint) error {
	// Determine the intermediate data files this reducer is responsible for
	var path string
	if jobShuffleMode == MergeAndDivide {
		path = j.fileSystem.Join(j.outputPath, "Shuffle_merge", "Reduce_"+strconv.Itoa(int(binID)), "*.data")
	} else if jobShuffleMode == Merge {
		path = j.fileSystem.Join(j.outputPath, "Shuffle_merge", "Merge", "*.data")
	} else if jobShuffleMode == General {
		// 一个Map生成R个文件时
		path = j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_map_*_reduce_%d.data", defaultShuffleID, binID))
	} else if jobShuffleMode == WriteCombine {
		// 一个Map生成一个文件时
		path = j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_*.data", defaultShuffleID))
	} else if jobShuffleMode == LSMCombine {
		path = j.fileSystem.Join(j.outputPath, "Shuffle_origin", "*.data")
	} else {
		return errors.New("Unsupport Shuffle Mode\n")
	}
	files, err := j.fileSystem.ListFiles(path)
	if err != nil {
		return err
	}

	// Open emitter for output data
	path = j.fileSystem.Join(j.outputPath, "Output", fmt.Sprintf("output-part-%d", binID))
	emitWriter, err := j.fileSystem.OpenWriter(path)
	defer emitWriter.Close()
	if err != nil {
		return err
	}

	data := make(map[string][]string, 0)
	var bytesRead int64 = 0

	re := regexp.MustCompile(`[0-9]+`)
	for _, file := range files {
		var startOffset = int64(0)
		var endOffset = file.Size
		if jobShuffleMode == WriteCombine {
			mapID, _ := strconv.Atoi(re.FindAllString(pathlib.Base(file.Name), -1)[1])
			indexPath := j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.index", defaultShuffleID, mapID))
			startOffset, endOffset, err = j.readOffsetByPartitionID(indexPath, binID)
			if err != nil {
				return err
			}
		} else if jobShuffleMode == Merge || jobShuffleMode == LSMCombine {
			// 通过拼接字符串，把/xxx.data生成/xxx.index文件路径
			indexPath := file.Name[0:len(file.Name)-5] + ".index"
			startOffset, endOffset, err = j.readOffsetByPartitionID(indexPath, binID)
			if err != nil {
				return err
			}
		}

		reader, err := j.fileSystem.OpenReader(file.Name, startOffset)
		bytesRead += endOffset - startOffset
		if err != nil {
			return err
		}

		if shuffleOutType == "json" {
			//json格式序列化
			//Feed intermediate data into reducers
			decoder := json.NewDecoder(reader)
			for decoder.More() && decoder.InputOffset() <= endOffset {
				var kv keyValue
				if err := decoder.Decode(&kv); err != nil {
					return err
				}

				if _, ok := data[kv.Key]; !ok {
					data[kv.Key] = make([]string, 0)
				}

				data[kv.Key] = append(data[kv.Key], kv.Value)
			}
			reader.Close()
		} else {
			// 按行读取
			var currentOffset = startOffset
			lineReader := bufio.NewReader(reader)

			for currentOffset < endOffset {
				line, _, err := lineReader.ReadLine()
				if err == io.EOF {
					break
				}
				currentOffset += int64(len(line)) + 1 // 1是换行符的大小，读出来会自动略去换行符
				kv := strings.Split(string(line), "\t")
				if _, ok := data[kv[0]]; !ok {
					data[kv[0]] = make([]string, 0)
				}

				data[kv[0]] = append(data[kv[0]], kv[1])
			}
			reader.Close()
		}

		// Delete intermediate map data
		if j.config.Cleanup {
			err := j.fileSystem.Delete(file.Name)
			if err != nil {
				log.Error(err)
			}
		}
	}

	var waitGroup sync.WaitGroup
	sem := semaphore.NewWeighted(10)

	emitter := newReducerEmitter(emitWriter)
	for key, values := range data {
		sem.Acquire(context.Background(), 1)
		waitGroup.Add(1)
		go func(key string, values []string) {
			defer sem.Release(1)

			keyChan := make(chan string)
			keyIter := newValueIterator(keyChan)

			go func() {
				defer waitGroup.Done()
				j.Reduce.Reduce(key, keyIter, emitter)
			}()

			for _, value := range values {
				// Pass current value to the appropriate key channel
				keyChan <- value
			}
			close(keyChan)
		}(key, values)
	}

	waitGroup.Wait()

	atomic.AddInt64(&j.reduceBytesWritten, emitter.bytesWritten())
	atomic.AddInt64(&j.reduceBytesRead, bytesRead)

	return nil
}

// Logic for running a single combine task
// 读取n个Shuffle文件，合并为一个
func (j *Job) runCombiner(outputPath string, files []string) error {
	// 传进来的都是.data文件

	// 打开输入文件的readers
	indexReaders := make(map[int]*bufio.Reader)
	inputDataReaders := make(map[int]*bufio.Reader)
	perFileOffset := make(map[int]int) // 每个Shuffle文件读到哪里了（读取index文件得来的数据）

	// 初始化各种原Shuffle文件的Reader
	for fileID, file := range files {
		indexPath := file[0:len(file)-5] + ".index"
		indexR, err := j.fileSystem.OpenReader(indexPath, 0)
		if err != nil {
			log.Errorf("Error occur in Merge phase when open reader of %v\n", indexPath)
		}
		indexReaders[fileID] = bufio.NewReader(indexR)

		dataR, err := j.fileSystem.OpenReader(file, 0)
		if err != nil {
			log.Errorf("Error occur in Merge phase when open reader of %v\n", file)
		}
		inputDataReaders[fileID] = bufio.NewReader(dataR)

		offset, _, _ := indexReaders[fileID].ReadLine()
		perFileOffset[fileID], _ = strconv.Atoi(string(offset))
	}

	// Open emitter for output data
	//outputPath := j.fileSystem.Join(j.outputPath, "Shuffle_combine", fmt.Sprintf("combine_%v.data", outFileName))
	emitWriter, err := j.fileSystem.OpenWriter(outputPath)
	emitter := newReducerEmitter(emitWriter)
	defer emitWriter.Close()
	if err != nil {
		return err
	}

	var bytesRead int64 = 0
	var bytesWritten int64 = 0
	var lengths []int

	var waitGroup sync.WaitGroup
	for partitionID := 0; partitionID < j.config.NumReduce; partitionID++ {
		data := make(map[string][]string, 0)
		for fileID, _ := range files {
			var startOffset int64 = int64(perFileOffset[fileID])
			endOffsetString, _, _ := indexReaders[fileID].ReadLine()
			endOffsetInt, _ := strconv.Atoi(string(endOffsetString))
			endOffset := int64(endOffsetInt)
			bytesRead += endOffset - startOffset

			if shuffleOutType == "json" {
				//json格式序列化
				//Feed intermediate data into reducers
				decoder := json.NewDecoder(inputDataReaders[fileID])
				for decoder.More() && decoder.InputOffset() <= endOffset {
					var kv keyValue
					if err := decoder.Decode(&kv); err != nil {
						return err
					}

					if _, ok := data[kv.Key]; !ok {
						data[kv.Key] = make([]string, 0)
					}

					data[kv.Key] = append(data[kv.Key], kv.Value)
				}
			} else {
				// 按行读取
				var currentOffset = startOffset
				lineReader := bufio.NewReader(inputDataReaders[fileID])

				for currentOffset < endOffset {
					line, _, err := lineReader.ReadLine()
					if err == io.EOF {
						break
					}
					currentOffset += int64(len(line)) + 1 // 1是换行符的大小，读出来会自动略去换行符
					kv := strings.Split(string(line), "\t")
					if _, ok := data[kv[0]]; !ok {
						data[kv[0]] = make([]string, 0)
					}

					data[kv[0]] = append(data[kv[0]], kv[1])
				}
			}
			perFileOffset[fileID] = endOffsetInt
		}

		waitGroup.Wait()
		lengths = append(lengths, int(emitter.bytesWritten()))

		sem := semaphore.NewWeighted(10)
		for key, values := range data {
			sem.Acquire(context.Background(), 1)
			waitGroup.Add(1)
			go func(key string, values []string) {
				defer sem.Release(1)

				keyChan := make(chan string)
				keyIter := newValueIterator(keyChan)

				go func() {
					defer waitGroup.Done()
					j.Reduce.Reduce(key, keyIter, emitter)
				}()

				for _, value := range values {
					// Pass current value to the appropriate key channel
					keyChan <- value
				}
				close(keyChan)
			}(key, values)
		}
	}

	waitGroup.Wait()
	lengths = append(lengths, int(emitter.bytesWritten()))

	emitter.close()
	emitWriter.Close()

	// 写出index文件
	indexPath := outputPath[0:len(outputPath)-5] + ".index"
	indexWriter, err := j.fileSystem.OpenWriter(indexPath)
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

	// Delete intermediate map data
	if clearLsmCombineIntermediateFiles {
		for _, file := range files {
			err := j.fileSystem.Delete(file)
			if err != nil {
				log.Error(err)
			}
			indexPath := file[0:len(file)-5] + ".index"
			err = j.fileSystem.Delete(indexPath)
			if err != nil {
				log.Error(err)
			}
		}
	}

	bytesWritten += emitter.bytesWritten()
	atomic.AddInt64(&j.combineBytesWritten, emitter.bytesWritten())
	atomic.AddInt64(&j.combineBytesRead, bytesRead)

	return nil
}

func (j *Job) readOffsetByPartitionID(indexPath string, partitionID uint) (int64, int64, error) {
	indexR, err := j.fileSystem.OpenReader(indexPath, 0)
	if err != nil {
		fmt.Printf(err.Error())
		return -1, -1, err
	}
	reader := bufio.NewReader(indexR)
	var rid uint

	var startOffset int64 = -1
	var endOffset int64 = -1
	for rid = 0; rid <= uint(j.config.NumReduce); rid++ {
		line, _, err := reader.ReadLine()
		if err != nil {
			fmt.Printf(err.Error())
			return -1, -1, err
		}
		if rid < partitionID {
			continue
		} else if rid == partitionID {
			startOffsetTmp, _ := strconv.Atoi(string(line))
			startOffset = int64(startOffsetTmp)
		} else if rid == partitionID+1 {
			endOffsetTmp, _ := strconv.Atoi(string(line))
			endOffset = int64(endOffsetTmp)
		} else {
			break
		}
	}
	err = indexR.Close()
	return startOffset, endOffset, err
}

// Logic for running a single reduce task
func (j *Job) runMerger(taskID int, startFileID int, endFileID int) error {
	if startFileID > endFileID {
		log.Errorf("Error occur in run merge task, recevice merge task file from %d to %d\n", startFileID, endFileID)
		return nil
	}

	var bytesRead int64 = 0
	var bytesWritten int64 = 0

	indexReaders := make(map[int]*bufio.Reader)
	dataReaders := make(map[int]*bufio.Reader)
	writers := make(map[int]io.WriteCloser)  // ShuffleMode==MergeAndDivide时，需要有R个writer
	var dataWriterOnMergeMode io.WriteCloser // ShuffleMode==Merge时，只输出一个Shuffle文件，只需要一个writer
	perFileOffset := make(map[int]int)       // 每个Shuffle文件读到哪里了（读取index文件得来的数据）

	// 初始化各种原Shuffle文件的Reader
	for fileID := startFileID; fileID <= endFileID; fileID++ {
		indexPath := j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.index", defaultShuffleID, fileID))
		indexR, err := j.fileSystem.OpenReader(indexPath, 0)
		if err != nil {
			log.Errorf("Error occur in Merge phase when open reader of %v\n", indexPath)
		}
		indexReaders[fileID] = bufio.NewReader(indexR)

		dataPath := j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.data", defaultShuffleID, fileID))
		dataR, err := j.fileSystem.OpenReader(dataPath, 0)
		if err != nil {
			log.Errorf("Error occur in Merge phase when open reader of %v\n", dataPath)
		}
		dataReaders[fileID] = bufio.NewReader(dataR)

		offset, _, _ := indexReaders[fileID].ReadLine()
		perFileOffset[fileID], _ = strconv.Atoi(string(offset))
	}

	// 初始化Writer指针
	if jobShuffleMode == MergeAndDivide {
		for partitionID := 0; partitionID < j.config.NumReduce; partitionID++ {
			outPath := j.fileSystem.Join(j.outputPath, "Shuffle_merge", "Reduce_"+strconv.Itoa(partitionID), fmt.Sprintf("%v-%v.data", startFileID, endFileID))
			writers[partitionID], _ = j.fileSystem.OpenWriter(outPath)
		}
	} else if jobShuffleMode == Merge {
		outPath := j.fileSystem.Join(j.outputPath, "Shuffle_merge", "Merge", fmt.Sprintf("%v-%v.data", startFileID, endFileID))
		dataWriterOnMergeMode, _ = j.fileSystem.OpenWriter(outPath)
	}

	var waitGroup sync.WaitGroup
	lengths := make([]int, 0)
	currentOffset := 0
	lengths = append(lengths, currentOffset)
	for partitionID := 0; partitionID < j.config.NumReduce; partitionID++ {
		var datas []byte
		for fileID := startFileID; fileID <= endFileID; fileID++ {
			nextOffsetBytes, _, _ := indexReaders[fileID].ReadLine()
			nextOffset, _ := strconv.Atoi(string(nextOffsetBytes))
			len := nextOffset - perFileOffset[fileID]
			if len == 0 {
				continue
			}
			bytesBuffer := make([]byte, len)
			n, _ := io.ReadFull(dataReaders[fileID], bytesBuffer)
			if n != len {
				log.Errorf("Error occur in run merger, Read File Partition Size Not Equal, Read partition:%d, file:%d, should read size:%d, real read size:%d\n", partitionID, fileID, len, n)
			}
			perFileOffset[fileID] += n
			datas = append(datas, bytesBuffer...)
			bytesRead = bytesRead + int64(len)
		}
		if jobShuffleMode == MergeAndDivide {
			waitGroup.Add(1)
			go func(writer io.WriteCloser, datas []byte) {
				writer.Write(datas)
				writer.Close()
				waitGroup.Done()
			}(writers[partitionID], datas)
		} else if jobShuffleMode == Merge {
			dataWriterOnMergeMode.Write(datas)
			currentOffset += len(datas)
			lengths = append(lengths, currentOffset)
		}
		bytesWritten = bytesWritten + int64(len(datas))
	}

	waitGroup.Wait()

	if jobShuffleMode == Merge {
		dataWriterOnMergeMode.Close()

		// 写出index文件
		indexPath := j.fileSystem.Join(j.outputPath, "Shuffle_merge", "Merge", fmt.Sprintf("%v-%v.index", startFileID, endFileID))
		indexWriter, err := j.fileSystem.OpenWriter(indexPath)
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
	}

	// Delete intermediate map data
	if j.config.Cleanup {
		for fileID := startFileID; fileID <= endFileID; fileID++ {
			indexPath := j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.index", defaultShuffleID, fileID))
			dataPath := j.fileSystem.Join(j.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.data", defaultShuffleID, fileID))
			err := j.fileSystem.Delete(indexPath)
			if err != nil {
				log.Error(err)
			}
			err = j.fileSystem.Delete(dataPath)
			if err != nil {
				log.Error(err)
			}
		}
	}

	atomic.AddInt64(&j.mergeBytesRead, bytesRead)
	atomic.AddInt64(&j.mergeBytesWritten, bytesWritten)
	return nil
}

// inputSplits calculates all input files' inputSplits.
// inputSplits also determines and saves the number of intermediate bins that will be used during the shuffle.
func (j *Job) inputSplits(inputs []string, maxSplitSize int64) []inputSplit {
	files := make([]string, 0)
	for _, inputPath := range inputs {
		fileInfos, err := j.fileSystem.ListFiles(inputPath)
		if err != nil {
			log.Warn(err)
			continue
		}

		for _, fInfo := range fileInfos {
			files = append(files, fInfo.Name)
		}
	}

	splits := make([]inputSplit, 0)
	var totalSize int64
	for _, inputFileName := range files {
		fInfo, err := j.fileSystem.Stat(inputFileName)
		if err != nil {
			log.Warnf("Unable to load input file: %s (%s)", inputFileName, err)
			continue
		}

		totalSize += fInfo.Size
		splits = append(splits, splitInputFile(fInfo, maxSplitSize)...)
	}
	if len(files) > 0 {
		log.Debugf("Average split size: %s bytes", humanize.Bytes(uint64(totalSize)/uint64(len(splits))))
	}

	j.intermediateBins = uint(j.config.NumReduce)
	//
	//j.intermediateBins = uint(float64(totalSize/j.config.ReduceBinSize) * 1.25)
	//if j.intermediateBins == 0 {
	//	j.intermediateBins = 1
	//}

	return splits
}

// NewJob creates a new job from a Mapper and Reducer.
func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:                 mapper,
		Reduce:              reducer,
		config:              &config{},
		mapperCompletedChan: make(chan string),
	}
}
