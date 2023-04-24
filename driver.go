package corral

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/spf13/viper"

	"golang.org/x/sync/semaphore"

	log "github.com/sirupsen/logrus"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bcongdon/corral/internal/pkg/corfs"
	flag "github.com/spf13/pflag"
)

// Driver controls the execution of a MapReduce Job
type Driver struct {
	jobs     []*Job
	Config   *config
	executor executor
}

// config configures a Driver's execution of jobs
type config struct {
	Inputs          []string
	SplitSize       int64
	MapBinSize      int64
	ReduceBinSize   int64
	MaxConcurrency  int
	WorkingLocation string
	Cleanup         bool
	NumReduce       int
	NumMap          int
}

func newConfig() *config {
	loadConfig() // Load viper config from settings file(s) and environment

	// Register command line flags
	flag.Parse()
	viper.BindPFlags(flag.CommandLine)

	return &config{
		Inputs:          []string{},
		SplitSize:       viper.GetInt64("splitSize"),
		MapBinSize:      viper.GetInt64("mapBinSize"),
		ReduceBinSize:   viper.GetInt64("reduceBinSize"),
		MaxConcurrency:  viper.GetInt("maxConcurrency"),
		WorkingLocation: viper.GetString("workingLocation"),
		Cleanup:         viper.GetBool("cleanup"),
		NumReduce:       viper.GetInt("numReduce"),
	}
}

// Option allows configuration of a Driver
type Option func(*config)

// NewDriver creates a new Driver with the provided job and optional configuration
func NewDriver(job *Job, options ...Option) *Driver {
	d := &Driver{
		jobs:     []*Job{job},
		executor: localExecutor{},
	}

	c := newConfig()
	for _, f := range options {
		f(c)
	}

	if c.SplitSize > c.MapBinSize {
		log.Warn("Configured Split Size is larger than Map Bin size")
		c.SplitSize = c.MapBinSize
	}

	d.Config = c
	log.Debugf("Loaded config: %#v", c)

	return d
}

// NewMultiStageDriver creates a new Driver with the provided jobs and optional configuration
func NewMultiStageDriver(jobs []*Job, options ...Option) *Driver {
	driver := NewDriver(nil, options...)
	driver.jobs = jobs
	return driver
}

// WithSplitSize sets the SplitSize of the Driver
func WithSplitSize(s int64) Option {
	return func(c *config) {
		c.SplitSize = s
	}
}

// WithMapBinSize sets the MapBinSize of the Driver
func WithMapBinSize(s int64) Option {
	return func(c *config) {
		c.MapBinSize = s
	}
}

// WithReduceBinSize sets the ReduceBinSize of the Driver
func WithReduceBinSize(s int64) Option {
	return func(c *config) {
		c.ReduceBinSize = s
	}
}

// WithWorkingLocation sets the location and filesystem backend of the Driver
func WithWorkingLocation(location string) Option {
	return func(c *config) {
		c.WorkingLocation = location
	}
}

// WithInputs specifies job inputs (i.e. input files/directories)
func WithInputs(inputs ...string) Option {
	return func(c *config) {
		c.Inputs = append(c.Inputs, inputs...)
	}
}

// Set lambda function name
func WithLambdaFuncName(funcName string) Option {
	return func(c *config) {
		viper.Set("lambdaFunctionName", funcName)
	}
}

// Set reduce funtion number
func WithNumReduce(num int) Option {
	return func(c *config) {
		viper.Set("numReduce", num)
		c.NumReduce = num
	}
}

func (d *Driver) runMapPhase(job *Job, jobNumber int, inputs []string) {
	inputSplits := job.inputSplits(inputs, d.Config.SplitSize)
	if len(inputSplits) == 0 {
		log.Warnf("No input splits")
		return
	}
	log.Debugf("Number of job input splits: %d", len(inputSplits))

	inputBins := packInputSplits(inputSplits, d.Config.MapBinSize)
	d.Config.NumMap = len(inputBins)
	log.Debugf("Number of job input bins: %d", len(inputBins))
	bar := pb.New(len(inputBins)).Prefix("Map").Start()

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.Config.MaxConcurrency))
	for binID, bin := range inputBins {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		job.cloudFuncNum++
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			err := d.executor.RunMapper(job, jobNumber, bID, b)
			if err != nil {
				log.Errorf("Error when running mapper %d: %s", bID, err)
			}
			if jobShuffleMode == LSMCombine {
				shuffleFilePath := job.fileSystem.Join(job.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_%d.data", defaultShuffleID, bID))
				job.mapperCompletedChan <- shuffleFilePath
			}
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()
	close(job.mapperCompletedChan)
}

func (d *Driver) runMergePhase(job *Job, jobNumber int) {
	numOfMergeTasks := (d.Config.NumMap + shuffleFileMergeDegree - 1) / shuffleFileMergeDegree
	log.Debugf("Number of job merge tasks: %d", numOfMergeTasks)
	bar := pb.New(numOfMergeTasks).Prefix("Merge").Start()

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.Config.MaxConcurrency))
	for taskId := 0; taskId < numOfMergeTasks; taskId++ {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(taskId int, startFileID int, endFileID int) { // 合并[start,end]范围里的Shuffle文件
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			job.cloudFuncNum++
			err := d.executor.RunMerge(job, jobNumber, taskId, startFileID, endFileID)
			if err != nil {
				log.Errorf("Error when running merge task, need to merged file range: [%d, %d], Error: %s", startFileID, endFileID, err.Error())
			}
		}(taskId, taskId*shuffleFileMergeDegree, int(min(int64((taskId+1)*shuffleFileMergeDegree-1), int64(d.Config.NumMap-1))))
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runReducePhase(job *Job, jobNumber int) {
	var wg sync.WaitGroup
	bar := pb.New(int(job.intermediateBins)).Prefix("Reduce").Start()
	sem := semaphore.NewWeighted(int64(d.Config.MaxConcurrency))
	for binID := uint(0); binID < job.intermediateBins; binID++ {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			job.cloudFuncNum++
			err := d.executor.RunReducer(job, jobNumber, bID)
			if err != nil {
				log.Errorf("Error when running reducer %d: %s", bID, err)
			}
		}(binID)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runCombineTask(job *Job, jobNumber int, outputPath string, paths []string, mustExecute bool) (bool, error) {
	if !mustExecute {
		var files []corfs.FileInfo
		for _, path := range paths {
			stat, _ := job.fileSystem.Stat(path)
			files = append(files, stat)
		}

		var accumulateSize int64 = 0
		for _, f := range files {
			accumulateSize += f.Size
		}
		// 如果文件累计大小达到了指定阈值，退出Combine过程，后面接着执行Reduce过程
		if accumulateSize >= lsmCombineShuffleFileSizeSumThreshold {
			return false, nil
		}
	}

	job.cloudFuncNum++
	err := d.executor.RunCombiner(job, jobNumber, outputPath, paths)
	if err != nil {
		log.Errorf("Error when running Combiner %v, files: %v, Error msg: %v\n", outputPath, paths, err)
	}

	return true, err
}

func (d *Driver) runCombinePhase(job *Job, jobNumber int) {
	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.Config.MaxConcurrency))

	// 计算可能有多少个MergeTask（不准，没关系）
	numOfCombineStages := 0
	numOfCombineTasks := 0
	for t := d.Config.NumMap; t > 1; t = t / lsmCombineDegree {
		numOfCombineStages++
		numOfCombineTasks += (t + lsmCombineDegree - 1) / lsmCombineDegree
	}
	bar := pb.New(numOfCombineTasks).Prefix("Combine").Start()

	// 不断读取Mapper/Combiner完成的输出文件，如果文件大小超出阈值，则不进行Combine
	mapperChannelExigFlag := false
	combineOutputChannel := make(chan string, corfs.MaxInt(d.Config.NumMap, 500))

	var needToCombinedFiles []string
	for {
		select {
		case path := <-combineOutputChannel:
			needToCombinedFiles = append(needToCombinedFiles, path)
		case path, ok := <-job.mapperCompletedChan:
			if !ok {
				mapperChannelExigFlag = true
			} else {
				needToCombinedFiles = append(needToCombinedFiles, path)
			}
		}

		if !mapperChannelExigFlag && len(needToCombinedFiles) >= lsmCombineDegree {
			//fmt.Printf("Combine %v in once \n", len(needToCombinedFiles))
			sem.Acquire(context.Background(), 1)
			files := make([]string, len(needToCombinedFiles))
			copy(files, needToCombinedFiles)
			wg.Add(1)
			go func(files []string) {
				defer wg.Done()
				defer sem.Release(1)
				defer bar.Increment()
				outputPath := job.fileSystem.Join(job.outputPath, "Shuffle_origin", fmt.Sprintf("combine_%s.data", uuid.NewString()))
				success, _ := d.runCombineTask(job, jobNumber, outputPath, files, false)
				if success {
					combineOutputChannel <- outputPath
				}
			}(files)

			needToCombinedFiles = []string{}
		}

		if mapperChannelExigFlag {
			break
		}
	}

	//close(combineOutputChannel)
	wg.Wait()
	bar.Finish()

	//for combineStageID := 1; combineStageID <= numOfCombineStages; combineStageID++ {
	//	// 读取上一轮的Shuffle文件，判断是否应该进行下一波Combine
	//	// 如果不就行Combine，则退出，
	//	// 后面接着做Reduce
	//	if !nextLoopShouldBeContinue {
	//		return
	//	}
	//
	//	mergeTaskIdInLoop := 0
	//
	//	var path string
	//	if combineStageID == 1 {
	//		//path = job.fileSystem.Join(job.outputPath, "Shuffle_origin", fmt.Sprintf("Shuffle_%d_Map_*.data", defaultShuffleID))
	//		channelExigFlag := false
	//		var needToCombinedFiles []string
	//		for {
	//			select {
	//			case path, ok := <-job.mapperCompletedChan:
	//				if !ok {
	//					channelExigFlag = true
	//				} else {
	//					needToCombinedFiles = append(needToCombinedFiles, path)
	//				}
	//				if (len(needToCombinedFiles) >= lsmCombineDegree) || (len(needToCombinedFiles) > 1 && channelExigFlag) {
	//					sem.Acquire(context.Background(), 1)
	//					wg.Add(1)
	//					go func() {
	//						defer wg.Done()
	//						defer sem.Release(1)
	//						defer bar.Increment()
	//						nextLoopFlag, _ := d.runCombineTask(job, jobNumber, combineStageID, mergeTaskIdInLoop, needToCombinedFiles)
	//						if !nextLoopFlag {
	//							nextLoopShouldBeContinue = true
	//						}
	//					}()
	//				}
	//			default:
	//				time.Sleep(10 * time.Millisecond)
	//			}
	//
	//			if channelExigFlag {
	//				break
	//			}
	//		}
	//	} else {
	//		path = job.fileSystem.Join(job.outputPath, "Shuffle_combine", "combine_*.data")
	//	}
	//files, _ := job.fileSystem.ListFiles(path)
	//var accumulateSize int64 = 0
	//for _, f := range files {
	//	accumulateSize += f.Size
	//}
	//avgSizePerShuffleFile := float64(accumulateSize) / float64(len(files)) // 如果文件平均大小达到了指定阈值，退出Combine过程，后面接着执行Reduce过程
	//if avgSizePerShuffleFile >= lsmCombineShuffleFileSizeThreshold {
	//	return
	//}
	//
	//fileSegments := corfs.ArraySplitByFixedInterval(files, lsmCombineDegree)
	//
	//for mergeTaskIdInLoop, segment := range fileSegments {
	//	mergeTaskID := fmt.Sprintf("%v_%v", combineStageID, mergeTaskIdInLoop)
	//	sem.Acquire(context.Background(), 1)
	//	wg.Add(1)
	//	go func(mergeTaskID string, files []corfs.FileInfo) {
	//		defer wg.Done()
	//		defer sem.Release(1)
	//		defer bar.Increment()
	//		err := d.executor.RunCombiner(job, jobNumber, mergeTaskID, files)
	//		if err != nil {
	//			log.Errorf("Error when running Combiner %v, files: %v, Error msg: %v\n", mergeTaskID, files, err)
	//		}
	//	}(mergeTaskID, segment)
	//
	//	//d.executor.RunCombiner(job, jobNumber, mergeTaskID, segment)
	//}
	//wg.Wait()
	//}

}

// run starts the Driver
func (d *Driver) run() {
	if runningInLambda() {
		lambdaDriver = d
		lambda.Start(handleRequest)
	}
	if lBackend, ok := d.executor.(*LambdaExecutor); ok {
		start := time.Now()
		lBackend.Deploy()
		end := time.Now()
		fmt.Printf("Deply function Time: %s\n", viper.GetString("lambdaFunctionName"), end.Sub(start))
	}

	if len(d.Config.Inputs) == 0 {
		log.Error("No inputs!")
		return
	}

	inputs := d.Config.Inputs
	for idx, job := range d.jobs {
		// Initialize job filesystem
		job.fileSystem = corfs.InferFilesystem(inputs[0])

		jobWorkingLoc := d.Config.WorkingLocation
		log.Infof("Starting job%d (%d/%d)", idx, idx+1, len(d.jobs))

		if len(d.jobs) > 1 {
			jobWorkingLoc = job.fileSystem.Join(jobWorkingLoc, fmt.Sprintf("job%d", idx))
		}
		job.outputPath = jobWorkingLoc

		*job.config = *d.Config

		job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Shuffle_origin"))
		if jobShuffleMode == Merge || jobShuffleMode == MergeAndDivide {
			job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Shuffle_merge"))
			job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Shuffle_merge", "Merge"))
		}
		if jobShuffleMode == LSMCombine {
			job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Shuffle_combine"))
		}
		job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Output"))

		for partitionID := 0; partitionID < d.Config.NumReduce; partitionID++ {
			job.fileSystem.MakeDir(job.fileSystem.Join(jobWorkingLoc, "Shuffle_merge", "Reduce_"+strconv.Itoa(partitionID)))
		}

		combinePhaseExitFlag := false
		if jobShuffleMode == LSMCombine {
			// 如果是LSMShuffle，则启动Combine阶段
			//combineOutputChannel := make(chan string)
			go func(job *Job, idx int) {
				combineStart := time.Now()
				d.runCombinePhase(job, idx)
				combineEnd := time.Now()
				fmt.Printf("Job%d (%d/%d) Combine phase Execution Time: %s\n", idx, idx+1, len(d.jobs), combineEnd.Sub(combineStart))
				combinePhaseExitFlag = true
			}(job, idx)
		}

		mapStart := time.Now()
		d.runMapPhase(job, idx, inputs)
		mapEnd := time.Now()
		fmt.Printf("Job%d (%d/%d) Map phase Execution Time: %s\n", idx, idx+1, len(d.jobs), mapEnd.Sub(mapStart))

		if jobShuffleMode == Merge || jobShuffleMode == MergeAndDivide {
			mergeStart := time.Now()
			d.runMergePhase(job, idx)
			mergeEnd := time.Now()
			fmt.Printf("Job%d (%d/%d) Merge phase Execution Time: %s\n", idx, idx+1, len(d.jobs), mergeEnd.Sub(mergeStart))
		}

		// 如果Combine阶段还没结束，需要等待
		for jobShuffleMode == LSMCombine && !combinePhaseExitFlag {
			time.Sleep(5 * time.Millisecond)
		}

		// 启动Reduce 阶段
		reduceStart := time.Now()
		d.runReducePhase(job, idx)
		reduceEnd := time.Now()
		fmt.Printf("Job%d (%d/%d) Reduce phase Execution Time: %s\n", idx, idx+1, len(d.jobs), reduceEnd.Sub(reduceStart))

		// Set inputs of next job to be outputs of current job
		inputs = []string{job.fileSystem.Join(jobWorkingLoc, "output-*")}

		log.Infof("Job %d - Map Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.mapBytesRead)))
		log.Infof("Job %d - Map Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.mapBytesWritten)))
		if jobShuffleMode == Merge || jobShuffleMode == MergeAndDivide {
			log.Infof("Job %d - Merge Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.mergeBytesRead)))
			log.Infof("Job %d - Merge Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.mergeBytesWritten)))
		}
		if jobShuffleMode == LSMCombine {
			log.Infof("Job %d - Combine Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.combineBytesRead)))
			log.Infof("Job %d - Combine Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.combineBytesWritten)))
		}
		log.Infof("Job %d - Reduce Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.reduceBytesRead)))
		log.Infof("Job %d - Reduce Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.reduceBytesWritten)))

		duration := time.Duration(job.cloudFuncRunningTime) * time.Millisecond
		log.Infof("Job %d - Cloud Functions Running Time:\t%v", idx, duration)
		log.Infof("Job %d - Cloud/Local Functions Number:\t%v", idx, job.cloudFuncNum)
	}
}

var lambdaFlag = flag.Bool("lambda", false, "Use lambda backend")
var outputDir = flag.StringP("out", "o", "", "Output `directory` (can be local or in S3)")
var memprofile = flag.String("memprofile", "", "Write memory profile to `file`")
var verbose = flag.BoolP("verbose", "v", false, "Output verbose logs")
var undeploy = flag.Bool("undeploy", false, "Undeploy the Lambda function and IAM permissions without running the driver")
var cleanup = flag.Bool("cleanup", true, "Whether delete shuffle files")
var localOutputDir = flag.String("localout", "lo", "Shuffle write dir in local") // 只有在running on lambda，WriteCombine Shuffle时才有用

//var numReduce = flag.Int("numReduce", 1, "Number of reduce funtions")

// Main starts the Driver, running the submitted jobs.
func (d *Driver) Main() {
	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
	}

	if *undeploy {
		lambda := NewLambdaExecutor(viper.GetString("lambdaFunctionName"))
		start := time.Now()
		lambda.Undeploy()
		end := time.Now()
		fmt.Printf("Undeply function %s time: %v\n", viper.GetString("lambdaFunctionName"), end.Sub(start))
		return
	}

	d.Config.Inputs = append(d.Config.Inputs, flag.Args()...)
	if *lambdaFlag {
		d.executor = NewLambdaExecutor(viper.GetString("lambdaFunctionName"))
	}

	if *outputDir != "" {
		d.Config.WorkingLocation = *outputDir
	}

	start := time.Now()
	d.run()
	end := time.Now()
	fmt.Printf("Job Execution Time: %s\n", end.Sub(start))

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
}
