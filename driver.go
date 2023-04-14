package corral

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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

const (
	shuffleOutType = "line" // "line", Shuffle数据按行输出，key`\tab`val; "json": Shuffle数据按json格式输出
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
	log.Debugf("Number of job input bins: %d", len(inputBins))
	bar := pb.New(len(inputBins)).Prefix("Map").Start()

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.Config.MaxConcurrency))
	for binID, bin := range inputBins {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			err := d.executor.RunMapper(job, jobNumber, bID, b)
			if err != nil {
				log.Errorf("Error when running mapper %d: %s", bID, err)
			}
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runReducePhase(job *Job, jobNumber int) {
	var wg sync.WaitGroup
	bar := pb.New(int(job.intermediateBins)).Prefix("Reduce").Start()
	for binID := uint(0); binID < job.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer bar.Increment()
			err := d.executor.RunReducer(job, jobNumber, bID)
			if err != nil {
				log.Errorf("Error when running reducer %d: %s", bID, err)
			}
		}(binID)
	}
	wg.Wait()
	bar.Finish()
}

// run starts the Driver
func (d *Driver) run() {
	if runningInLambda() {
		lambdaDriver = d
		lambda.Start(handleRequest)
	}
	if lBackend, ok := d.executor.(*lambdaExecutor); ok {
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

		mapStart := time.Now()
		d.runMapPhase(job, idx, inputs)
		mapEnd := time.Now()
		fmt.Printf("Job%d (%d/%d) Map State Execution Time: %s\n", idx, idx+1, len(d.jobs), mapEnd.Sub(mapStart))

		reduceStart := time.Now()
		d.runReducePhase(job, idx)
		reduceEnd := time.Now()
		fmt.Printf("Job%d (%d/%d) Reduce State Execution Time: %s\n", idx, idx+1, len(d.jobs), reduceEnd.Sub(reduceStart))

		// Set inputs of next job to be outputs of current job
		inputs = []string{job.fileSystem.Join(jobWorkingLoc, "output-*")}

		log.Infof("Job %d - Map Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.mapBytesRead)))
		log.Infof("Job %d - Map Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.mapBytesWritten)))
		log.Infof("Job %d - Reduce Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.reduceBytesRead)))
		log.Infof("Job %d - Reduce Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.reduceBytesWritten)))
	}
}

var lambdaFlag = flag.Bool("lambda", false, "Use lambda backend")
var outputDir = flag.StringP("out", "o", "", "Output `directory` (can be local or in S3)")
var memprofile = flag.String("memprofile", "", "Write memory profile to `file`")
var verbose = flag.BoolP("verbose", "v", false, "Output verbose logs")
var undeploy = flag.Bool("undeploy", false, "Undeploy the Lambda function and IAM permissions without running the driver")
var cleanup = flag.Bool("cleanup", true, "Whether delete shuffle files")

//var numReduce = flag.Int("numReduce", 1, "Number of reduce funtions")

// Main starts the Driver, running the submitted jobs.
func (d *Driver) Main() {
	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
	}

	if *undeploy {
		lambda := newLambdaExecutor(viper.GetString("lambdaFunctionName"))
		start := time.Now()
		lambda.Undeploy()
		end := time.Now()
		fmt.Printf("Undeply function %s time: %s\n", viper.GetString("lambdaFunctionName"), end.Sub(start))
		return
	}

	d.Config.Inputs = append(d.Config.Inputs, flag.Args()...)
	if *lambdaFlag {
		d.executor = newLambdaExecutor(viper.GetString("lambdaFunctionName"))
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
