package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bcongdon/corral"
	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TargetSize         = 100 * 1024 * 1024
	MaxNumOfFiles      = 100
	LocalOuputDir      = "/Users/chase/Downloads/MotivationExpr/"
	S3OutputDir        = "s3://huangxu/motivation_expr/"
	lambdaFunctionName = "huangxu_motivation_expr"
	MaxConcurrency     = 200

	JobRunningInLocal                 = false
	RunningTaskType        S3taskType = S3Read
	S3WriteParallelEnabled            = false
	S3ReadParallelEnabled             = false
)

var CloudFunctionRunningTimes []int64 = make([]int64, 0)
var lambdaExe *corral.LambdaExecutor
var OutputDir string
var Fs corfs.FileSystem

type S3taskType int

const (
	S3Write S3taskType = iota
	S3Read
	S3ConcurrencyRead
)

type S3task struct {
	TaskType S3taskType

	// S3Write task
	S3Write_outputDir string
	S3Write_fileNum   int

	// S3Read task
	S3ReadDir string
}

type S3taskResult struct {
	functionRunningTime int64
}

func Init() {
	// viper设置
	viper.SetConfigName("corralrc")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.corral")
	defaultSettings := map[string]interface{}{
		"lambdaMemory":     1500, // 3000
		"lambdaTimeout":    180,  // 180
		"lambdaManageRole": true,
	}
	for key, value := range defaultSettings {
		viper.SetDefault(key, value)
	}
	viper.ReadInConfig()
	viper.SetEnvPrefix("corral")
	viper.AutomaticEnv()

	// 部署函数
	if !JobRunningInLocal && !runningInLambda() {
		lambdaExe = corral.NewLambdaExecutor(lambdaFunctionName)
		start := time.Now()
		lambdaExe.Deploy()
		end := time.Now()
		fmt.Printf("Deploy function %v Time: %s\n", lambdaFunctionName, end.Sub(start))
	}

	if JobRunningInLocal {
		OutputDir = LocalOuputDir
		Fs = &corfs.LocalFileSystem{}
	} else {
		OutputDir = S3OutputDir
		Fs = &corfs.S3FileSystem{}
	}
	Fs.Init()

	if RunningTaskType == S3Write || RunningTaskType == S3Read {
		OutputDir = Fs.Join(OutputDir, "S3Write")
		Fs.MakeDir(OutputDir)
	}
}

func S3WriteTask_CreateFixedFile(OutputDir string, fileNum int) {
	eachFileSize := TargetSize / fileNum
	wg := sync.WaitGroup{}
	for i := 0; i < fileNum; i++ {
		if S3WriteParallelEnabled {
			wg.Add(1)
			go func(fileIdx int, size int) {
				defer wg.Done()
				outputPath := Fs.Join(OutputDir, fmt.Sprintf("FileNum=%v_FileSize=%v", fileNum, humanize.Bytes(uint64(eachFileSize))), fmt.Sprintf("file_%v.bin", fileIdx))
				outputPath = strings.Replace(outputPath, " ", "", -1)
				writer, err := Fs.OpenWriter(outputPath)
				if err != nil {
					fmt.Printf("Error occur when Fs.OpenWriter(), error msg: %v\n", err.Error())
				}
				buffer := make([]byte, 1024)
				for i := 0; i < size/1024; i++ {
					_, err := writer.Write(buffer)
					if err != nil {
						fmt.Printf("Error occur when writer.write(), error msg: %v\n", err.Error())
					}
				}
				writer.Close()
			}(i, eachFileSize)
		} else {
			outputPath := Fs.Join(OutputDir, fmt.Sprintf("FileNum=%v_FileSize=%v", fileNum, humanize.Bytes(uint64(eachFileSize))), fmt.Sprintf("file_%v.bin", i))
			outputPath = strings.Replace(outputPath, " ", "", -1)
			writer, err := Fs.OpenWriter(outputPath)
			if err != nil {
				fmt.Printf("Error occur when Fs.OpenWriter(), error msg: %v\n", err.Error())
			}
			buffer := make([]byte, 1024)
			for i := 0; i < eachFileSize/1024; i++ {
				_, err := writer.Write(buffer)
				if err != nil {
					fmt.Printf("Error occur when writer.write(), error msg: %v\n", err.Error())
				}
			}
			writer.Close()
		}
	}
	wg.Wait()
}

func S3ReadTask_ReadDir(dirPath string) {
	files, _ := Fs.ListFiles(Fs.Join(dirPath, "*.bin"))
	wg := sync.WaitGroup{}
	for _, file := range files {
		if S3ReadParallelEnabled {
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				buffer := make([]byte, 1024)
				reader, _ := Fs.OpenReader(path, 0)
				for {
					_, err := reader.Read(buffer)
					if err == io.EOF {
						break
					}
				}
			}(file.Name)
		} else {
			buffer := make([]byte, 1024)
			reader, _ := Fs.OpenReader(file.Name, 0)
			for {
				_, err := reader.Read(buffer)
				if err == io.EOF {
					break
				}
			}
		}
	}
	wg.Wait()
}

func LoadS3TaskResult(payload []byte) S3taskResult {
	// Unescape JSON string
	payloadStr, _ := strconv.Unquote(string(payload))

	var result S3taskResult
	err := json.Unmarshal([]byte(payloadStr), &result)
	if err != nil {
		log.Errorf("%s", err)
	}

	return result
}

func prepareS3TaskResult(task S3task, functionStartTime time.Time) string {
	result := S3taskResult{
		functionRunningTime: time.Now().Sub(functionStartTime).Milliseconds(),
	}

	payload, _ := json.Marshal(result)
	//fmt.Printf("Result payload: %v, resule obj: %v\n", payload, result)
	return string(payload)
}

func InvokeS3TaskFunction(s3task S3task) error {
	payload, err := json.Marshal(s3task)
	if err != nil {
		return err
	}

	resultPayload, err := lambdaExe.Invoke(lambdaExe.FunctionName, payload)
	taskResult := LoadS3TaskResult(resultPayload)
	//fmt.Printf("Received result payload: %v, taskResult obj: %v\n", resultPayload, taskResult)

	CloudFunctionRunningTimes = append(CloudFunctionRunningTimes, taskResult.functionRunningTime)
	fmt.Printf("Function running time: %v ms\n", taskResult.functionRunningTime)

	//atomic.AddInt64(&job.combineBytesRead, int64(taskResult.BytesRead))

	return err
}

func handleS3TaskRequest(ctx context.Context, task S3task) (string, error) {
	// Precaution to avoid running out of memory for reused Lambdas
	fmt.Printf("Receive task %v\n", task)
	functionStartTime := time.Now()
	debug.FreeOSMemory()

	if task.TaskType == S3Write {
		S3WriteTask_CreateFixedFile(task.S3Write_outputDir, task.S3Write_fileNum)
	} else {
		S3ReadTask_ReadDir(task.S3ReadDir)
	}
	return prepareS3TaskResult(task, functionStartTime), nil
}

func runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

// S3写出实验
// 同样是写1GB数据，输出到1个文件，和输出到100个文件，耗时对比
// 并行写出

// 运行方式 on lambda： go run Expr_S3ReadWrite.go --lambdaTimeout 180 --lambdaMemory 1500 --lambdaManageRole true
func main() {
	Init()

	if runningInLambda() {
		lambda.Start(handleS3TaskRequest)
		return
	}

	//var wg sync.WaitGroup
	//sem := semaphore.NewWeighted(int64(MaxConcurrency))
	if RunningTaskType == S3Write {
		var fileNums = []int{1, 10, 20, 50, 100, 200, 500, 1000}
		//var fileNums = []int{1, 10}
		for _, fileNum := range fileNums {
			jobLoopStartTime := time.Now()
			if JobRunningInLocal {
				S3WriteTask_CreateFixedFile(OutputDir, fileNum)
			} else {
				s3task := S3task{
					TaskType:          S3Write,
					S3Write_outputDir: OutputDir,
					S3Write_fileNum:   fileNum,
				}
				InvokeS3TaskFunction(s3task)
			}
			var lambdaRunningTime = 0
			if !JobRunningInLocal {
				lambdaRunningTime = int(CloudFunctionRunningTimes[0])
			}
			fmt.Printf("Job[TargetSize=%v][fileNum=%v] end to end time: %v , lambdaRunningTime: %v (ms)\n", humanize.Bytes(TargetSize), fileNum, time.Now().Sub(jobLoopStartTime), lambdaRunningTime)
		}
	} else if RunningTaskType == S3Read {
		if JobRunningInLocal {
			fileNames := []string{
				"FileNum=1_FileSize=10MB",
				"FileNum=10_FileSize=1.0MB",
				"FileNum=20_FileSize=524kB",
				"FileNum=50_FileSize=210kB",
				"FileNum=100_FileSize=105kB",
				"FileNum=200_FileSize=52kB",
			}
			for _, name := range fileNames {
				jobLoopStartTime := time.Now()
				S3ReadTask_ReadDir(Fs.Join(OutputDir, name))
				fmt.Printf("Job[%v] end to end time: %v \n", name, time.Now().Sub(jobLoopStartTime))
			}
		} else {
			fileNames := []string{
				"FileNum=1_FileSize=105MB/",
				"FileNum=10_FileSize=10MB/",
				"FileNum=20_FileSize=5.2MB/",
				"FileNum=50_FileSize=2.1MB/",
				"FileNum=100_FileSize=1.0MB/",
				"FileNum=200_FileSize=524kB/",
				"FileNum=500_FileSize=210kB/",
				"FileNum=1000_FileSize=105kB/",
			}
			for _, name := range fileNames {
				jobLoopStartTime := time.Now()
				s3task := S3task{
					TaskType:  S3Read,
					S3ReadDir: Fs.Join(OutputDir, name),
				}
				InvokeS3TaskFunction(s3task)
				fmt.Printf("Job[%v] end to end time: %v \n", name, time.Now().Sub(jobLoopStartTime))
			}
		}
	} else {
		fmt.Printf("Unsupport Task Type, exit\n")
	}
}
