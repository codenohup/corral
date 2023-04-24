package corral

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/bcongdon/corral/internal/pkg/coriam"
	"github.com/bcongdon/corral/internal/pkg/corlambda"
)

var (
	lambdaDriver *Driver
)

// corralRoleName is the name to use when deploying an IAM role
const corralRoleName = "CorralExecutionRole"

// runningInLambda infers if the program is running in AWS lambda via inspection of the environment
func runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

func prepareResult(job *Job, task task, functionStartTime time.Time, lengths []int) string {
	var bytesRead int
	var bytesWritten int
	if task.Phase == MapPhase {
		bytesRead = int(job.mapBytesRead)
		bytesWritten = int(job.mapBytesWritten)
	} else if task.Phase == ReducePhase {
		bytesRead = int(job.reduceBytesRead)
		bytesWritten = int(job.reduceBytesWritten)
	} else if task.Phase == MergePhase {
		bytesRead = int(job.mergeBytesRead)
		bytesWritten = int(job.mergeBytesWritten)
	} else if task.Phase == CombinePhase {
		bytesRead = int(job.combineBytesRead)
		bytesWritten = int(job.combineBytesWritten)
	}

	result := taskResult{
		BytesRead:    bytesRead,
		BytesWritten: bytesWritten,
		RunningTime:  time.Now().Sub(functionStartTime).Milliseconds(),
		Lengths:      nil, // lengths
	}

	payload, _ := json.Marshal(result)
	return string(payload)
}

func handleRequest(ctx context.Context, task task) (string, error) {
	// Precaution to avoid running out of memory for reused Lambdas
	functionStartTime := time.Now()

	fmt.Printf("[handleRequest] receive task %v, binID: %v, taskID: %v\n", task.Phase, task.BinID, task.TaskID)

	debug.FreeOSMemory()

	// Setup current job
	fs := corfs.InitFilesystem(task.FileSystemType)
	currentJob := lambdaDriver.jobs[task.JobNumber]
	currentJob.fileSystem = fs
	currentJob.intermediateBins = task.IntermediateBins
	currentJob.outputPath = task.WorkingLocation
	currentJob.config.Cleanup = task.Cleanup

	// Need to reset job counters in case this is a reused lambda
	currentJob.mapBytesRead = 0
	currentJob.mapBytesWritten = 0
	currentJob.reduceBytesRead = 0
	currentJob.reduceBytesWritten = 0
	currentJob.mergeBytesRead = 0
	currentJob.mergeBytesWritten = 0
	currentJob.combineBytesRead = 0
	currentJob.combineBytesWritten = 0

	currentJob.config.NumReduce = task.NumReduce

	if task.Phase == MapPhase {
		lengths, err := currentJob.runMapper(task.BinID, task.Splits)
		return prepareResult(currentJob, task, functionStartTime, lengths), err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(task.BinID)
		return prepareResult(currentJob, task, functionStartTime, nil), err
	} else if task.Phase == MergePhase {
		err := currentJob.runMerger(task.TaskID, task.StartFileID, task.EndFileID)
		return prepareResult(currentJob, task, functionStartTime, nil), err
	} else if task.Phase == CombinePhase {
		err := currentJob.runCombiner(task.CombineOutputFilePath, task.NeedToCombinedFiles)
		return prepareResult(currentJob, task, functionStartTime, nil), err
	}
	return "", fmt.Errorf("Unknown phase: %d", task.Phase)
}

type LambdaExecutor struct {
	*corlambda.LambdaClient
	*coriam.IAMClient
	FunctionName string
}

func (l *LambdaExecutor) RunMerge(job *Job, jobNumber int, taskID int, startFileID int, endFileID int) error {
	mergeTask := task{
		JobNumber:       jobNumber,
		Phase:           MergePhase,
		FileSystemType:  corfs.S3,
		WorkingLocation: job.outputPath,
		TaskID:          taskID,
		StartFileID:     startFileID,
		EndFileID:       endFileID,
		NumReduce:       job.config.NumReduce,
	}
	payload, err := json.Marshal(mergeTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.FunctionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.mergeBytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.mergeBytesWritten, int64(taskResult.BytesWritten))
	if taskResult.RunningTime > 0 {
		atomic.AddInt64(&job.cloudFuncRunningTime, taskResult.RunningTime)
	}

	return err
}

func NewLambdaExecutor(functionName string) *LambdaExecutor {
	return &LambdaExecutor{
		LambdaClient: corlambda.NewLambdaClient(),
		IAMClient:    coriam.NewIAMClient(),
		FunctionName: functionName,
	}
}

func loadTaskResult(payload []byte) taskResult {
	// Unescape JSON string
	payloadStr, _ := strconv.Unquote(string(payload))

	var result taskResult
	err := json.Unmarshal([]byte(payloadStr), &result)
	if err != nil {
		log.Errorf("%s", err)
	}
	return result
}

func (l *LambdaExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.S3,
		WorkingLocation:  job.outputPath,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.FunctionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.mapBytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.mapBytesWritten, int64(taskResult.BytesWritten))
	atomic.AddInt64(&job.cloudFuncRunningTime, taskResult.RunningTime)

	//if shuffleEmitterType == "SingleFile" {
	//	if taskResult.Lengths == nil || len(taskResult.Lengths) != job.config.NumReduce {
	//		return errors.New("Error occur in write lengths to index file to local")
	//	}
	//	emitter := newSingleFileMapperEmitter(job.intermediateBins, binID, viper.GetString("localout"), &corfs.LocalFileSystem{})
	//	err := emitter.writeToIndexFile(taskResult.Lengths)
	//	if err != nil {
	//		return err
	//	}
	//}

	return err
}

func (l *LambdaExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	reduceTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.S3,
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
		NumReduce:       job.config.NumReduce,
	}
	payload, err := json.Marshal(reduceTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.FunctionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.reduceBytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.reduceBytesWritten, int64(taskResult.BytesWritten))
	atomic.AddInt64(&job.cloudFuncRunningTime, taskResult.RunningTime)

	return err
}

func (l *LambdaExecutor) RunCombiner(job *Job, jobNumber int, outputPath string, files []string) error {
	reduceTask := task{
		JobNumber:             jobNumber,
		Phase:                 CombinePhase,
		FileSystemType:        corfs.S3,
		WorkingLocation:       job.outputPath,
		Cleanup:               job.config.Cleanup,
		NumReduce:             job.config.NumReduce,
		CombineOutputFilePath: outputPath,
		NeedToCombinedFiles:   files,
	}
	payload, err := json.Marshal(reduceTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.FunctionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.combineBytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.combineBytesWritten, int64(taskResult.BytesWritten))
	atomic.AddInt64(&job.cloudFuncRunningTime, taskResult.RunningTime)

	return err
}

func (l *LambdaExecutor) Deploy() {
	var roleARN string
	var err error
	if viper.GetBool("lambdaManageRole") {
		roleARN, err = l.DeployPermissions(corralRoleName)
		if err != nil {
			panic(err)
		}
	} else {
		roleARN = viper.GetString("lambdaRoleARN")
	}

	config := &corlambda.FunctionConfig{
		Name:       l.FunctionName,
		RoleARN:    roleARN,
		Timeout:    viper.GetInt64("lambdaTimeout"),
		MemorySize: viper.GetInt64("lambdaMemory"),
	}
	err = l.DeployFunction(config)
	if err != nil {
		panic(err)
	}
}

func (l *LambdaExecutor) Undeploy() {
	log.Info("Undeploying function")
	err := l.LambdaClient.DeleteFunction(l.FunctionName)
	if err != nil {
		log.Errorf("Error when undeploying function: %s", err)
	}

	log.Info("Undeploying IAM Permissions")
	err = l.IAMClient.DeletePermissions(corralRoleName)
	if err != nil {
		log.Errorf("Error when undeploying IAM permissions: %s", err)
	}
}
