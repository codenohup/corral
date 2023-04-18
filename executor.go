package corral

type executor interface {
	RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error
	RunReducer(job *Job, jobNumber int, binID uint) error
	RunMerge(job *Job, jobNumber int, taskID int, startFileID int, endFileID int) error
}

type localExecutor struct{}

func (localExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	_, err := job.runMapper(binID, inputSplits)
	return err
}

func (localExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	return job.runReducer(binID)
}

func (localExecutor) RunMerge(job *Job, jobNumber int, taskID int, startFileID int, endFileID int) error {
	return job.runMerger(taskID, startFileID, endFileID)
}
