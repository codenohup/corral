package corral

import (
	"github.com/spf13/viper"
)

func loadConfig() {
	viper.SetConfigName("corralrc")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.corral")

	setupDefaults()

	viper.ReadInConfig()

	viper.SetEnvPrefix("corral")
	viper.AutomaticEnv()
}

func setupDefaults() {
	defaultSettings := map[string]interface{}{
		"lambdaFunctionName": "corral_function",
		"lambdaMemory":       3000,
		"lambdaTimeout":      180,
		"lambdaManageRole":   true,
		"cleanup":            true,
		"verbose":            false,
		"splitSize":          100 * 1024 * 1024, // Default input split size is 100Mb
		"mapBinSize":         512 * 1024 * 1024, // Default map bin size is 512Mb
		"reduceBinSize":      512 * 1024 * 1024, // Default reduce bin size is 512Mb
		"maxConcurrency":     200,               // Maximum number of concurrent executors
		"workingLocation":    ".",
		"numReduce":          1,
	}
	for key, value := range defaultSettings {
		viper.SetDefault(key, value)
	}

	aliases := map[string]string{
		"verbose":          "v",
		"working_location": "o",
	}
	for key, alias := range aliases {
		viper.RegisterAlias(alias, key)
	}
}

const (
	shuffleOutType         = "line" // "line", Shuffle数据按行输出，key`\tab`val; "json": Shuffle数据按json格式输出
	defaultShuffleID       = 0
	shuffleFileMergeDegree = 10 // 每10个 Shuffle file合并，并分发到成R个文件

	// MapReduce，每个Map生成R个Shuffle文件
	shuffleEmitterType = "General"   // "SingleFIle", 一个Map输出一个Shuffle文件; "General", 一个Map输出R个Shuffle文件
	jobRunningType     = "MapReduce" // 作业类型是MapReduce，还是MapMergeReduce;  MapMergeReduce应该和 shuffleEmitterType=SingleFile一起使用

	// MapReduce, 每个Map生成一个Shuffle文件
	//shuffleEmitterType = "SingleFile" // "SingleFIle", 一个Map输出一个Shuffle文件; "General", 一个Map输出R个Shuffle文件
	//jobRunningType     = "MapReduce"  // 作业类型是Map-Reduce，还是Map-Merge-Reduce;  MapMergeReduce应该和 shuffleEmitterType=SingleFile一起使用

	// MapMergeReduce, 每个Map生成一个Shuffle文件
	//shuffleEmitterType = "SingleFile"     // "SingleFIle", 一个Map输出一个Shuffle文件; "General", 一个Map输出R个Shuffle文件
	//jobRunningType     = "MapMergeReduce" // 作业类型是Map-Reduce，还是Map-Merge-Reduce;  MapMergeReduce应该和 shuffleEmitterType=SingleFile一起使用
)
