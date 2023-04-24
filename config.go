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
		"lambdaMemory":       3000, // 3000
		"lambdaTimeout":      180,  // 180
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
	shuffleOutType                        = "line" // "line", Shuffle数据按行输出，key`\tab`val; "json": Shuffle数据按json格式输出
	defaultShuffleID                      = 0
	shuffleFileMergeDegree                = 10 // 每10个 Shuffle file合并，并分发到成R个文件
	lsmCombineDegree                      = 5  // 每10个Shuffle文件做Combine，分发到1个文件还是R个文件，取决于文件大小
	lsmCombineShuffleFileSizeSumThreshold = 1024 * 1024 * 1024
	clearLsmCombineIntermediateFiles      = true

	jobShuffleMode ShuffleMode = LSMCombine
)

type ShuffleMode int

const (
	General      ShuffleMode = iota // 每个Map生成R个Shuffle文件, Shuffle文件位置：Shuffle_origin/Shuffle_{ShuffleID}_map_{MapperID}_reduce_{ReducerID}.data
	WriteCombine                    // 每个Map生成1个Shuffle文件, Shuffle文件位置：Shuffle_origin/Shuffle_{ShuffleID}_Map_{MapperID}.data/index
	Merge                           /* 每个Map生成1个Shuffle文件，经过合并k个Shuffle文件，仍然生成1个Shuffle文件（包含R个分区的数据块）,
	原始Shuffle文件位置：Shuffle_origin/Shuffle_{ShuffleID}_Map_{MapperID}.data/index
	合并后Shuffle_merge/Merge/{StartMapperID}-{EndMapperID}.data/index,
	*/
	MergeAndDivide /* 每个Map生成1个Shuffle文件，经过合并k个Shuffle文件，生成R个Shuffle文件（每个文件只包含对应分区的数据块）
	原始Shuffle文件位置：Shuffle_origin/Shuffle_{ShuffleID}_Map_{MapperID}.data/index
	合并后Shuffle_merge/Reduce_{ReduceID}/{StartMapperID}-{EndMapperID}.data/index,
	*/

	LSMCombine /* = WriteCombine Shuffle + 与Map并行的动态Combine
	每个Map生成1个Shuffle文件，例如一共有1000个Mapper, 先是有100个Mapper同时完成，那么启动10个Combine任务，每个Combine读取10个Shuffle文件做Combine操作（Reduce操作），然后生成1个Shuffle文件；
	Shuffle文件的个数随着Combine的波数，分别为：1000、100、10、1， 中间根据Shuffle文件的大小，判断应该生成1个Shuffle文件

	原始Shuffle文件位置：Shuffle_origin/Shuffle_{ShuffleID}_Map_{MapperID}.data/index
	Combine 文件位置: Shuffle_combine/{uuid}.data/index
	*/
)
