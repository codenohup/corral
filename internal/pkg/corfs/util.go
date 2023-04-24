package corfs

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// 把数组切片为固定大小的组
func ArraySplitByFixedInterval[T any](laxiconid []T, subGroupLength int64) [][]T {
	max := int64(len(laxiconid))
	var segmens = make([][]T, 0)
	quantity := max / subGroupLength
	remainder := max % subGroupLength
	i := int64(0)
	for i = int64(0); i < quantity; i++ {
		segmens = append(segmens, laxiconid[i*subGroupLength:(i+1)*subGroupLength])
	}
	if quantity == 0 || remainder != 0 {
		segmens = append(segmens, laxiconid[i*subGroupLength:i*subGroupLength+remainder])
	}
	return segmens
}
