package utils

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}


func ContainsInt(slices []int, contain int) bool {
	for _, v := range slices {
		if v == contain {
			return true
		}
	}

	return false
}

func MapKeysToSlice(maps map[int][]string) []int {
	result := make([]int, 0, len(maps))
	for k := range maps {
		result = append(result, k)
	}

	return result
}