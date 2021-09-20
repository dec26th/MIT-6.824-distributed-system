package utils

func GetIntPtr(x int64) *int64 {
	return &x
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
