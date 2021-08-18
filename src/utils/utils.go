package utils

func GetIntPtr(x int) *int {
	return &x
}


func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}