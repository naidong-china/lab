package mr

func IndexOf(arr []string, target string) int {

	for i, s := range arr {
		if s == target {
			return i
		}
	}
	return -1
}
