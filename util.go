package dxda

const (
	NumRetriesDefault   = 3
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Min ...
// https://mrekucci.blogspot.com/2015/07/dont-abuse-mathmax-mathmin.html
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
