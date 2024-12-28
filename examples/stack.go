package main

import (
	"errors"
	"fmt"

	"github.com/oarkflow/squealx/stack"
)

func main() {
	fn := stack.WrapFunc(add)
	fmt.Println(fn(5, 6))
}

func add(a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, errors.New("inputs must be non-negative")
	}
	return a + b, nil
}
