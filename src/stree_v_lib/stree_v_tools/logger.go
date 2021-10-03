package stree_v_tools

import "fmt"

type Stree_logger struct {
	level int
}

func (l Stree_logger) debug(s string) {
	fmt.Println(s)
}
