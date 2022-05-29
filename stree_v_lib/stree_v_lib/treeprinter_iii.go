// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package stree_v_lib

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/nixomose/nixomosegotools/tools"
)

// https://stackoverflow.com/questions/30767106/print-a-binary-search-tree-with-correct-formatting

type Node struct {
	si  *Stree_v
	pos uint32
}

func New_node(sin *Stree_v, posin uint32, data string) *Node {
	var node Node
	node.si = sin
	node.pos = posin
	return &node
}

func (this *Node) Get_key() string {
	var _, node = this.si.Node_load(this.pos)
	return node.Get_key()
}

func (this *Node) Get_data() []byte {
	var _, node = this.si.Node_load(this.pos)
	return node.Get_value()
}

func (this *Node) Get_left() *Node {
	var _, resp = this.si.Node_load(this.pos)
	var leftpos uint32 = resp.Get_left_child()
	if leftpos == 0 {
		return nil
	}
	var junk string
	var retn *Node = New_node(this.si, leftpos, junk)
	return retn
}

func (this *Node) Get_right() *Node {
	var _, resp = this.si.Node_load(this.pos)
	var rightpos uint32 = resp.Get_right_child()
	if rightpos == 0 {
		return nil
	}

	var junk string
	var retn *Node = New_node(this.si, rightpos, junk)
	return retn
}

/********************************************************************************************************/

type Treeprinter_iii struct {
}

func (tp *Treeprinter_iii) printint(i uint64) {
	fmt.Print(i)
}
func (tp *Treeprinter_iii) print(s string) {
	fmt.Print(s)
}
func (tp *Treeprinter_iii) println(s string) {
	fmt.Println(s)
}

func (tp *Treeprinter_iii) PrintNode(s *Stree_v, pos uint32) {
	// make a Node out of our stree_ii node and call the actual printer
	if s.Get_free_position() < 2 {
		tp.println("tree is empty")
		return
	}
	var junk string
	var n = New_node(s, pos, junk)
	tp.PrintNodeRoot(n)

	s.Print(s.Get_logger())

}

func (tp *Treeprinter_iii) PrintNodeRoot(root *Node) {
	var maxLevel int = tp.maxLevel(root)

	var l *list.List = list.New()
	l.PushBack(root)
	tp.PrintNodeInternal(l, 1, maxLevel)
}

func (tp *Treeprinter_iii) PrintNodeInternal(nodes *list.List, level int, maxLevel int) {
	if nodes.Len() == 0 || tp.isAllElementsNull(nodes) {
		return
	}

	// my int math causes tree to print funny for some reason.
	// var ifloor int = maxLevel - level
	// var iendgeLines int = tools.Powint(2, (tools.Maxint(floor-1, 0)))
	// var ifirstSpaces int = tools.Powint(2, floor) - 1
	// var ibetweenSpaces int = tools.Powint(2, (floor+1)) - 1

	var floor int = maxLevel - level
	var endgeLines int = int(math.Pow(2, (math.Max(float64(floor)-1, 0))))
	var firstSpaces int = int(math.Pow(2, float64(floor)) - 1)
	var betweenSpaces int = int(math.Pow(2, float64(floor+1)) - 1)

	tp.printWhitespaces(firstSpaces)

	var newnodes *list.List = list.New()
	var itemkey *list.Element
	for itemkey = nodes.Front(); itemkey != nil; itemkey = itemkey.Next() {
		//		var spaceoffset int = 0
		/* I can not explain this, itemvalue is not nil it is a pointer to a type
		   that is nil. in my test program I can check for nil but here it comes up
		   as not nil. so we do the extra check */

		var itemval = itemkey.Value
		var node, ok = itemval.(*Node)
		// in go you must check for nil before casting to the list entry's type for some reason or it will panic
		if ok && node != nil {

			// the key is a little endian hex stored value of the block_num now, convert to int before printing
			var data string = node.Get_key()
			var key_int = binary.LittleEndian.Uint64([]byte(data))
			tp.printint(key_int)

			// var slen int = len(data)

			// if slen > 1 {
			// 	spaceoffset = 2
			// } else {
			// 	spaceoffset = slen - 1
			// }
			// if spaceoffset < 0 {
			// 	spaceoffset = 0
			// }
			newnodes.PushBack(node.Get_left())
			newnodes.PushBack(node.Get_right())
		} else {
			newnodes.PushBack(nil)
			newnodes.PushBack(nil)
			tp.print(" ")
		}

		tp.printWhitespaces(betweenSpaces) //- spaceoffset)
	}
	tp.println("")

	for i := 1; i <= endgeLines; i++ {
		for j := 0; j < nodes.Len(); j++ {
			tp.printWhitespaces(firstSpaces - i)

			if tools.Getlistitematpos(nodes, j) == nil {
				tp.printWhitespaces(endgeLines + endgeLines + i + 1)
				continue
			}

			var itemkey = tools.Getlistitematpos(nodes, j)
			var itemval = itemkey.Value
			var itematpos, ok = itemval.(*Node)
			if !ok || itematpos == nil { // this is like above if the == nil check didn't work right.
				tp.printWhitespaces(endgeLines + endgeLines + i + 1)
				continue
			}

			if itematpos.Get_left() != nil {
				tp.print("/")
			} else {
				tp.printWhitespaces(1)
			}

			tp.printWhitespaces(i + i - 1)

			// itemkey = tools.Getlistitematpos(nodes, j)
			// itemval = itemkey.Value
			// itematpos, ok = itemval.(*Node)
			// if !ok || itematpos == nil {
			// 	// itematpos = tools.Getitematpos(nodes, j).Value.(*Node)
			// 	fmt.Print("this can't fail because it didn't fail above")
			// }
			if itematpos.Get_right() != nil {
				tp.print("\\")
			} else {
				tp.printWhitespaces(1)
			}

			tp.printWhitespaces(endgeLines + endgeLines - i)
		}

		tp.println("")
	}

	tp.PrintNodeInternal(newnodes, level+1, maxLevel)
}

func (tp *Treeprinter_iii) printWhitespaces(count int) {
	for i := 0; i < count; i++ {
		tp.print(" ")
	}
}

func (tp *Treeprinter_iii) maxLevel(n *Node) int {
	if n == nil {
		return 0
	}

	return tools.Maxint(tp.maxLevel(n.Get_left()), tp.maxLevel(n.Get_right())) + 1
}

func (tp *Treeprinter_iii) isAllElementsNull(l *list.List) bool {
	for e := l.Front(); e != nil; e = e.Next() {
		var val = e.Value
		if val != nil {
			return false
		}
	}
	return true
}
