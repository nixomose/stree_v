#!/bin/bash


#time go build github.com/nixomose/stree_v/stree_v_lib/debugtools github.com/nixomose/stree_v/stree_v_lib/stree_v_interfaces github.com/nixomose/stree_v/stree_v_lib/stree_v_lib github.com/nixomose/stree_v/stree_v_lib/stree_v_node github.com/nixomose/stree_v/test

#time go build ./stree_v_lib/debugtools ./stree_v_lib/stree_v_interfaces ./stree_v_lib/stree_v_lib ./stree_v_lib/stree_v_node ./test/driver.go

time go build test/driver.go test/stree_v_test_lib.go

