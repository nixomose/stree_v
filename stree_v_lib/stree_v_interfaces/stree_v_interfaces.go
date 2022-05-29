// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package stree_v_lib

import "github.com/nixomose/nixomosegotools/tools"

type Stree_v_backing_store_interface interface {

	/* 12/26/2020 this is the interface that localstorage calls so these are the functions we
	 * have to lock on. See how I got the compiler to tell me that? */

	Init() tools.Ret

	Is_backing_store_uninitialized() (tools.Ret, bool)

	Startup(force bool) tools.Ret

	Shutdown() tools.Ret

	Load(block_num uint32) (tools.Ret, *[]byte)

	Load_limit(block_num uint32, len uint32) (tools.Ret, *[]byte)

	Store(block_num uint32, n *[]byte) tools.Ret

	Get_root_node() (tools.Ret, uint32)

	Set_root_node(block_num uint32) tools.Ret

	Get_free_position() (ret tools.Ret, resp uint32)

	Get_total_blocks() (tools.Ret, uint32)

	Allocate(amount uint32) (tools.Ret, []uint32)

	Deallocate() tools.Ret

	Wipe() tools.Ret // zero out the first block so as to make it inittable again

	Dispose() tools.Ret
}

type Stree_v_backing_store_internal_interface interface {
	Init() tools.Ret

	Startup() tools.Ret

	Shutdown() tools.Ret

	Load(block_num uint32) (tools.Ret, *[]byte)

	Load_limit(block_num uint32, len uint32) (tools.Ret, *[]byte)

	Store(block_num uint32, n *[]byte) tools.Ret

	Get_root_node() (tools.Ret, uint32)

	Set_root_node(block_num uint32) tools.Ret

	Get_free_position() (ret tools.Ret, resp uint32)

	Get_total_blocks() (tools.Ret, uint32)

	Allocate(amount uint32) (tools.Ret, []uint32)

	Deallocate() tools.Ret

	Dispose() tools.Ret

	Print(*tools.Nixomosetools_logger)

	Get_logger() *tools.Nixomosetools_logger

	Diag_dump()
}
