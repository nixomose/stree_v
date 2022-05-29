/* this is the go library port of stree_iv.
   actually we should probablhy call it stree_v because it will implement the transaction log somehow. */

// package name must match directory name
package stree_v_lib

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"syscall"

	"github.com/nixomose/nixomosegotools/tools"
	stree_v_interfaces "github.com/nixomose/stree_v/stree_v_lib/stree_v_interfaces"
	"github.com/nixomose/stree_v/stree_v_lib/stree_v_node"
)

/* 10/28/2020 so tom just gave me a great idea.
 * if you recall:
 * stree 1 was a binary heap that was searchable so it was actually a binary tree
 * but it turned out that making that tree search-correct after an insert became horribly
 * complicated because all the bubbling up and down moved things that were in the right place
 * into the wrong place and I think there's only one layout of tree that any given combintion
 * of tree can be and still be compact. So that's out.
 *
 * stree 2 was include the pointers to parent and children in the node and just shuffle them
 * about and not require the left node = n*2 and right node =n*2+1.
 * But that left me with the problem of storing metadata nice and compactly but still not
 * knowing where the actual data was, and then the data section of the disk
 * ended up requiring a freelist, or some other allocation scheme.
 *
 * stree 3 implemented here solves that problem by putting the data in the node.
 * This solves the problem of having a fragmented pile of data because the data is now in the
 * node that is compact, it is implemented and it works and the world is good.
 * the downside is that deletes are slow, and all node updates, for which there are many
 * for delete, involve reading the entire data block in as well (although I guess we can optmizie
 * by only reading and writing headers when we just need to update metadata.)
 *
 * But tom's idea is better.
 * Two sections on disk, just like stree 2, the first will be just the stree metata like stree 3
 * and the second section of the disk will be just the raw data. The positions of the data will
 * be known because it will match the position of the same record in the metadata.
 * So if you move metadata tree pos 6 to pos 4, you also move actual data block 6 to block 4,
 * it's two mirrored blocks.
 * This way you can do searches much more quickly because there is way less data to read.
 * although I think we can get almost the same effiency out of stree 3 by only reading and writing
 * metadata when we only need to read and update metadata.
 */

/* recovery. So if we are unable to shutdown cleanly, how can we recover.
 * 1) if the tree is okay, we can calculate the root node by picking the first element and following its
 * parent until it comes up with zero. the only time that wouldn't work is if the tree was empty and element 1
 * was just abandoned after delete but still has valid data.
 *
 * 2) if the tree is okay, we can calculate free position by traversing the whole tree, and just keeping
 * the highest numbered element.
 *
 * 3) if the tree is not okay, have to think of ways we can recover.
 * It might be more robust to just write a transaction log and make sure all the log actions are
 * idempotent (write this thing there, not perform this relative operation) and then recover by
 * replaying the unplayed log, and if something gets replayed twice, no big deal. */

/* unintended side effect: you can't have a block size smaller than the header which at the moment
 * v1 is 32 bytes. so your key + value + 20 has to be 32 or more */

//10/29/2020
//implement v4 and block devices after you get zen storage working

/* 11/2/2020 so we're going to skip on the store-the-data-in-a-parallel-tree-at-the-end-of-the-disk
 * idea, and for stree_iv we're going to implement the offspring array.
 * then stree_v will be the front and back trees.
 */

/* 12/26/2020 there should be an interface to stree, but at the moment there isn't
 * so stree is called directly, and so be it.
 * The problem is it is not threadsafe, and now that we have sponge in a separate
 * thread, it's pRetty much guaranteed that a sponge will immediately start screwing
 * up the stree when it runs, so we will put a lock around every entry point.
 * might as well make an interface while we're here. */

/* 10/10/2021 so it turns out stree_v will be the port to golang and include the transaction log
   to make sure the tree can never get messed up. I've been reading a bit about atomic disk writes and it turns
   out you can't really rely on atomic disk writes, like any of them. The smallest unit of write should be a
   sector which is either 512 or 4096 depending on the disk. they say you'll either get the old data, the new
   data or an error or something else. so really nobody guarantees anything. in fact they even say the first
   part of your sector can be the new data, and the last part of your sector can be the old data.
   And I think there's nothing saying you can't have the beginning and end be new and the middle be old.
   so. Best we can do is write markers at the front and back of each sector of transaction log
   and if our marker is not there for every piece of the transction (including the end of transaction marker)
   then the entire transaction does not run. if everything is there, then you can run the whole transaction
   and get your tree from one good state to another good state. */

// type Stree_v_interface interface {

// 	/* 12/26/2020 this is the interface that localstorage calls so these are the functions we
// 	 * have to lock on. See how I got the compiler to tell me that? */

// 	Get_used_blocks() (Ret, uint32)

// 	Get_total_blocks() (Ret, uint32)

// 	Get_block_size_in_bytes() uint32

// 	Update_or_insert(key string, new_value string) Ret

// 	Fetch(key string) (RetRet Ret, found bool, data string)

// 	Delete(key string, not_found_is_error bool) Ret
// }

// type Stree_v_internal_interface interface {
// 	Get_used_blocks() (Ret, uint32)

// 	Get_total_blocks() (Ret, uint32)

// 	Get_block_size_in_bytes() uint32

// 	Update_or_insert(key string, new_value string) Ret

// 	Fetch(key string) (RetRet Ret, found bool, data string)

// 	Delete(key string, not_found_is_error bool) Ret

// 	Node_load(lp uint32) (Ret, *Stree_node)

// 	Get_free_position() uint32

// 	Print()

// 	Get_logger() *tools.Stree_logger
// }

// verify that stree implements the interface
// var _ stree_v_lib.Stree_v_backing_store_interface = &Stree_v{}
// var _ stree_v_lib.Stree_v_backing_store_interface = (*Stree_v)(nil)

// // verify that stree implements the internal interface
// var _ stree_v_lib.Stree_v_backing_store_internal_interface = &Stree_v{}
// var _ stree_v_lib.Stree_v_backing_store_internal_interface = (*Stree_v)(nil)

type Stree_v struct {

	/* This is the number of pieces you want to break up a block into. So if you have 128k blocks, and
	 * you think compression will be of benefit, then make this 8, so you will be able to store your
	 * 128k block of data in 16k (128/8) if it compresses enough.
	 * this should be a power of 2 */
	/* 11/2/2020 okay, we're making this a bit more flexible and from my notes, we're calling them
	 * offspring.
	 * So a parent node has a left and a right child node just like stree_iii.
	 * any given node can either be a mother or an offspring node.
	 * the mother nodes are basically the first in the chain and are directly
	 * referred to in the tree (by parents and left and right child), and the offspring are
	 * only referred to by the mother node, the offspring nodes have parent references to their
	 * mother node. All nodes are the same size, so we waste a bit of metadata space on the offspring
	 * nodes, but we can then delete or shuffle any individual offspring or mother node.
	 */

	storage            stree_v_interfaces.Stree_v_backing_store_interface // the backing store mechanism for writing stree_v data
	m_max_key_length   uint32                                             // this is the maximum size of the key defined by the instantiator
	m_max_value_length uint32                                             // this is the maximum size of the value
	m_default_key      string                                             // this is what we make clones from, does not have to be padded to max size
	m_default_value    []byte

	/* How many elements in the offspring array for each node */
	m_offspring_per_node       uint32
	m_verify_client_block_size uint32 // this is only used to make sure the client knows what they're doing.

	/* 12/26/2020 only one of anything in the interface can happen at once, so here's the lock for it. */
	interface_lock sync.Mutex
	log            *tools.Nixomosetools_logger

	cached_metadata_size uint32 // calc once and cache the length of the stree node header
	debugprint           bool
}

func New_Stree_v(l *tools.Nixomosetools_logger, b stree_v_interfaces.Stree_v_backing_store_interface, max_key_length uint32,
	max_value_length uint32, additional_offspring_nodes uint32, verify_client_block_size uint32, default_key string,
	default_value []byte) *Stree_v {
	/* caller doesn't know what offspring nodes are and how we can store as much data as node_size * offspring_nodes + 1
	 * so they just pass us the number of nodes total they want to store, and we subtract accordingly. */
	var s Stree_v
	s.log = l
	s.storage = b
	s.m_max_key_length = max_key_length
	s.m_max_value_length = max_value_length // the amount of data that will fit in one node, not one block
	s.m_default_key = default_key
	s.m_default_value = default_value
	s.m_offspring_per_node = additional_offspring_nodes
	/* (offspring_per_node + 1) * value_length is the max data size we can store per write insert request.
	 * This is what the client will see as the "block size" which is the max data we can store in a block. */
	s.m_verify_client_block_size = verify_client_block_size // we only care about this to verify what the client sent us is sane
	// s.interface_lock   doesn't need to be initted
	return &s
}

func (this *Stree_v) Get_logger() *tools.Nixomosetools_logger {
	return this.log
}

func (this *Stree_v) Is_initialized() (tools.Ret, bool) {
	/* check the first 4k for zeroes. */

	var ret, uninitted = this.storage.Is_backing_store_uninitialized()
	if ret != nil {
		return ret, false
	}

	if uninitted {
		return nil, false
	}
	return nil, true
}

func (this *Stree_v) Init() tools.Ret {
	/* init the backing store, as in if it's a filestore, write the header info
	so it becomes initted */
	return this.storage.Init()
}

func (this *Stree_v) Startup(force bool) tools.Ret {
	/* Verify that the numbers the client send us make sense.
	 * The block size is the amount of space it takes us to store the key the value
	 * and all the offspring list metadata. */
	var measure_node *stree_v_node.Stree_node = stree_v_node.New_Stree_node(this.log, this.m_default_key, this.m_default_value, this.m_max_key_length, this.m_max_value_length, uint32(this.m_offspring_per_node))

	var max_block_size uint32 = measure_node.Serialized_size(this.m_max_key_length, this.m_max_value_length)

	if max_block_size != this.m_verify_client_block_size {
		return tools.Error(this.log, "the calculated block size ", max_block_size, " doesn't equal ",
			"supplied block size of ", this.m_verify_client_block_size)
	}

	// we also verify that the value type supplied is within the correct size, make sure client knows what it is doing.
	if uint32(len(this.m_default_value)) > this.m_max_value_length {
		return tools.Error(this.log, "the length of the default value ", len(this.m_default_value),
			" is greater than the provided max value length ", this.m_max_value_length)
	}
	if uint32(len(this.m_default_key)) > this.m_max_key_length {
		return tools.Error(this.log, "the length of the default key ", len(this.m_default_key),
			" is greater than the provided max key length ", this.m_max_key_length)
	}

	return this.storage.Startup(force)
}

func (this *Stree_v) Shutdown() tools.Ret {
	return this.storage.Shutdown()
}

func (this *Stree_v) Print(log *tools.Nixomosetools_logger) {
	var ret, rootnode = this.storage.Get_root_node()
	if ret != nil {
		fmt.Println(ret.Get_errmsg())
		return
	}
	if rootnode == 0 {
		fmt.Println("empty tree")
	} else {
		this.print_me(rootnode, "")
	}
	fmt.Println()
	var save_root_node uint32 = rootnode

	ret, free_position := this.storage.Get_free_position()
	if ret != nil {
		fmt.Println(ret.Get_errmsg())
		return
	}
	var lp uint32
	for lp = 1; lp < free_position; lp++ {
		var ret, n = this.Node_load(lp)
		if ret != nil {
			fmt.Println(ret.Get_errmsg())
			return
		}
		//xxxz for testing xxxz for debugging.
		//		var value string = n.Get_value()
		var _ = n //		fmt.Print("["+Uint32tostring(lp)+"] "+n.Get_key(), " (", value, ") ")
	}
	fmt.Println()
	fmt.Println("root node: ", save_root_node, " free position: ", free_position)
}

func (this *Stree_v) Get_metadata_size() uint32 {
	/* return the size of the stree node without the value on the end */

	if this.cached_metadata_size == 0 {
		var n stree_v_node.Stree_node = *stree_v_node.New_Stree_node(this.log, this.m_default_key, this.m_default_value,
			this.m_max_key_length, this.m_max_value_length, this.m_offspring_per_node)
		this.cached_metadata_size = n.Serialized_size_without_value(this.m_max_key_length, this.m_max_value_length)
	}
	return this.cached_metadata_size
}

func (this *Stree_v) Node_load_metadata(lp uint32) (tools.Ret, *stree_v_node.Stree_node) {
	/* same as node_load except it only reads enough of the data, or enough
	   blocks to get the header which is all we need when doing a search through
	   the tree. obviously the resulting node can't be used to read payload data. */
	if lp == 0 {
		return tools.Error(this.log, "sanity failure, somebody is trying to load node zero."), nil
	}
	var metadata_size = this.Get_metadata_size()
	var ret, bresp = this.storage.Load_limit(lp, metadata_size)
	if ret != nil {
		return ret, nil
	}
	// without clone this will add the same instance to every node
	// that might be okay because I think possibly the node gets replaced anyway? not sure. better safe than sorry.
	var n stree_v_node.Stree_node = *stree_v_node.New_Stree_node(this.log, this.m_default_key, this.m_default_value,
		this.m_max_key_length, this.m_max_value_length, this.m_offspring_per_node)
	ret = n.Deserialize_without_value(*this.log, bresp)
	if ret != nil {
		return ret, nil
	}
	return nil, &n

}

func (this *Stree_v) Node_load(lp uint32) (tools.Ret, *stree_v_node.Stree_node) {
	if lp == 0 {
		return tools.Error(this.log, "sanity failure, somebody is trying to load node zero."), nil
	}
	var ret, bresp = this.storage.Load(lp)
	if ret != nil {
		return ret, nil
	}
	// without clone this will add the same instance to every node
	// that might be okay because I think possibly the node gets replaced anyway? not sure. better safe than sorry.
	var n stree_v_node.Stree_node = *stree_v_node.New_Stree_node(this.log, this.m_default_key, this.m_default_value,
		this.m_max_key_length, this.m_max_value_length, this.m_offspring_per_node)
	ret = n.Deserialize(*this.log, bresp)
	if ret != nil {
		return ret, nil
	}
	return nil, &n
}

func (this *Stree_v) node_store(pos uint32, n *stree_v_node.Stree_node) tools.Ret {
	if pos == 0 {
		return tools.Error(this.log, "sanity failure, somebody is trying to store node zero.")
	}

	var ret tools.Ret
	var bn *bytes.Buffer
	ret, bn = n.Serialize()
	if ret != nil {
		return ret
	}
	var bnbytes = bn.Bytes()
	ret = this.storage.Store(pos, &bnbytes)
	return ret
}

func (this *Stree_v) print_me(pos uint32, last_key string) {

	var ret, n = this.Node_load(pos)
	if ret != nil {
		fmt.Println(ret.Get_errmsg())
		return
	}
	var left uint32 = n.Get_left_child()
	if left != 0 {
		this.print_me(left, last_key)
	}
	fmt.Print("[" + tools.Uint32tostring(pos) + "] " + n.Get_key() + " ")
	if n.Get_key() < last_key {
		fmt.Print("\n\nError, keys not in sequence: " + n.Get_key() + " vs " + last_key)
	}
	var right uint32 = n.Get_right_child()
	if right != 0 {
		this.print_me(right, last_key)
	}
}

func (this *Stree_v) calculate_offspring_nodes_for_value(value_length uint32) (tools.Ret, *uint32) {
	/* return how many offspring nodes we need for a value of this length,
	 * not the total number of nodes, don't include mother node */
	if value_length == 0 {
		var rval uint32 = 0
		return nil, &rval

	}
	var nnodes uint32 = value_length / this.m_max_value_length
	if value_length%this.m_max_value_length != 0 {
		nnodes++ // there was some data that spilled over to the next node
	}
	/* remove the count for the space in the mother node. If we were passed zero then this will yield -1
	 * so we check for zero above */
	nnodes--
	if nnodes > this.m_offspring_per_node { // they sent us a value larger than fits in the block
		return tools.Error(this.log,
			"value size ", value_length, " is too big to fit in ", (this.m_offspring_per_node + 1), " nodes of ",
			this.m_max_value_length, " length totaling ", (this.m_offspring_per_node+1)*this.m_max_value_length), nil
	}
	return nil, &nnodes
}

func (this *Stree_v) get_block_size_in_bytes() uint32 {
	/* this returns the number of bytes of user storable data in a node, it is not the size of the node.
	 * this is used to report to the user how much space is available to store, so it should be used in the
	 * used/total block count * this number to denote the number of actual storable bytes. */

	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	return this.m_max_value_length
}

func (this *Stree_v) Get_node_size_in_bytes() uint32 {
	/* Get_block_size_in_bytes returns the number of bytes you can store in the mother or one of
	the offpsring entries in a node. This function returns the total number bytes you can store
	in the entire stree node entry, which is the number of bytes you can store in the mother or
	offspring node times the additional nodes per block plus 1 */

	var max_value_length = this.get_block_size_in_bytes()
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	// let's see if we can nest locks, apparently you can not. not a recursive lock, good to know.
	var max_node_size = max_value_length * (this.m_offspring_per_node + 1) // +1 is for mother node
	return max_node_size
}

func (this *Stree_v) update(key string, new_value []byte) tools.Ret {
	// return error if not found, otherwise update the data  for key with new value
	// return error if there was a read or write problem.

	/* 11/3/2020 update with offspring is not so simple. If we are storing more than what's there, we need
	 * to add blocks, if we are storing less, we need to delete. We can't leave leftover junk laying around,
	 * it will waste space, and the system can't account for it. You can't have a node with no data in it.
	 * it will mess up the tree when you try and delete it. I suppose we could make a sorta dead pool
	 * in mother nodes, but that just makes everything messier than it already is for no gain. */

	// if key == nil {
	// 	return Error(s.log, "trying to update with null key")
	// }
	// if new_value == nil {
	// 	return Error(log, "trying to update with null value")
	// }

	var ret, found, resp, resppos = this.search(key, false)
	if ret != nil {
		return ret
	}

	if found == false {
		return tools.Error(this.log, "key ", key, " not found.") // can't update what we can't find.
	}

	var n *stree_v_node.Stree_node = resp // this is the mother node
	var pos uint32 = resppos              // this is the physical block position it is located at, I think.

	return this.perform_new_value_write(n, pos, new_value)
}

func (this *Stree_v) perform_new_value_write(mother_node *stree_v_node.Stree_node, mother_node_pos uint32, new_value []byte) tools.Ret {
	/* this handles new inserts and updates. In the case of insert it will either just write the value in the mother
	 * node and expand into offspring if need be, in the case of updates, it can possibly grow or shrink the
	 * offspring list. */
	// get a count of how many offspring there are now in mother node
	var current_offspring_count uint32 = mother_node.Count_offspring()

	// figure out how many nodes we need to store this write.
	var new_value_length uint32 = uint32(len(new_value))

	var ret, iresp = this.calculate_offspring_nodes_for_value(new_value_length)
	if ret != nil {
		return ret
	}
	var offspring_nodes_required uint32 = *iresp
	/* first handle shrinking if need be */
	//s.log.Debug("current additional nodes: " + Uint32tostring(current_offspring_count) + " additional nodes required: " + Uint32tostring(offspring_nodes_required))
	if offspring_nodes_required < current_offspring_count {
		/* all the nodes past what we need get deleted and zeroed in mother's offspring array */

		/* Very sneaky problem here:
		   this physically_delete_one delete of offspring can cause anything to move, including the mother node.
		   we must be careful to update the new location of the mother node when
		   we go to write updates because it might have moved.
		   So I thought this out and it's true, so we can either make the list of nodes to delete up front
		   and just modify the list as it runs through (just like delete does) or we can re-read the mother
		   node each time (following it if it has moved) until we've removed enough offspring nodes.
		   I guess the first way is easier. but we still need to keep track of the movement of the mother node
		   because I think it needs to be updated later anyway with the update we're trying to do. */

		// make a list of the offspring positions to delete
		var amount_to_remove uint32 = current_offspring_count - offspring_nodes_required
		this.log.Debug("removing: " + tools.Uint32tostring(amount_to_remove) + " nodes.")

		var delete_list []uint32 = make([]uint32, amount_to_remove) // need to including deleting of mother node
		var delete_list_pos uint32 = 0
		/* We have to delete from right to left to maintain the correctness of the offspring list, we need
		 * to be able to always go from left to right until we get to a zero. If we delete from the left
		 * we'll have to set a zero in the first position (for really complicated reasons) and then
		 * there will be valid live offspring after that and that's an invalid offspring layout
		 * so we must delete from right to left. */
		var rp int // must be int, because we're counting down to zero, and need to get to -1 to end loop
		for rp = int(current_offspring_count - 1); rp >= int(offspring_nodes_required); rp-- {
			var ret, resp = mother_node.Get_offspring_pos(uint32(rp))
			if ret != nil {
				return ret
			}
			var offspring_value uint32 = *resp
			if offspring_value == 0 {
				break
			}
			delete_list[delete_list_pos] = offspring_value
			delete_list_pos++
			this.log.Debug("removing node: " + tools.Uint32tostring(offspring_value))
		}

		/* Now go through the delete list individually deleting each item, updating the list if
		 * something in the list got moved. also keep the mother node position up to date */

		this.log.Debug("going to delete " + tools.Uint32tostring(delete_list_pos) + " items.")
		var dp uint32
		for dp = 0; dp < delete_list_pos; dp++ {
			var pos_to_delete uint32 = delete_list[dp]
			var ret, moved_from_resp, moved_to_resp = this.physically_delete_one(pos_to_delete)
			if ret != nil {
				return ret
			}

			/* now update the remainder of the list if anything in it moved. we can do the whole list,
			 * it doesn't hurt to update something that was already processed/deleted. */
			var from uint32 = moved_from_resp
			var to uint32 = moved_to_resp
			this.log.Debug("moved mover from " + tools.Uint32tostring(from) + " to " + tools.Uint32tostring(to))
			var wp int
			for wp = rp + 1; wp < int(delete_list_pos); wp++ { // as we deleted wp in this round, we don't need to update it.
				if delete_list[wp] == from {
					delete_list[wp] = to
					this.log.Debug("mover node " + tools.Uint32tostring(from) + " was in the delete list so we moved it to " + tools.Uint32tostring(to))
				}
			}
			if mother_node_pos == from {
				mother_node_pos = to
				this.log.Debug("mover node " + tools.Uint32tostring(from) + " was the mother node and was in the delete list so we moved it to " + tools.Uint32tostring(to))
			}
			if this.debugprint {
				this.Print(this.log)
			}
		}

		/* so what is on disk is correct, what is in memory is probably/possibly not.
		 * read mother back in before we do anything. we have to zero out the mother node index
		 * entries for the guys we just deleted unless delete does that already.
		 * I just checked it doesn't so we have to do that here, but I think that's it.
		 * 11/20/2020 so now physically_delete_one does clear out the mother's offspring so we can probably skip this. */

		Ret, mother_node_resp := this.Node_load(mother_node_pos)
		if ret != nil {
			return Ret
		}

		mother_node = mother_node_resp
		// okay so now we have the mother node back, zero out the offspring nodes we just deleted.
		/* this was actually taken care of in physically_delete_one, so we're done already. */
	} else /* if handling shrinking. */ { /* the add more nodes if we need them, third option is old and new size (number of offspring)
		   are the same, do nothing. */
		if offspring_nodes_required > current_offspring_count {
			// add some empty nodes and fill in the array with their position.
			var amount uint32 = offspring_nodes_required - current_offspring_count
			//this.log.Debug("allocating " + tools.Uint32tostring(amount) + " new nodes to expand for update.")
			var iresp []uint32
			ret, iresp = this.storage.Allocate(amount)
			if ret != nil {
				return ret
			}

			// now peel off the new node numbers and add them to the offspring array in the correct position.
			var i int = 0
			for rp := current_offspring_count; rp < offspring_nodes_required; rp++ {
				var new_node_pos uint32 = iresp[i]
				i++
				mother_node.Set_offspring_pos(rp, new_node_pos)
				this.log.Debug("adding offspring node " + tools.Uint32tostring(new_node_pos) + " to position " + tools.Uint32tostring(rp) + " in mother offspring list.")
			}
		} // else {
		//s.log.Debug("node update takes the same number of offspring, no offspring change.")
		//	}
	}
	/* so now in all cases, we've allocated or freed the space, set the offspring array to match
	 * and now we just have to update the data. */

	if len(new_value) == 0 { // unless of course it's empty

		this.log.Debug("new value length is zero.")
		var ret = mother_node.Set_value(new_value)
		if ret != nil {
			return ret
		}
		return this.node_store(mother_node_pos, mother_node)
	}

	/* break new_value up into pieces that can fit into a node across mother and offspring,
	 * we're going to be updating/writing over all the offspring nodes, we don't
	 * have to read update write, we know their on disk position, we just generate new nodes
	 * and write them. just the mother node is important to maintain, and we've already
	 * loaded it. */

	var num_parts uint32 = uint32(len(new_value)) / this.m_max_value_length
	if len(new_value)%int(this.m_max_value_length) != 0 {
		num_parts++
	}

	if num_parts == 0 {
		return tools.Error(this.log, "error splitting data value by ", this.m_max_value_length, " bytes, new_value is empty.")
	}

	var data_parts [][]byte = make([][]byte, num_parts)

	var counter uint32
	var pos int = 0
	for counter = 0; counter < num_parts; counter++ {
		var end_of_data = tools.Minint(len(new_value[pos:]), int(this.m_max_value_length))

		data_parts[counter] = new_value[pos : pos+end_of_data]

		pos += int(this.m_max_value_length)
	}

	if num_parts != (offspring_nodes_required + 1) {
		return tools.Error(this.log, "calculated nodes ", tools.Uint32tostring(offspring_nodes_required+1),
			" doesn't match count of split data: ", tools.Uint32tostring(num_parts))
	}

	// put the first one in the mother node
	var partpos int = 0
	ret = mother_node.Set_value(data_parts[partpos])
	if ret != nil {
		return ret
	}
	partpos++
	/* all the offspring values are set, the parent and children values don't change, we're
	 * just updating the value, not the key. write the mother node to disk. */
	ret = this.node_store(mother_node_pos, mother_node)
	if ret != nil {
		return ret
	}

	// now do all the offspring, we have to make new nodes for each, they've already been allocated on disk.
	var lp uint32
	for lp = 0; lp < offspring_nodes_required; lp++ {
		var offspring_node *stree_v_node.Stree_node = stree_v_node.New_Stree_node(this.log, this.m_default_key, data_parts[partpos],
			this.m_max_key_length, this.m_max_value_length, 0) // offspring nodes have no offspring node array
		partpos++
		// none of the fields matter except the data and the parent. everything else should be zero.
		offspring_node.Set_parent(mother_node_pos)
		var ret, iresp = mother_node.Get_offspring_pos(lp)
		if ret != nil {
			return ret
		}
		var offspring_pos_to_store uint32 = *iresp
		// this.log.Debug("updating data in offspring position ", tools.Uint32tostring(lp), " pointing to node ", offspring_pos_to_store)
		ret = this.node_store(offspring_pos_to_store, offspring_node)
		if ret != nil {
			return ret
		}
	}
	// if we got here we're good.
	return nil
}

func (this *Stree_v) Update_or_insert(key string, new_value []byte) tools.Ret {
	/* this function will insert if not there and update if there, so no duplicates will be created in
	 * this situation. */
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	return this.update_or_insert_always(key, new_value, false)
}

func (this *Stree_v) update_or_insert_always(key string, new_value []byte, insert_always bool) tools.Ret {

	/* if we are forcing an insert, if it is not found we will end up at a leaf, and if it is found
	 * we will end up at the leafiest of the matching node */
	/* if we are not forcing an insert, then if it's found, we will update what we find
	 * and if it's not found, we will be at a leaf, which is just what we want. */

	var ret, respfound, respnode, respnodepos = this.search(key, insert_always)
	if ret != nil {
		return ret
	}
	var found bool = respfound
	// fn and fpos refer to the the parent node and its position to whom this new child will be added.
	var fn *stree_v_node.Stree_node = respnode
	var fpos uint32 = respnodepos

	if (found == false) || insert_always {
		/* 11/18/2020 so in the case where we fail we have to try and not mess up the tree. Originally
		 * I was writing the pointing-to node first thus screwing up the tree if the new node write
		 * fails. Then I added the dealloc, but now I realize all I have to do, is try and write the new
		 * node first, if that fails, bail, no problem.
		 * if that works write the parent/pointing-to node and if that fails, then just dealloc
		 * and the tree should be okay, except for multiple deallocs needed and if the parent update
		 * partially worked, but that's a lower layer problem. */
		/* 11/18/2020 a few minutes later. okay so I figured out why we do it backwards.
		 * No, wait, when inserting a new mother and offspring... why do we have to update the
		 * parent first? not seeing it. xxxz Revisit and try and write new nodes first, then parent.
		 * I guess because the parent can move? no this is an insert there's no deleting so nothing
		 * will move. I dunno.
		 * Not seeing why it won't work, I'll try it and wait for something to show up.
		 * So we will write the new parent node, write the offspring and if all that works
		 * update the parent to point to the new node. That way we can bail early and skip that
		 * step if the node write fails and not have messed up the tree at all in some complicated
		 * way that would be hard to recover from. This is all only for inserts. */
		// not found, or inserting duplicate, insert at returned node
		// we're not supplying the new_value to the mother node that gets done later

		if found && insert_always {
			this.log.Debug("this is a forced insert and there's already a key matching ", key, ", inserting a duplicate.")
		}

		var nn *stree_v_node.Stree_node = stree_v_node.New_Stree_node(this.log, key, make([]byte, 1), this.m_max_key_length, this.m_max_value_length, this.m_offspring_per_node)

		// make room, and add new node at first free spot
		var ret, iresp = this.storage.Allocate(1) // ask for one node for the mother node
		if ret != nil {
			return ret
		}

		var new_item_pos uint32 = iresp[0]
		// now point the parent to the new node
		if fpos == 0 {
			// we have to do node value update last since it's more complicated now.
			// not ideal in that we're setting the root to point to data that hasn't been written yet.
			/* 11/18/2020 okay... why did I say that, oh I think it's because if things get moved because
			   an update causes a delete and thus nodes to move, we need to have written the final thing already.
			   No, that can't be it, this is an insert, so nothing will get deleted or moved. Not sure what I was
			   thinking when I wrote that. We might still be able to write the node first.
			   so thinking about it more I think that is unneccesary. we always write the new mother node
			   then call new_value_write to expand the offspring if need be. In either case, the mother node
			   is not going to move and we can point the parent to it after we're sure the offspring wrote
			   correctly. I'm not seeing why that doesn't work. What was I thinking... */

			// try and write the new node first.
			ret = this.perform_new_value_write(nn, new_item_pos, new_value)
			if ret != nil {
				/* if we didn't succeed, undo the allocate so we don't mess up the tree */

				this.deallocate_on_failure()
				return ret
			}
			// if that works, update the parent (root node) to point to it */
			ret = this.storage.Set_root_node(new_item_pos) // adding first node at root.
			if ret != nil {
				this.deallocate_on_failure()
				return ret
			}

			return nil
		}
		if key > fn.Get_key() {
			fn.Set_right_child(new_item_pos)
		} else {
			fn.Set_left_child(new_item_pos) // duplicates will get inserted on the left.
		}

		// set the new node's parent to the ... parent.
		nn.Set_parent(fpos) // not on disk yet.

		/* write the new node and offspring to disk, expanding allocation if need be.
		 * the new node would have been created with a fully allocated but completed zeroed
		 * offspring array, so perform_new_value_write will then allocate the required nodes
		 * for the offspring data. */
		/* So here we set nn's parent but we don't immediately write to disk.
		 * writing nn to disk happens in new_value_write, it is worth noting that in the case
		 * where we shrink the offspring list, the new node can move and it gets reloaded and
		 * we'd lose the above set_parent(fpos) but that only happens when shrinking, this is
		 * a new node to be inserted and possibly have offspring added to, so that mother/new node
		 * reload never happens so we don't lose the set_parent update. */
		ret = this.perform_new_value_write(nn, new_item_pos, new_value) // xxxz this needs to deallocate it's allocations on failure if any.
		if ret != nil {
			this.deallocate_on_failure()
			return ret
		}

		/* set the parent's new child info to point to mother of new node, actually write to disk now that we
		 * know the new node we're going to point to is there really on disk now. */
		ret = this.node_store(fpos, fn)
		if ret != nil {
			this.deallocate_on_failure()
			return ret
		}

		return nil
	}
	// found the node, just update value
	return this.perform_new_value_write(fn, fpos, new_value)
}

func (this *Stree_v) deallocate_on_failure() {
	var deRet tools.Ret = this.storage.Deallocate()
	if deRet != nil {
		tools.Error(this.log, "unable to deallocate tree item after insert failure, tree is corrupt: ", deRet.Get_errmsg())
	}
}

func (this *Stree_v) Insert(key string, value []byte) tools.Ret {
	return this.update_or_insert_always(key, value, true)
}

func (this *Stree_v) Fetch(key string) (Ret tools.Ret, Retfoundresp bool, resp []byte) {
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()

	/* client should call this to fetch a block from the backing store.
	if found(0) is true, mothernoderesp only has the metadata for that node.
	you must call node.Load if you need the mother node's data. */
	var ret, foundresp, mothernoderesp, _ = this.search(key, false)
	if ret != nil {
		return ret, false, nil
	}

	var found bool = foundresp
	if found == false {
		return nil, false, nil
	}

	var found_mother_node stree_v_node.Stree_node = *mothernoderesp // now this has the node's value as well.
	var fRet, respdata = this.fetch_stree_data(found_mother_node)
	if fRet != nil {
		return fRet, false, nil
	}
	return fRet, true, respdata
}

func (this *Stree_v) fetch_stree_data(found_mother_node stree_v_node.Stree_node) (tools.Ret, []byte) {
	// we don't know how much data there is, but we know it can't be bigger than this.
	/* actually we CAN know what it is, actually, we can almost know. we need the variable size value
	 * in the last offspring node, and that we don't find out until we read it in, so we can either size
	 * to just the number of offspring nodes, or just max out completely, either way we have to resize so it
	 * almost doesn't matter. we waste a bit more memory here, temporarily. */
	var alldata *bytes.Buffer = bytes.NewBuffer(make([]byte, 0, this.m_max_value_length*(this.m_offspring_per_node+1)))

	// add the data in the mother node.
	var node_data []byte = found_mother_node.Get_value()
	var bdata []byte = []byte(node_data)
	ralldata := append((*alldata).Bytes(), bdata...)

	/* here we have to fetch and append all the offspring if any.
	 * we always append the full max length value for each node.
	 * we don't store the length of partial node values. */
	var lp uint32
	for lp = 0; lp < this.m_offspring_per_node; lp++ {
		// get the offspring node pos

		var osresp *uint32
		var ret tools.Ret
		ret, osresp = found_mother_node.Get_offspring_pos(lp)
		if ret != nil {
			return ret, nil
		}
		var offspring_pos uint32 = *osresp
		if offspring_pos == 0 { // did we run out
			break
		}
		// go get the node
		var offspring_node_resp *stree_v_node.Stree_node
		ret, offspring_node_resp = this.Node_load(offspring_pos)
		if ret != nil {
			return ret, nil
		}
		var found_offspring_node stree_v_node.Stree_node = *offspring_node_resp
		// get the data and add it.
		var node_data []byte = found_offspring_node.Get_value()
		var bdata []byte = []byte(node_data)
		ralldata = append(ralldata, bdata...)
	}
	// now we just need to resize this array down to the actual size of the data
	// we allocated the max possible, but we really only want to return the exact data
	var exactdata []byte = ralldata[:] // this is probably pointless, it's not capacity, it's length

	return nil, exactdata
}

func (this *Stree_v) search(key string, to_insert bool) (fRet tools.Ret, respfound bool, respnode *stree_v_node.Stree_node, respnodepos uint32) {
	/* internal use for insert and update and delete and fetch, use fetch to actually get the data as a client. */
	// upon search success (finding the node) it returns found boolean, the node and the position of that node
	// if not found it returns not found boolean the node and position of the last search point.

	/* 12/12/2021 to correctly support duplicates, the problem I found was when we find a node, we stop at the
	   * first one, if there are duplicates, we need to keep going until we get to the leaf one, so that insert
	   * will be able to correctly insert in tree order. so find the 10, or the 12.
	   * The real answer I now realize is that search to find and search to insert are two different functions.
	   * search to insert has to go until it gets to a leaf.
		               12
		             /     \
		           10      20
		          /  \     /
		         9   11   12
		            /      \
		          10       12 */
	var ret, iresp = this.storage.Get_root_node()
	if ret != nil {
		return ret, false, nil, 0
	}
	var i uint32 = iresp
	var sn *stree_v_node.Stree_node = nil
	var spos uint32 = i

	for {
		if i == 0 {
			// not found
			// last known good node to which an insert would add, if null then tree is empty
			/* we also have to load the last full node we searched through to get the payload
			   because the caller is going to update this node. */
			if spos != 0 {
				ret, sn = this.Node_load(spos)
				if ret != nil {
					return ret, false, nil, 0
				}
			}
			return nil, false, sn, spos
		}
		var ret, nresp = this.Node_load_metadata(i)
		if ret != nil {
			return ret, false, nil, 0
		}
		sn = nresp
		spos = i
		if (to_insert == false) && (key == sn.Get_key()) {
			// found, return the found node and its position, note sn does not have the value payload.

			/* so it turns out there are more callers to search() than I thought, and most of them expect the value
			to be there, because they rewrite the block they're updating, so go fetch the entire block we're going to
			return before we return it. */
			ret, sn = this.Node_load(spos)
			if ret != nil {
				return ret, false, nil, 0
			}
			// we were able to read the entire block, return found with the data.
			return nil, true, sn, spos
		} else {
			if key > sn.Get_key() {
				i = sn.Get_right_child()
			} else {
				i = sn.Get_left_child() // duplicates will hang off the left
			}
		}
		// quick sanity check since this actually happened to me 12/22/2020, as a result of failing deletes during testing that I didn't clean up
		/* it would be smarter to make a list of the chain we followed and make sure we never see a duplicate so we can
		 * make sure we don't further ruin our tree and detect problems as early as possible... */
		if i == spos {
			return tools.Error(this.log, "sanity failure tree node ", spos, " has a child that refers to itself."), false, nil, 0
		}
	} // for true
	// return Error(s.log, "sanity failure can not reach this return path."), false, nil, 0
}

// I forget what this is for, I think it's for sponge
/* ahhh, I looked it up, it's for the write back cache. because deletes are expensive, we
process the write back cache from the end, so the delete is relatively cheap.
what order we process the write back cache in doesn't matter, so this is perfectly fine. */
func (this *Stree_v) fetch_last_physical_block() (tools.Ret, bool, *[]byte) {
	/* if there is no data in the stree, return foundresp false, otherwise return true
	 * and send back the data for the last physical block in the stree. */
	/* THIS ONLY WORKS IF YOU HAVE AN STREE WITH NO OFFSPRING. if there are offspring, the last physical
	 * node might not be the parent, and you could look up the parent, but it would defeat the purpose
	 * of this optimization for zos write back cache. */

	if this.m_offspring_per_node != 0 {
		return tools.Error(this.log, "you can not fetch the last physical block of an stree that has offspring."), false, nil
	}
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()

	var r, iresp = this.storage.Get_root_node()
	if r != nil {
		return r, false, nil
	}

	// if root node is zero, then tree is empty, therefore no data.
	var i uint32 = iresp
	if i == 0 {
		return nil, false, nil
	}

	/* the free position is one greater than the last physical block. We know there is data
	 * because the root node is not zero, so we can safely subtract one to get the last
	 * physical node. */
	r, iresp = this.storage.Get_free_position()
	if r != nil {
		return r, false, nil
	}
	var last_node uint32 = iresp - 1

	/* load in the last_node, then call common fetcher to return it.
	 * this doesn't do a double read, it mostly does nothing but this way
	 * all fetches return data the same way. */

	r, nresp := this.Node_load(last_node)
	if r != nil {
		return r, false, nil
	}
	var last_Stree_node stree_v_node.Stree_node = *nresp

	r, resp := this.fetch_stree_data(last_Stree_node)
	if r == nil {
		return r, false, nil
	}
	return nil, true, &resp
}

func (this *Stree_v) find_deeper_path(pos uint32) (a tools.Ret, iresp uint32) {
	/* follow rightmost of left child and leftmost of right child and return the one in the
	 * lowest/deepest position. */
	if pos == 0 {
		return tools.Error(this.log, "find deeper path was passed empty tree."), 0
	}
	var ret, resp = this.Node_load(pos)
	if ret != nil {
		return ret, 0
	}
	var n stree_v_node.Stree_node = *resp
	var left uint32 = n.Get_left_child()
	var right uint32 = n.Get_right_child()
	if (left == 0) || (right == 0) { // we should only ever be called on a node with 2 children
		return tools.Error(this.log, "find deeper path was passed a node with less than 2 children."), 0
	}
	var deepest uint32 = left
	for (left != 0) || (right != 0) {
		if left != 0 {
			deepest = left
			var ret, resp = this.Node_load(left)
			if ret != nil {
				return ret, 0
			}
			var n stree_v_node.Stree_node = *resp
			left = n.Get_right_child()
		}
		if right != 0 {
			deepest = right
			var ret, resp = this.Node_load(right)
			if ret != nil {
				return ret, 0
			}
			var n = resp
			right = n.Get_left_child()
		}
	}

	return nil, deepest
}

func (this *Stree_v) logically_delete(pos uint32) tools.Ret {
	/* step 1, the delete algorithm:
	   * if there's no children, update parent's child to point to nothing.
	   * if node has one child, update parent's child to point to deleted node's child
	   * if the node has two children:
	   * follow left child's right most child, and right child's left most child, pick whichever is longer.
	   * let's say it's the right child's leftmost, it only has one child.
	   * move it to the deleted spot by
	   * 1) setting moving node's left child to deleted node's left
	   * 2) set moving node's new left child's parent to moving node
	   * 3) set moving node's right child to deleted node's old right node
	   * 4) set moving node's new right child's parent to moving node
	   * 5) set deleted node's parent's child to moving node
	   *
	   * actually flat puts it nicely... (guest/flat.c)
	   *
	   *   Check the simplest case. no kids
	       tell my parent that I'm gone

	       next easiest case, 1 kid
	       tell my parent that their kid is my kid
	       If I was the root node, my kid becomes the root node
	       And tell my kid who his new parent is

	       if we're here, this is the worst case, we have 2 kids.
	       the trick is to get my immediate sucessor or predecessor
	       and put them in my spot.
	       The predecessor is found by going left once and going right
	       till you're at a leaf. Kill him and he's our parent's new
	       kid.
	       He's either a leaf or a single child, kill him (call delindex recursively)
	       The oddest case is where the pred is our kid, have to delete
	       him (we just did) and then reread our index record because
	       it just got changed
	       Tell the origional record's 2 kids who their
	       new parent is. Since we know we have 2 kids we shouldn't
	       have to check except for the bad case that our pred is also
	       our kid, and he may have gone away just now
	       and Now tell our parent that his kid is our pred
	       if we're rootnode, our predecessor becomes the root node. */

	/* 11/6/2020 before we do any of that, see if we're a mother node or an offspring node.
	 * if we're offspring, it's a lot easier, actually, you never logically delete an
	 * offspring node, you only physically delete them. And when physically deleting, if an
	 * offspring node gets moved, you have to work differently, but logically deleting an
	 * offspring node is impossible. */

	/* 11/20/2020 when logically deleting a mother node, we must set its parent and children values
	 * to int_max. this is a flag for later when we physically delete to make sure we don't try
	 * and update pointers of an orphaned node if the mother is being moved because the offspring
	 * are being physically deleted. */

	/* 12/22/2020 our first bug. when you delete the last node it seems we don't always set the root
	 * to zero, my guess is because logically delete never took offspring into account.
	 * 12/23/2020 Nope that wasn't it, but I figured it out. Check this out.
	 * If you're logically deleting the last node, the root pointer gets set to zero, then we go about
	 * physically deleting all of the offspring then the mother. While doing this, unless the mother is
	 * already in position 1, which is unlikely, it will get moved, probably a number of times as it will
	 * have to eventually get shuffled all the way to position one, because we delete all the offspring
	 * first, then the mother node. So when the mother node gets moved, if it is the last one, it is
	 * also the root node, so we update the root node to point to the newly moved location of the mother.
	 * EVEN IF THE MOTHER HAS BEEN LOGICALLY DELETED, and that is the problem. We can only update the
	 * root node, if the root node is pointing to the thing being moved. Basically our check to see
	 * if the mother node was the root node wasn't good enough. It was only checking if the mother's
	 * parent was zero, it should also check that the root node points to that parent. If it does not
	 * then we won't update the root node. */

	var ret, resp = this.Node_load(pos)
	if ret != nil {
		return ret
	}
	var n stree_v_node.Stree_node = *resp

	/* first check and see if it's an offspring node. */
	if n.Is_offspring() {
		return tools.Error(this.log, "sanity failure, request to logically delete an offspring node, position: ", pos)
	}

	/* Check the simplest case. no kids */
	if (n.Get_left_child() == 0) && (n.Get_right_child() == 0) {
		/* tell my parent that I'm gone */
		if n.Get_parent() == 0 { /* deleting root node */
			return this.storage.Set_root_node(0)
		}
		/* Whichever child of the parent we are, remove it. */
		var ret, resp = this.Node_load(n.Get_parent())
		if ret != nil {
			return ret
		}

		var p *stree_v_node.Stree_node = resp
		if p.Get_left_child() == pos {
			p.Set_left_child(0)
		} else {
			if p.Get_right_child() == pos {
				p.Set_right_child(0)
			} else {
				return tools.Error(this.log, "sanity failure, deleting node with no children ", pos, " we're trying to find ",
					pos, " as one of my parent's children but I am not found.")
			}
		}

		ret = this.node_store(n.Get_parent(), p)
		if ret != nil {
			return ret
		}
		return this.logically_orphan_mother_node(pos) // set pointers to max_int for physical delete later.
	}

	/* next easiest case, 1 kid */
	if ((n.Get_left_child() == 0) || (n.Get_right_child() == 0)) && (((n.Get_left_child() != 0) && (n.Get_right_child() != 0)) == false) {
		/* tell my parent that their kid is my kid */
		var mykid uint32

		if n.Get_left_child() == 0 {
			mykid = n.Get_right_child()
		} else {
			mykid = n.Get_left_child()
		}
		if n.Get_parent() == 0 { /* If I was the root node, my kid becomes the root node */
			ret = this.storage.Set_root_node(mykid)
			if ret != nil {
				return ret
			}
			/* And he has no parent */
			var ret, resp = this.Node_load(mykid)
			if ret != nil {
				return ret
			}
			var c *stree_v_node.Stree_node = resp
			c.Set_parent(n.Get_parent()) /* my child's new parent is my parent which is zero */
			ret = this.node_store(mykid, c)
			if ret != nil {
				return ret
			}
			return this.logically_orphan_mother_node(pos) // set pointers to max_int for physical delete later.
		}

		/* Whichever child of the parent we are, replace it with my child.*/
		ret, resp = this.Node_load(n.Get_parent())
		if ret != nil {
			return ret
		}
		var p *stree_v_node.Stree_node = resp
		if p.Get_left_child() == pos {
			p.Set_left_child(mykid)
		} else {
			if p.Get_right_child() == pos {
				p.Set_right_child(mykid)
			} else {
				return tools.Error(this.log, "sanity failure, deleting node ", pos, " we're trying to find ", pos,
					" as one of my parent's children but I am not found.")
			}
		}

		ret = this.node_store(n.Get_parent(), p)
		if ret != nil {
			return ret
		}

		/* And tell my kid who his new parent is */
		var ret, resp = this.Node_load(mykid)
		if ret != nil {
			return ret
		}
		var c *stree_v_node.Stree_node = resp
		c.Set_parent(n.Get_parent()) /* my child's new parent is my parent */
		ret = this.node_store(mykid, c)
		if ret != nil {
			return ret
		}
		return this.logically_orphan_mother_node(pos) // set pointers to max_int for physical delete later.
	}

	/* if we're here, this is the worst case, we have 2 kids. */
	return this.logically_delete_two_kids(pos)
}

func (this *Stree_v) logically_delete_two_kids(pos uint32) tools.Ret {
	/* the trick is to get my immediate sucessor or predecessor
	   and put them in my spot.
	   The predecessor is found by going left once and going right
	   till you're at a leaf. delete him and he's our parent's new kid. */
	/* to assist with balancing the tree a bit, we'll search both directions
	 * and pick the deeper one to pull from. */

	var ret, iresp = this.find_deeper_path(pos)
	if ret != nil {
		return ret
	}
	var mover uint32 = iresp
	if mover == 0 {
		return tools.Error(this.log, "find deeper path returned root node.")
	}
	/* He's either a leaf or a single child, delete him in one of the simpler above ways. */
	/* so with offspring this is a problem, if the thing we logically delete is a mother
	   node... oh wait, it's logically deleting, it's just pulling it out of the pointers of
	   the tree, it's not moving anything, offspring not affected, never mind. */

	/* 11/20/2020 this does however, logically orphan this node, we have to update his relations later
	 * to compensate */
	ret = this.logically_delete(mover)
	if ret != nil {
		return ret
	}
	/* that logically removed mover out of the tree, but its data is still located at the
	 * mover position in the array, we now just repoint everybody to make mover the node logically replacing
	 * the pos that we're actually deleting. If mover was a direct child of pos, then pos got modified too.
	 * That's okay and good though, because we're going to need the new values to update
	 * mover with. */
	/* The oddest case is where the mover is our kid, we have to delete
	   him (we just did) and then reread our pos index record because
	   it just got changed. */

	/* Tell the origional pos's record's 2 kids who their
	   new parent is. Since we know we have 2 kids we shouldn't
	   have to check except for the bad case that mover is also
	   our kid, and he may have gone away just now */

	var resp *stree_v_node.Stree_node
	ret, resp = this.Node_load(pos) // the one we're going to actually delete, might have a new child from above delete.
	if ret != nil {
		return ret
	}
	var n stree_v_node.Stree_node = *resp
	if n.Get_left_child() != 0 {
		ret, resp = this.Node_load(n.Get_left_child())
		if ret != nil {
			return ret
		}
		var nl *stree_v_node.Stree_node = resp
		nl.Set_parent(mover)
		ret = this.node_store(n.Get_left_child(), nl)
		if ret != nil {
			return ret
		}
	}
	if n.Get_right_child() != 0 {
		var ret, resp = this.Node_load(n.Get_right_child())
		if ret != nil {
			return ret
		}
		var nr *stree_v_node.Stree_node = resp
		nr.Set_parent(mover)
		ret = this.node_store(n.Get_right_child(), nr)
		if ret != nil {
			return ret
		}
	}

	/* and now tell pos's parent that his kid is mover */
	var p uint32 = n.Get_parent()
	if p == 0 { /* if we're rootnode, mover becomes the root node */
		ret = this.storage.Set_root_node(mover)
		if ret != nil {
			return ret
		}
	} else {
		/* Whichever kid we WERE, point that to mover */
		var ret, resp = this.Node_load(p)
		if ret != nil {
			return ret
		}
		var np *stree_v_node.Stree_node = resp
		if np.Get_left_child() == pos { // if we're pos's parent's left child point left to mover
			np.Set_left_child(mover)
		} else {
			if np.Get_right_child() == pos { // if we're pos's parent's right child point right to mover
				np.Set_right_child(mover)
			} else {
				return tools.Error(this.log,
					"sanity failure, corrupt tree, mover is not either child of its parent while deleting node "+tools.Uint32tostring(pos))
			}
		}
		ret = this.node_store(p, np)
		if ret != nil {
			return ret
		}
	}

	/* now we have to fix up mover to point to his new parent and children,
	 * which is just taking the place of n, so we grab all his relations. */
	ret, resp = this.Node_load(mover) // the one we're going to actually delete, might have a new child from above delete.
	if ret != nil {
		return ret
	}
	var m *stree_v_node.Stree_node = resp
	m.Set_parent(n.Get_parent())         // if n was root, we are now root, our parent = 0
	m.Set_left_child(n.Get_left_child()) // this might have been modified by the simple delete above
	m.Set_right_child(n.Get_right_child())
	ret = this.node_store(mover, m)
	/* 11/20/2020 now that we've copied the node we're logically deleting's relations to mover
	 * we can logically orphan him. */
	if ret != nil {
		return ret
	}
	return this.logically_orphan_mother_node(pos) // set pointers to max_int for physical delete later.
}

func (this *Stree_v) logically_orphan_mother_node(pos uint32) tools.Ret {
	/* 11/20/2020 when we logically delete a mother node, we must flag its relations as max_int
	 * to deal with a physically delete problem, later.
	 * I now realize this is all way more complicated than I expected and it would have made more
	 * sense to store the mother nodes at the beginning of the disk, and the offspring at the end
	 * of the disk (growing backwards) but this would guarantee the absolute worst possible seek times
	 * because every read is pRetty much guaranteed to have to go to both ends of the disk. So this
	 * is still a better deal. despite the complexity. maybe. */

	var ret, resp = this.Node_load(pos)
	if ret != nil {
		return ret
	}
	var orphan *stree_v_node.Stree_node = resp
	orphan.Set_parent(math.MaxUint32)
	orphan.Set_left_child(math.MaxUint32)
	orphan.Set_right_child(math.MaxUint32)
	ret = this.node_store(pos, orphan)
	if ret != nil {
		return ret
	}
	return nil
}

func (this *Stree_v) physically_delete(pos uint32) tools.Ret {
	/* step 2, is just physically copy the data from the last node to the hole,
	 * then find the parent of the moved node and repoint it to the newly filled hole,
	 * then the children's parents must also be set to its new location, the newly
	 * filled hole. */

	/* 11/6/2020 so physically deleting a node is a bit more interesting with offspring.
	 * if the node is a mother node, we have to delete all the offspring, and the mother node.
	 * makes sense to do it in reverse in case they're all at the end, deleting is easier.
	 * if deleting an offspring node, we would have to update the parent, but we know it's going away
	 * so we don't have to do that, all we do have to do is if we're MOVING an offspring node, update
	 * the parent's offspring array to point to the new location.
	 * It sounds a bit risky that we are going to recurse a bit when we delete a mother node because
	 * we have to physically delete all the offspring nodes, but if the tree isn't corrupt, it should
	 * work out fine. Famous last words. */

	/* so here is where we care if it's a mother node or offspring being deleted we can take care of two things.
	 * if it's a mother node the rewrite of the ones that point to it are the same as stree_iii
	 * with the added caveat that we have to go through the effort of deleting the offspring as well.
	 * and if it's an offspring node being moved, rewriting the parent is a little different.
	 * actually we have to think about this a little more.
	 * deleting a mother node just means we have to delete the offspring
	 * but keep in mind that deleting offspring might cause the mother node to move.
	 * so we can't just delete this position we got because it might be different after we do the offspring
	 * so maybe order is important.
	 * I keep missing that there's an important distinction between the nodes that are moving and the one(s)
	 * being deleted. it was a lot simpler when there was only one thing being deleted.
	 * so let's go through all the combinations.
	 *
	 * by the time physically delete is called, the mother node is not in the binary search tree anymore
	 * it is just taking up space on disk. The offspring of that mother node were never in the tree and as
	 * soon as the mother node is gone there's no more reference to the offspring nodes.
	 * physically deleting nodes however can cause anything to move, so we have to be careful
	 * if we make a list of what to delete because items in that list can get stale after one of them
	 * is deleted. so I think what has to happen is that we DO have to update the mother node as offspring
	 * are deleted so that we can always ask the mother node for currently correct things to delete.
	 * that means
	 * a) we have to physically delete offspring first
	 * b) we have to phyiscally delete offspring from back to front (well we don't HAVE to but it keeps the list
	 * consistent and we have to blank out the offspring list nodes as we delete them
	 * c) we have to keep an eye on the mother node location because it is possible that it can move as a
	 * result of deleting one of its offspring.
	 * I don't think there's a way around c.
	 * if we delete the mother first then we have no way of keeping track of if the other offspring moved as
	 * a result of other offsprings nodes physically deleted earlier.
	 * the only thing we can rely on is the mother node, and I guess the only way to be sure is
	 * to make sure we keep track if the mother node moved as a result of deleting an offspring node.
	 * one way to cheap out would be to say if the mother node is the last thing in the list
	 * then move it manually (swap with the one before it) so that it will not get moved out from under us
	 * unexpectedly, but I'm not sure that's any easier than just keeping track of if it moves because of an
	 * offspring node delete.
	 * then there's the problem of if the mother node is the last one but I guess it can't be since it gets
	 * deleted only after its offspring is deleted.
	 * so in summary, I think we just check if we're deleting a mother node, delete the offspring first
	 * have that update the mother node as it happens, and somehow report if the mother node got moved
	 * as a result. deleting is hard. testing will be hard too. */

	/* 11/9/2020 okay so I drew it out and worked it all out, and it's not that bad.
	   * The short of it is we need the parent function that handles physical deletes of
	   * mother nodes, to generate calls to a function that physically deletes one like we used to.
	   * the trick is to keep track of what got moved where so that if the original list contains
	   * the item moved, its number gets updated so the newly moved position gets deleted.
	                   deleteing m1 and offspring o2 o3 o4...
	                    1  2  3  4  5  6  7        delete list
	                A   m o2 o3 o4 m1 m5 o6          4 3 2 5
	                B   m o2 o3 o6 m1 m5      7->4   x 3 2 5
	                C   m o2 m5 o6 m1         6->3   x x 2 5
	                D   m m1 m5 o6            5->2   x x x 2  (because 5 moved to 2 so we change 5 to 2)
	                E   m o6 m5               4->2   x x x x

	     another example

	                   deleteing m1 and offspring o1 o2 o3...
	                    1  2  3  4  5              delete list
	                A   m o1 o2 o3 m1                4 3 2 5
	                B   m o1 o2 m1            5->4   x 3 2 4  (because 5 moved to 4 so we change 5 to 4)
	                C   m o1 m1               4->3   x x 2 3  (because 4 moved to 3 so we change 4 to 3)
	                D   m m1                  3->2   x x x 2  (because 3 moved to 2 so we change 3 to 2)
	                E   m                     2->2   x x x x
	*/
	/* caller already loaded the node from which we can get the list of offspring to delete */
	/* 11/20/2020 so I dunno WHY the caller loaded the node and passed it to me, but it loaded it
	 * before it was logically deleted, and therefore is stale, we reload the modified mother node here. */

	var ret, noderesp = this.Node_load(pos)
	if ret != nil {
		return ret
	}
	var mothertodelete stree_v_node.Stree_node = *noderesp

	if mothertodelete.Is_offspring() {
		return tools.Error(this.log, "request to delete an offspring node not allowed.")
	}

	// copy the list of offspring so we have what to delete
	var delete_list []uint32 = make([]uint32, this.m_offspring_per_node+1) // need to including deleting of mother node

	var delete_list_pos int = 0
	//        for (int lp = 0; lp < m_offspring_per_node; lp++) // go to end of list, it might not end in zero
	/* 11/20/2020 this was a problem, we used to delete offspring from left to right, but that doesn't work
	 * because physically_delete_one stops fixing the offspring list when it gets to a zero from left to
	 * right, so we must delete in right to left order, just like shrink does so physically delete
	 * will always have a correct offspring list in mother node to work with.
	 * okay so that works, if all offspring are full, or rather it did when we went from left to right.
	 * Now we have to go from right to left, but we have to start either at the end of the offspring list
	 * or don't use zero as the end of the list because we're not going left to right we're going right to
	 * left and if not all offspring are used, the last one will be empty/zero and we will bail right away.
	 * so we romp through the whole offspring list backwards, and only add non-zero things, but don't stop short. */
	var rp int
	for rp = int(this.m_offspring_per_node) - 1; rp >= 0; rp-- { // delete them in reverse just like shrink does

		var ret, resp = mothertodelete.Get_offspring_pos(uint32(rp))
		if ret != nil {
			return ret
		}
		var offspring_value uint32 = *resp
		if offspring_value == 0 {
			continue
		}
		delete_list[delete_list_pos] = offspring_value
		delete_list_pos++
		this.log.Debug("removing node: ", offspring_value)
	}
	/* we must delete the mother node last because in deleting the offspring
	 * nodes it will update the parent with the new offspring list
	 * with the deleted one zeroed out, so the mother must be around to
	 * note all of its offspring going away. */
	delete_list[delete_list_pos] = pos // the mother node to delete
	delete_list_pos++
	this.log.Debug("removing mother node: ", pos)

	/* Now go through the delete list individually deleting each item, updating the list if
	 * something in the list got moved. */
	this.log.Debug("going to delete ", delete_list_pos, " items.")
	for rp := 0; rp < delete_list_pos; rp++ {
		var pos_to_delete uint32 = delete_list[rp]

		var ret, moved_resp_from, moved_resp_to = this.physically_delete_one(pos_to_delete)
		if ret != nil {
			return ret
		}
		/* now update the remainder of the list if anything in it moved. we can do the whole list,
		 * it doesn't hurt to update something that was already processed/deleted. */
		var from uint32 = moved_resp_from
		var to uint32 = moved_resp_to
		this.log.Debug("moved mover from ", from, " to ", to)

		for wp := rp + 1; wp < delete_list_pos; wp++ { // as we deleted lp in this round, we don't need to update it.

			if delete_list[wp] == from {
				delete_list[wp] = to
				this.log.Debug("mover node ", from, " was in the delete list so we moved it to ", to)
			}
		}
		// only for small trees this.print(); // xxxz
	}
	return nil
}

func (this *Stree_v) clean_deleted_offspring_from_mother(toremove_pos uint32) tools.Ret {
	// caller already loaded pos into toremove, this is a double read, but that's what caches are for.

	var ret, resp = this.Node_load(toremove_pos)
	if ret != nil {
		return ret
	}
	var toremove stree_v_node.Stree_node = *resp
	if toremove.Is_offspring() == false {
		return tools.Error(this.log,
			"sanity failure, offspring was told to clean itself out of mother but it is not an offspring node.")
	}

	// get our mother.
	ret, resp = this.Node_load(toremove.Get_parent())
	if ret != nil {
		return ret
	}
	var mother *stree_v_node.Stree_node = resp
	if mother.Is_offspring() != false {
		return tools.Error(this.log, "sanity failure, deleting offspring node who's parent is not a mother node.")
	}
	var found bool = false
	/* go through all the mother's offspring, find ourselves and erase us from the list.
	 * deletes all work from right to left in the offspring list, so this should leave a correct offspring list */
	var rp uint32
	for rp = 0; rp < this.m_offspring_per_node; rp++ {

		var ret, offspring_resp = mother.Get_offspring_pos(rp)
		if ret != nil {
			return ret
		}
		var offspring_peek uint32 = *offspring_resp
		if offspring_peek == 0 { // end of list
			break
		}
		if offspring_peek == toremove_pos {
			found = true
			mother.Set_offspring_pos(rp, 0) // remove it
			break                           // there can (better) be only one, and it better be the last one too.
		}
	}
	if found == false {
		return tools.Error(this.log, "sanity failure, tree is corrupt, while physically deleting ", toremove_pos,
			" we tried find ourselves in our mother's offspring list, but we didn't find ourselves.")
	}
	ret = this.node_store(toremove.Get_parent(), mother)
	if ret != nil {
		return ret
	}
	return nil
}

func (this *Stree_v) physically_delete_one(pos uint32) (tools.Ret /* moved_resp_from */, uint32 /* moved_resp_to*/, uint32) {
	/* 11/9/2020 Okay, now that we've separated it into two parts, it's not that bad.
	 * the first part figures out what to delete and updates the list if things got moved,
	 * and here, we just delete one. deleting the mother node is same as always, we have to update
	 * any parents and children that pointed to the thing that moved into our (the deleted node's) place.
	 * almost forgot, if we're the mother node, we also have to update all of our offspring to say we (pos)
	 * is the new location of mover's parent.
	 * deleting an offspring node involves just updating the mother that points to us.
	 * The other important change is that we have to return the two value of from and to positions
	 * that were moved, so caller can update the list of things to delete appropriately if things
	 * in the list to delete get moved while in here. */

	/* When deleting a mother node we know we're going to delete all the offspring and the mother node
	 * so we don't have to update anything, as all members of this node, mother and offspring will be
	 * overwritten soon enough.
	 * But in the case where we are shrinking a node because we are updating an existing node
	 * with less data that needs fewer offspring, we will be deleting offspring and NOT
	 * deleting the mother node, so we do in fact have to update the mother node if things got
	 * moved.  wait maybe not. no, I'm being dumb, remember, when we delete something it goes away
	 * we only update things related to the mover node, not the node being deleted.
	 * in the case of shrinking a node, we are simply deleting offspring nodes
	 * that nobody will ever refer to again, the zeroing out of the parent happens in the
	 * update/shrinking function and here we deal with moving important nodes into its place
	 * and all the correct updates are performed. all is well.
	 * that's funny, in update/shrink I said I zeroed out the mother node offspring entries here, and here
	 * I say that I did it there. doing it there makes more sense. don't need to do it on every delete
	 * just offspring deletes where we don't delete the parent which only happens in update/shrink. */

	/* 12/23/2020 I think I figured out the bad root node bug. when we move mother nodes around we shuffle
	 * all of the parents and children and stuff, but if the mother node being moved is the root node
	 * we have to update the logically deleted root node header, and I think we missed doing that. */

	if pos == 0 {
		return tools.Error(this.log, "sanity failure, tree is corrupt, physically delete one asked to delete node zero."), 0, 0
	}

	var moved_resp_from uint32
	var moved_resp_to uint32

	this.log.Debug("physically delete one at position: ", pos)
	var iresp uint32
	var ret tools.Ret
	ret, iresp = this.storage.Get_free_position()
	if ret != nil {
		return ret, 0, 0
	}
	var free_position uint32 = iresp
	if (pos == 1) && (free_position == 2) { // deleted the root node
		// removing the last remaining node, nothing to do
		moved_resp_from = pos // nothing is being moved, but we have to return something
		moved_resp_to = pos

		/* 11/20/2020 when you physically delete something, if it was a mother, no big deal, it has already
		 * been logically deleted  but if you're deleting an offspring, you still have to find your mother
		 * and tell her you're gone otherwise when the mother goes to move or get deleted, it is pointing
		 * to a node that is gone or at least isn't hers. This was a quick out for the simple delete case,
		 * but there are no simple delete cases. */
		/* okay I meant this for below, in this case, it actually is simple, if you're deleting the root node
		 * then it must be a mother, just deallocate it. logically delete would have already set the root
		 * node to 0 by the time we get here. */
		return this.storage.Deallocate(), moved_resp_from, moved_resp_to
	}
	if free_position == 2 {
		return tools.Error(this.log,
			"tree is corrupt, free position is 2, but delete request is not to delete position 1, but position: ",
			pos), 0, 0 // something wrong here.
	}

	/* Special case, if we're removing the last item, do nothing but lower
	 * the free position, the array item has already been orphaned and
	 * nothing is pointing to it. For recovery's sake it might be worth zeroing out the newly deleted element
	 * because if we search for the end manually we might pick up a deleted node and then we're screwed forever.
	 * 11/9/2020 we're not going to do recovery that way, we're going to do it with a transaction log, so don't worry
	 * about zeroing out deleted nodes. */
	/* this is true for orphans too, I think, it depends on the order in which we delete a node with offspring.
	   regardless of order all the nodes mother and offspring will go away, so once the node is logically
	   deleted we really just need to remove the nodes and we don't have to update the mother because it will
	   be going away too. so when we physically delete a node we just have to worry about the one we're moving into
	   its place. the node being deleted and its mother don't matter at all. */

	// so that last bit is not entirely true...

	/* but wait! we can't overwrite pos yet, because pos might be an offspring and if we're overwriting it
	 * we have to update it's mother to remove the offspring from the mother's list.
	 * I don't think anybody else takes care of that, logically delete doesn't, it only works on mother
	 * node links. So I believe we have to do that here. */
	/* There are two callers of this, shrink and delete. Shrink does take care of cleaning out the mother's offspring list
	 * but I don't think delete does, which is how we found this problem in the first place, when the simple remove
	 * the offspring off the end case didn't clean itself up in the mother. So same thing, delete doesn't do it
	 * for the complex case either, which means we have to remove the cleaning from shrink since it's done here
	 * because it will fail if we clean it and shrink tries to because shrink won't find it. Actually
	 * shrink knew what it was doing and just blindly cleared out the offspring list, but since we're
	 * taking care of it here, I removed it from shrink. save us a read. */

	/* HERE is where we can't simply deallocate, we have to update the mother if we're an offspring. */
	// first see if it's an offspring or not, this will cause a double load, but that's what caches are for.
	/* So it turns out we need to do this in all cases, whether the node being removed is at the end or
	 * not so just do it up front here. Go find the about-to-be-deleted-node's mother and update it. */

	var resp *stree_v_node.Stree_node
	ret, resp = this.Node_load(pos)
	if ret != nil {
		return ret, 0, 0
	}
	var toremove stree_v_node.Stree_node = *resp
	if toremove.Is_offspring() {
		/* we haven't overwritten anything yet so we can still load and find its mother.
		 * surprisingly, as similar as all these fixups are, none are identical, sharing
		 * code is risky because it's confusing enough to follow what's going on as it is,
		 * without introducing a bunch of "if this mode, do it slightly differently" so
		 * for now, we handle this case right here. well, since this isn't rust we can
		 * do it in a function. */
		var ret = this.clean_deleted_offspring_from_mother(pos)
		if ret != nil {
			return ret, 0, 0
		}
	}

	if pos == free_position-1 {
		this.log.Debug("deleted item is in last position, just deallocating.")
		moved_resp_from = pos // nothing is being moved, but we have to return something
		moved_resp_to = pos
		return this.storage.Deallocate(), moved_resp_from, moved_resp_to
	}

	// nothing in mover has to change, just the people pointing to mover have to change
	var mover_pos uint32 = free_position - 1

	// now we know what we're moving where, make sure we return it to caller.
	moved_resp_from = mover_pos
	moved_resp_to = pos

	if this.debugprint {
		this.diag_dump_one(mover_pos)
	}

	ret, resp = this.Node_load(mover_pos)
	if ret != nil {
		return ret, 0, 0
	}
	var mover *stree_v_node.Stree_node = resp

	/* for both mother and offspring, copy the data into the correct array location.
	 * actually, that worked fine when the node was simpler but now I think it just
	 * makes more sense to read the mover node and write it into the movee's location (pos),
	 * that way we don't have to worry about missing anything regardless of type (mother or offspring) */

	/* this is the first important bit, we write m's data over the old n's location (pos) */
	/* okay now 11/16/2020 we're doing this at the end because the act of writing mother over it's new
	 * location can possibly overwrite the offspring it's deleting which makes updating
	 * the mother's offspring's parents difficult, so since we have to update the mother's
	 * offspring list (setting the offspring to zero if its in there) we will save writing
	 * for last. */

	/* another explanation of the sinister problem.
	 * If we just copied the mother node over one of the mother's offspring
	 * that we are deleting, the attempt to update the offspring's parent will corrupt the
	 * tree because the mother will think it's updating an offspring node, when in fact
	 * it will be updating itself. */

	// now update all the people pointing to mover to point to pos instead.
	/* This is where mother/offspring type matters */

	if mover.Is_offspring() {
		/* mover node is offspring, go get our parent, find mover in the offspring
		 * list and change it to pos */

		/* since we had to move the actual mover node store/write to the end of if-mother, we have to remember to do it
		 * for if-offspring nodes too, it doesn't affect anything about the mover node, since it's an offspring
		 * only things pointing to it matter, so just write it out now. */

		var ret = this.node_store(pos, mover)
		if ret != nil {
			return ret, 0, 0
		}

		ret, resp = this.Node_load(mover.Get_parent())
		if ret != nil {
			return ret, 0, 0
		}
		var mother *stree_v_node.Stree_node = resp
		if mother.Is_offspring() != false {
			return tools.Error(this.log, "sanity failure, deleting offspring node who's parent is not a mother node."), 0, 0
		}

		var found bool = false
		var rp uint32
		for rp = 0; rp < this.m_offspring_per_node; rp++ {

			var ret, offspring_resp = mother.Get_offspring_pos(rp)
			if ret != nil {
				return ret, 0, 0
			}

			var offspring_peek uint32 = *offspring_resp
			if offspring_peek == 0 { // end of list
				break
			}
			if offspring_peek == mover_pos {
				found = true
				mother.Set_offspring_pos(rp, pos)
				break // there can (better) be only one
			}
		}
		if found == false {
			return tools.Error(this.log,
				"sanity failure, tree is corrupt, while physically deleting ", pos, " we tried to move ",
				mover_pos, " to it, mover ", mover_pos, " was an offspring node, but its parent ",
				mover.Get_parent(), " does not point to mover"), 0, 0
		}

		ret = this.node_store(mover.Get_parent(), mother)
		if ret != nil {
			return ret, 0, 0
		}
	} else { // mover node is a mother node, update all the tree pointers to point from mover to pos */

		// first update mover's parent, either root or parent's children should point to pos
		/* 11/20/2020 will problems never wane...
		 * so in this case:
		 * -- (wx) bb (ZYXW) 10 (stuv)
		 *      bb
		 *     /
		 *    10
		 * if we're deleting 10, first we logically delete it, then we, here, physically
		 * delete node 1 (10's offspring) and we do that by moving 10 (node 3) to node 1.
		 * then because 10 is a mother node, we try and update its parent's children pointers
		 * and its children's parent pointers.
		 * But if we were just bb's left child which we were, remember we first logically
		 * deleted it to remove it from the tree
		 * so we are no longer bb's child and that's okay. but our parent still says bb.
		 * we can't trigger an error here if we can't
		 * find ourselves as one of our parent's kids, because of this case.
		 * basically if we are an orphaned mother node, it is okay to not update any pointers
		 * we logically do not exist, do not have childen or a parent, nothing to update. */
		var parent uint32 = mover.Get_parent()
		if parent == math.MaxUint32 {
			// if parent is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan")
		} else {
			// check for pos being the root node
			if mover.Get_parent() == 0 {
				/* this is our 12/23/2020 bug. We can't rely on the mother node's parent being zero to tell
				 * us if it is the root node. It may be the one being deleted, and therefore is already
				 * logically deleted, so we should not update the root node in that case. We took care of
				 * that problem for all other mother's being moved by setting the sentinal value so we
				 * know not to update pointers of logically deleted nodes. But we missed this one case, where
				 * the one being deleted is also the root node. */

				var ret, rootnoderesp = this.storage.Get_root_node()
				if ret != nil {
					return ret, 0, 0
				}
				/* so if mover thinks it is the root node, AND the root node thinks mover is the
				 * root node, only then do we update it. If this mother node which WAS the root node
				 * got logically deleted and is now being physically deleted, we don't update the root node. */
				var root_node_pos uint32 = rootnoderesp
				if root_node_pos == mover_pos {
					ret = this.storage.Set_root_node(pos)
					if ret != nil {
						return ret, 0, 0
					}
				}
			} else {
				ret, resp = this.Node_load(mover.Get_parent())
				if ret != nil {
					return ret, 0, 0
				}
				var p *stree_v_node.Stree_node = resp
				if p.Get_left_child() == mover_pos {
					p.Set_left_child(pos)
				} else {
					if p.Get_right_child() == mover_pos {
						p.Set_right_child(pos)
					} else {
						if this.debugprint {
							this.diag_dump_one(mover.Get_parent())
						}
						return tools.Error(this.log,
							"sanity failure, tree is corrupt, moving mother node ", mover_pos,
							" but neither of mother's parent's ", mover.Get_parent(), " children ",
							p.Get_left_child(), " and ", p.Get_right_child(), " point to mover ", mover_pos), 0, 0
					}
				}
				var ret = this.node_store(mover.Get_parent(), p)
				if ret != nil {
					return ret, 0, 0
				}
			}
		} // if moving mother node is live in the tree and should have its pointers updated.

		// second update mover's children, if any, set their parent to pos
		/* 11/20/2020 this has the same problem as above if the node that's being moved is the mother
		 * node that is being deleted as part of this physically moving delete of an offspring
		 * we can't try and set our children's mother because our children are not our children anymore.
		 * so I think when we logically delete a mother node, we should set its parent and children
		 * to sentinal values so that when we get here, in case of move, we don't try and update them.
		 * this way we can still do validity checks on the ones that should be moving that aren't deleted. */
		var left_child uint32 = mover.Get_left_child()
		if left_child == math.MaxUint32 { // if left_child is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan") // we will see this message 3 times.
		} else {
			if left_child != 0 {
				ret, resp = this.Node_load(mover.Get_left_child())
				if ret != nil {
					return ret, 0, 0
				}
				var lc *stree_v_node.Stree_node = resp
				lc.Set_parent(pos)
				var ret = this.node_store(mover.Get_left_child(), lc)
				if ret != nil {
					return ret, 0, 0
				}
			}
		}
		var right_child uint32 = mover.Get_right_child()
		if right_child == math.MaxUint32 { // if right_child is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan") // we will see this message 3 times.
		} else {
			if mover.Get_right_child() != 0 {
				var ret, resp = this.Node_load(mover.Get_right_child())
				if ret != nil {
					return ret, 0, 0
				}
				var rc *stree_v_node.Stree_node = resp
				rc.Set_parent(pos)
				ret = this.node_store(mover.Get_right_child(), rc)
				if ret != nil {
					return ret, 0, 0
				}
			}
		}
		/* lastly, now we have to go to all of our offspring and tell them their parent is now pos */
		/* 11/20/2020 in the case of the orphaned mother node, this is still true, we are only logically
		 * orphaned from the tree, our offspring are still our problem and must be kept up to date until
		 * they are deleted. Remember in this case, we are a logically orphaned mother node that got moved BECAUSE
		 * our offspring got physically deleted. */
		var rp uint32
		for rp = 0; rp < this.m_offspring_per_node; rp++ {

			var offspring_resp *uint32
			ret, offspring_resp = mover.Get_offspring_pos(rp)
			if ret != nil {
				return ret, 0, 0
			}
			var offspring_node uint32 = *offspring_resp
			if offspring_node == 0 { // end of list
				break
			}
			/* if we happen come across the offspring being deleted, zero it out
			 * and especially do not try and update its parent. This is that sinister
			 * problem I was talking about where a mother is moving over its own offspring
			 * that's being deleted. */
			if offspring_node == pos {
				mover.Set_offspring_pos(rp, 0)
				continue
			}

			var ret, resp = this.Node_load(offspring_node)
			if ret != nil {
				return ret, 0, 0
			}
			var o *stree_v_node.Stree_node = resp
			o.Set_parent(pos)
			ret = this.node_store(offspring_node, o)
			if ret != nil {
				return ret, 0, 0
			}
		} // for

		/* so now that we've updating all the mother's offspring and set the offspring array if we are overwriting
		 * one of our own offspring, we can finally rewrite the mother node to disk in it's correct final place. */
		var ret = this.node_store(pos, mover)
		if ret != nil {
			return ret, 0, 0
		}

	} // if mover is a mother node

	// remove old mover position from the allocated array list.
	return this.storage.Deallocate(), moved_resp_from, moved_resp_to
}

func (this *Stree_v) Delete(key string, not_found_is_error bool) tools.Ret {
	// if you want to print the pre-delete tree.
	//        ArrayList<Integer> iresp = new ArrayList<Integer>();
	//        String Ret = this.storage.get_root_node(iresp);
	//          if ret != nil {
	//          return Ret;
	//        int root_node = iresp.get(0);
	//        treeprinter_iii.printNode(this, root_node);

	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()

	var ret, respfound, _, respnodepos = this.search(key, false)
	if ret != nil {
		return ret
	}

	var found bool = respfound
	var pos uint32 = respnodepos
	if found == false {
		if not_found_is_error {
			return tools.Error(this.log, syscall.ENOENT, "no node found to delete for key: ", key) // can't delete what we can't find.
		}
		// this.log.Debug("no node found to delete for key: ", key) // this is rather noisy for discard
		return nil
	}
	/* two steps, first we have to logically delete the node, then
	 * we have to physically move something into its place in storage. */
	/* regardless of what happens during logical delete, it's only pointers
	 * that move around, when it comes to step 2 to physically delete,
	 * we're always going to be deleting the pos array entry, even
	 * if it's the root node and it's the only one left. */
	ret = this.logically_delete(pos)
	if ret != nil {
		return ret
	}
	ret = this.physically_delete(pos)
	if ret != nil {
		return ret
	}

	// if you want to print the post-delete tree.
	//        Ret = this.storage.get_root_node(iresp);
	//          if ret != nil {
	//          return Ret;
	//        root_node = iresp.get(0);
	//        treeprinter_iii.printNode(this, root_node);
	return nil
}

func (this *Stree_v) Get_free_position() uint32 { // package scope
	// only used for treeprinter

	var ret, iresp = this.storage.Get_free_position()
	if ret != nil {
		fmt.Println(ret)
		return 0
	}
	return iresp
}

/* in java there is a package scope, but I don't think go has that, so it's public. */
func (this *Stree_v) Get_root_node() uint32 {
	// only used for treeprinter, package scope

	var ret, iresp = this.storage.Get_root_node()
	if ret != nil {
		fmt.Println(ret)
		return 0
	}
	return iresp
}

func (s *Stree_v) Load(pos uint32) *stree_v_node.Stree_node { // only used for testing, package scope

	var ret, resp = s.Node_load(pos)
	if ret != nil {
		fmt.Println(ret)
		return nil
	}
	return resp
}

func Calculate_block_size(log *tools.Nixomosetools_logger, key_type string, value_type []byte,
	max_key_length uint32, max_value_length uint32, additional_offspring_nodes uint32) (ret tools.Ret, resp uint32) {
	/* for startup, the caller doesn't know how big we're going to make a block, so it can use this to ask us.
	 * They don't know what offspring nodes are and how we can store as much data as node_size * offspring_nodes + 1
	 * so they just pass us the number of nodes total they want to store, and we subtract accordingly.
	 so for the go version, we're not doing the +1/-1 thing. */
	var n *stree_v_node.Stree_node = stree_v_node.New_Stree_node(log, key_type, value_type, max_key_length, max_value_length,
		additional_offspring_nodes)
	// we pass the max field size because that's what we determine block size with.
	var serialized_size uint32 = n.Serialized_size(max_key_length, max_value_length)

	return nil, uint32(serialized_size)
}

func (this *Stree_v) Get_used_blocks() (tools.Ret, uint32) {
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	return this.storage.Get_free_position()
}

func (this *Stree_v) Get_total_blocks() (tools.Ret, uint32) {
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	return this.storage.Get_total_blocks()
}

// func (s *Stree_v) get_max_key_length() uint32 {
// 	return s.m_max_key_length
// }

// func (s *Stree_v) get_max_value_length() uint32 {

// 	return s.m_max_value_length
// }

func (this *Stree_v) Diag_dump(printtree bool) {
	/* load all active nodes and print out their contents */

	var root_node = this.Get_root_node()
	fmt.Println("root node: ", root_node)
	var ret, iresp = this.storage.Get_free_position()
	if ret != nil {
		fmt.Println(ret)
		return
	}
	var free_position uint32 = iresp

	var lp uint32
	for lp = 1; lp < free_position; lp++ {
		fmt.Println("---------------------")
		this.diag_dump_one(lp)
	}

	if printtree {
		var tp Treeprinter_iii
		tp.PrintNode(this, this.Get_root_node())
	}

}

func (this *Stree_v) diag_dump_one(lp uint32) {

	var ret, nresp = this.Node_load(lp)
	if ret != nil {
		fmt.Println(ret)
		return
	}

	var n stree_v_node.Stree_node = *nresp
	fmt.Println("node pos:       ", lp)
	fmt.Println("parent:         ", n.Get_parent())
	fmt.Println("left child:     ", n.Get_left_child())
	fmt.Println("right child:    ", n.Get_right_child())
	fmt.Println("key:            ", n.Get_key())
	/* print beginning and end of data */
	if len(n.Get_value()) < 8 {
		fmt.Println("value:        ", tools.Dump(n.Get_value()))
	} else {
		var bout []byte // = make([]byte, 1)
		bout = append(bout, n.Get_value()[0:8]...)
		bout = append(bout, n.Get_value()[(len(n.Get_value())-8):]...)
		fmt.Println("value:        ", tools.Dump(bout))
	}

	fmt.Print("offspring:      ")
	if n.Is_offspring() {
		fmt.Println("none")
	} else {
		var rp uint32
		for rp = 0; rp < this.m_offspring_per_node; rp++ {

			var ret, offspring_resp = n.Get_offspring_pos(rp)
			if ret != nil {
				fmt.Println(ret)
				return
			}
			fmt.Print("", *offspring_resp, " ")
		}
		fmt.Println()
	}
}

func (this *Stree_v) Wipe() tools.Ret {
	return this.storage.Wipe()
}

func (this *Stree_v) Dispose() tools.Ret {
	this.Shutdown()
	return this.storage.Dispose()
}
