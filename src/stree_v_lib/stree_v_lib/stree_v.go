/* this is the go library port of stree_iv.
   actually we should probablhy call it stree_v because it will implement the transaction log somehow. */

// package name must match directory name
package stree_v_lib

func setup(log Stree_logger) bool {
	log.debug("stree_v init.")
	return true
}
