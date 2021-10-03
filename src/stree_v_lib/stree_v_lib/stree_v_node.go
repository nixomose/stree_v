package stree_v_lib;

import "bytes"


type Stree_node struct {
	 log Logger
     parent int // position in storage array of parent, 0 = root
     left_child int  // position in storage array of the left child node
     right_child int // position in storage array of the right child node
     key keytype
     value valuetype

    /* These are only kept around for ease of validation, these values are not serialized to disk.
     * serializing a node means getting the actual size of the value and writing that so we can
     * deserialize correctly. which means these values get set once at creation and deserializing
     * does not overwrite them so they better be correct. */
 	 max_key_length int
     max_value_length int

    /* This requires a lot of explanation.
     * offspring is an array of size m_offspring_per_node defined in stree.
     * it represents a list of other nodes that store data (in order)
     * the value in the array will be filled from 0 up to max, and if the value
     * is zero then that's the end of the list, if the last element is not zero
     * then it is a maxed out block, that means zero does not denote the end of the list
     * unless the block being stored doesn't use all the offspring nodes.
     * This scheme allows us to store a 64k block that compresses down to 4 in one block
     * instead of 16, so we can save lots of space.
     * this also gives us a lot of variability in the range of size blocks we can efficiently store.
     * When we get to stree_v it will get even more flexible.
     * But for now, since the goal is to be able to store 4k efficiently, that means that the
     * block size will be 4k, so one 4k block fits in 4k+metadata and the max size 'block' you can store
     * will be 4k * the number of offspring nodes you set, 16 is probably a pretty good number,
     * making the max blocksize 64k.
     * The first element in the offspring node is NOT the mother node, it is the first offspring node,
     * so if the array is all zeroes, then only the mother node holds data.
     *
     * this also means that value length is only the value length of THIS node,
     * as a mother or offspring, it only refers to this node, so the mother and
     * all but the last child will be the block size, and only the last offspring
     * node will be less than block_size in length. of course if the stored amount
     * is less than one block size, then yea the mother node will also be short.
     *
     * So the big question is how do we define the block size. Is it the size of data you
     * can fit in one node, or the max size of data you can store given the defined
     * size of the offspring array? Which one do you specify, the max size and the number
     * of pieces you want? or the smallest size and the multiple and we calculate the max.
     * Both have one thing that works and one that makes no sense.
     * I need to store 4k, so I say okay, the 4k is the node size and I want 15 of them
     * but then I have to ask the stree to find out what the actual max block size is.
     * And that's kinda weird.
     * Or I can give it the max block size, and number of divisions, and I just have to make sure
     * it magically comes out to the node size being 4k, and then I can never change it to make it
     * bigger. Actually I can't change it anyway, because that would change the on disk layout
     * so there's no offspring array size changing anyway.
     * I think I'm leaning towards the 4k and the offspring array size, it makes it easier to
     * work everything else out rather than dividing and possibly screwing it up.
     * So caller will supply a node block size (the smallest thing you will store)
     * and the number of offspring you want, and we will tell you the max size block you
     * can store. So be it. */
     offspring_nodes int // how many elements in the offspring array, always fully allocated to max for block size
	 offspring []int

}

type Stree_node_interface interface
{
get_key() string
get_value() string
init()
}

func (n Stree_node) init(l Logger, Key string, Val string,  Max_key_length int, Max_value_length int, Offspring_nodes int) string{
	n.log = l
	n.key = Key
	n.value = Val
	n.parent = 0
	n.left_child = 0
	n.right_child = 0
	n.max_key_length = Max_key_length
	n.max_value_length = Max_value_length
	// if the array is null then you are an offspring node, otherwise you are a mother node.
	/* we don't serialize this or store it, but we need it to know how big to make the offspring array
	 * when we deserialize a node from disk, the array size has to be the max allowable for this block size.
	 * This is the offspring array size, which does not include the space we can also use in the mother node. */
	 n.offspring_nodes = Offspring_nodes
	if Offspring_nodes > 0 {
	n.offspring = make([]string, Offspring_nodes)  // array is fully allocated but set all to zero
	}	else{
	n.offspring = make([]string, 0)  // nil
}

}


func (n Stree_node) get_key() string      {
        return n.key;
      }

    func (n Stree_node)  get_value() string{
        return n.value;
      }

   func (n Stree_node) set_key( new_key string)      {
	n.key = new_key;
      }

    func (n Stree_node) set_value( new_value string) int      {
        // make sure the value we're setting doesn't exceed the limits we said we can store
        if (new_value == null) {
          return error(log, EINVAL, "trying to set a value that is null");
		}
        if (new_value.get_length() > n.max_value_length){
          return error (log, EINVAL, "trying to set value length of " + new_value.get_length() + " but only have space for  " +             max_value_length)
        n.value = new_value
        return 0
      }
	}

    func (n Stree_node)  toString() string{
         ret := "" + key;
        return ret;
      }

   func  (n Stree_node) compareTo(a Stree_node)  int      {
        return n.get_key().compareTo(a.get_key());
      }

    func  (n Stree_node) get_parent()  int      {
        return n.parent;
      }

     func  (n Stree_node) get_left_child() int      {
        return n.left_child;
      }

     func  (n Stree_node) get_right_child()  int      {
        return n.right_child;
      }

    // do we need this?
//    public int[] get_offspring()
//      {
//        return offspring;
//      }

    func  (n Stree_node) set_parent(spos int)      {
        n.parent = spos;
      }

    func (n Stree_node) set_left_child( spos int )      {
        n.left_child = spos;
      }

    func (n Stree_node) set_right_child(spos int )      {
        right_child = spos;
      }

    // I doubt we'll ever need this, we'll only modify the existing array, which is always full size.
//    public void set_offspring(int o[])
//      {
//        offspring = o;
//      }

     func (n Stree_node) set_offspring_pos( offspring_pos int,  node_pos int) int      {
        if (n.offspring_pos < 0 || n.offspring_pos > n.offspring_nodes){
          log.error( "trying to set offspring pos " + n.offspring_pos +
              " which is greater than the number of offpsring nodes " + n.offspring_nodes);
              return EINVAL
            }
        if (n.offspring_pos > n.offspring.length) {
          log.error  ("trying to set offspring pos " + n.offspring_pos +
              " which is greater than the offpsring array length " + n.offspring.length);
              return EINVAL
        }
                      n.offspring[offspring_pos] = node_pos;
        return 0
      }

    func (n Stree_node) is_offspring() boolean      {
        /* 6/22/2021 the node's offspring variable is non null of we're the mother node because the mother has a list
         * of offspring, and the offspring do not. However, we missed a case where if the whole stree is not set up
         * to use offspring, then offspring is null in the mother node as well. But the node has no concept of
         * the state of the stree it is part of, so all callers of this function have to check for themselves.
         * okay, that's not true, apparently we store the number of offspring nodes in the stree. how handy. */
        if (n.offspring_nodes == 0){
          return false}
        if (n.offspring == null){
          return true}
        return false
      }

    func (n Stree_node) get_offspring_pos( offspring_pos int  ) (int, bytes.Buffer)     {
        if (offspring_pos < 0 || offspring_pos > offspring_nodes) {
          return Ret.r(log, "trying to get offspring pos " + offspring_pos +
              " which is greater than the number of offpsring nodes " + offspring_nodes);
              return EINVAL, nil
      }
        if (offspring_pos > offspring.length) {
          return Ret.r(log, "trying to get offspring pos " + offspring_pos +
              " which is greater than the offpsring array length " + offspring.length); 
              return EINVAL, nil
            }
        resp.clear()
        resp.add(n.offspring[offspring_pos])
        return 0, resp
      }

    func (n Stree_node) serialized_size(int key_length, int value_length) int {
        /* return the size of this node in bytes when it is serialized in the serialize function below
         * ie, add up all the sizes of the fields we're going to serialize. this includes the variable size of the
         * key and value. which is why they are passed in. */
        retval := 4 + // parent
            4 + // left child
            4 + // right child
            4 + // key length (we need to serialize this too)
            4 + // value length (so we can restore exactly what was passed to us even if it is less than the block size
            key_length + //  space needed for key
            value_length + // space needed for value
            4 ; // space needed to store the number of items in the offspring array (or 0 or max_int)
            
            if (offspring != null) {
               retval += (4 * offspring.length)
            }
             // space needed for offspring array, if any
             return retval
      }
      
    func (n Stree_node) serialize( data *bytes.Buffer) int      {
        /* serialize this node into a byte array, of exactly block_size size */
         var bkey bytes.Buffer
         bkey = n.key.to_bytes()
        var bval bytes.Buffer
         bval = n.value.to_bytes();
        var ssize int
         ssize = serialized_size(bkey.length, bval.length);
        var bb bytes.Buffer
        bb= ByteBuffer.allocate(ssize);
        bb.putInt(n.parent);
        bb.putInt(n.left_child);
        bb.putInt(n.right_child);
        bb.putInt(n.bkey.length);
        /* as of 11/22/2020 we're going to try making the value only store the actual size of the value
         * data so all value lengths will be the amount stored, the idea being
         * a) we can reproduce the size the original calling writer sent to us,
         * b) this only applies to the last node in the block, all others should be full.
         * c) since string is used for key and value this holds for keys too which means
         *    searching on keys might work differently as they're not padded out for you. */
        bb.putInt(n.bval.length);
        /* we have to distinguish between a zero length array and no offspring array at all (a mother node
         * versus an offspring node */
        if offspring != null          {
            bb.putInt(offspring.length);
            for rp := 0; rp < offspring.length; rp++ {
              bb.putInt(n.offspring[rp]); 
            }
          }         else {
          bb.putInt(Integer.MAX_VALUE); // flag for offspring node.
        }
        if (bkey.length > max_key_length) {
          log.err( "key length is bigger than max key length: " + max_key_length + " it is " + bkey.length)
return EINVAL
        }
        if (bval.length > max_value_length) {
          log.err(              "value length is bigger than max value length: " + max_value_length + " it is " + bval.length);
              return EINVAL
            }
        bb.put(ByteBuffer.wrap(bkey));
        bb.put(ByteBuffer.wrap(bval));
        data.clear();
        data.add(bb.array());
        return 0;
      }

    func (n Stree_node) deserialize(byte data[])int {
        /* deserialize incoming data into this node. */
        var bb ByteBuffer
         bb = ByteBuffer.wrap(data);
        parent = bb.getInt();
        left_child = bb.getInt();
        right_child = bb.getInt();
        var  field_key_length int
         field_key_length = bb.getInt();
        var field_value_length int
         field_value_length = bb.getInt();
        var offspring_length int
         offspring_length = bb.getInt(); // this is how many we serialized, not the max number of offspring we can have.
        if (offspring_length == Integer.MAX_VALUE) { // flag for offspring node.
          offspring = null;}
        else          {
            n.offspring = new int[n.offspring_nodes]; // allocate the max numbere allowed, init all to zero.
            for (int rp = 0; rp < n.offspring_length; rp++){
              n.offspring[rp] = bb.getInt();}
          }
        /* we serialized the actual length of these fields so that's all we read in.
         * this has nothing to do with the max_key/value_length, those are not restored on deserialize they are
         * set once at construction and never change. */
       var bkey[] byte
        bkey = new byte[field_key_length];
        bb.get(bkey);
        key.from_bytes(bkey);
var bvalue        byte
 bvalue[] = new byte[field_value_length];
        bb.get(bvalue);
        n.value.from_bytes(bvalue);
        return 0;
      }

 func (n Stree_node) count_offspring() int {
        if (n.offspring == null)
          return 0;
        int rp;
        for (rp = 0; rp < offspring.length; rp++)
          if (n.offspring[rp] == 0)
            return rp;
        return n.offspring.length; // if they're all full, then there are as many as there are array elements
      }

  }
