// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package stree_v_lib

import (
	"bytes"
	"crypto"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	. "github.com/nixomose/nixomosegotools/tools"
	stree_v_lib "github.com/nixomose/stree_v/stree_v_lib/stree_v_interfaces"

	"github.com/nixomose/nixomosegotools/tools"

	"golang.org/x/sys/unix"
)

const ZENDEMIC_OBJECT_STORE_STREE_V_MAGIC_5k uint64 = 0x5a454e5354354b41 // ZENST5KA  zendemic stree4kaligned
const USABLE_SPACE_PERCENTAGE = 80
const CHECK_START_BLANK_BYTES int = 4096
const STREE_FILEMODE = 0755
const S_ISBLK uint32 = 060000 // stole from cpio
const S_IFMT uint32 = 00170000

type File_store_aligned struct {
	log *Nixomosetools_logger

	m_store_filename                     string
	m_datastore                          *os.File // the file handle to backing file/block device
	m_initial_block_size                 uint32   // the number of bytes we need to store a block of data passed in from parent storage layer
	m_initial_store_size_in_bytes        uint64   // the number of bytes in store passed in from parent storage layer
	m_initial_nodes_per_block            uint32   // the number of additional nodes that make up a single stree entry
	m_initial_file_store_block_alignment uint32   // this is file position alignment over and above any directio alignment that might exist.
	m_header                             File_store_header

	// this is not stored, this is the injected io path object to use
	m_iopath   File_store_io_path
	m_sync     bool // similar to directio, but more about how we open the file.
	m_readonly bool // did we open readonly
}

type File_store_header struct {
	// must be capitalized or we can deserialize because it's not exported...
	M_magic               uint64
	M_store_size_in_bytes uint64 // the total size of file we have to work with.
	M_nodes_per_block     uint32 // how many nodes per block, ie offspring nodes in mother node offspring array, plus 1. with this we can make an external reader
	M_block_size          uint32 // the number of bytes we need to store a block of data, not the size of just the data, stored in header on disk for checking on load
	M_block_count         uint32 // how many blocks we calculated this file/block device can store
	M_root_node           uint32
	M_free_position       uint32 // location of first free block (starts life at 1, because zero is our header storage area)
	M_alignment           uint32 // size of block alignment, 0 = not aligned.
	M_dirty               uint32 // was this filestore shutdown cleanly.

	// stree header format
	/*        magic                    store size in bytes
	00000000  5a 45 4e 53 54 35 4b 41 | 00 00 01 3d 4a 15 99 99  |ZENST5KA...=J...|
	          nodes/block|block size  | block count|root node
	00000010  00 00 01 01 00 00 14 38 | 0f b1 5d 42 00 00 00 01  |.......8..]B....|
	          free pos    alignment   | dirty
	00000020  00 00 00 01 00 00 00 00 | 00 00 00 00 00 00 00 00  |................|
	*/
}

func New_file_store_header_copy(original *File_store_header) File_store_header {
	return File_store_header{
		M_magic:               original.M_magic,
		M_store_size_in_bytes: original.M_store_size_in_bytes,
		M_nodes_per_block:     original.M_nodes_per_block,
		M_block_size:          original.M_block_size,
		M_block_count:         original.M_block_count,
		M_root_node:           original.M_root_node,
		M_free_position:       original.M_free_position,
		M_alignment:           original.M_alignment,
		M_dirty:               original.M_dirty,
	}
}

func (this *File_store_header) Serialized_size() uint32 {
	return 8 + // magic
		8 + // m_store_size_in_bytes
		4 + // m_nodes_per_block
		4 + // m_block_size
		4 + // m_block_count
		4 + // m_root_node
		4 + // m_free_position
		4 + // m_alignment
		4 // m_dirty
}

func (this *File_store_header) serialize(log *Nixomosetools_logger) (Ret, *[]byte) {
	/* serialize this header into a byte array */
	// the binary serializer eats buffers, so once again, we must copy...
	var workarea = New_file_store_header_copy(this)
	structbuf := &bytes.Buffer{}
	err := binary.Write(structbuf, binary.BigEndian, workarea)
	if err != nil { // this sick
		return Error(log, "unable to serialize stree header: ", err), nil
	}
	var b []byte = structbuf.Bytes()
	return nil, &b
}

func (this *File_store_header) Deserialize(log *Nixomosetools_logger, data *[]byte) Ret {
	/* deserialize data into header fields, we steal the provided data array */

	var databuffer *bytes.Buffer = bytes.NewBuffer(*data)
	var err = binary.Read(databuffer, binary.BigEndian, this)
	if err != nil {
		return Error(log, "unable to deserialize stree header: ", err.Error())
	}

	return nil
}

// verify that File_store_aligned implements backing_store
var _ stree_v_lib.Stree_v_backing_store_interface = &File_store_aligned{}
var _ stree_v_lib.Stree_v_backing_store_interface = (*File_store_aligned)(nil)

func New_File_store_aligned(l *Nixomosetools_logger, store_filename string, block_size uint32,
	alignment uint32, nodes_per_block uint32, iopath File_store_io_path) *File_store_aligned {
	var f File_store_aligned
	f.log = l

	f.m_datastore = nil
	f.m_store_filename = store_filename
	f.m_initial_block_size = block_size
	f.m_initial_store_size_in_bytes = 0 // set in startup
	f.m_initial_nodes_per_block = nodes_per_block
	// m_header is set in init()
	f.m_initial_file_store_block_alignment = alignment
	f.m_iopath = iopath

	// init is called to load up the f.m_header if we're making a new one, otherwise it would be read from disk
	return &f
}

func (this *File_store_aligned) calc_block_count() uint32 {
	/* figure out how many blocks can fit in the available storage size taking
	   the user supplied alignment requirement into account. */

	// get the size of an aligned block by asking for the position of block 1
	var adjusted_block_size = this.calc_offset(1)                            // sounds like a tax term
	var max_count = this.m_initial_store_size_in_bytes / adjusted_block_size // rounded down to fit
	return uint32(max_count)
}

func (this *File_store_aligned) Get_store_information() (Ret, string) {
	var m map[string]string = make(map[string]string)

	if this.m_header.M_store_size_in_bytes == 0 {
		return Error(this.log, "invalid file store parameters, store size is zero."), "{}"
	}

	m["backing_storage"] = this.m_store_filename
	m["inital_store_size_in_bytes"] = tools.Prettylargenumber_uint64(this.m_initial_store_size_in_bytes)
	m["inital_nodes_per_block"] = tools.Prettylargenumber_uint64(uint64(this.m_initial_nodes_per_block))
	m["inital_block_size_in_bytes"] = tools.Prettylargenumber_uint64(uint64(this.m_initial_block_size))
	m["number_of_blocks_available_in_backing_store"] = tools.Prettylargenumber_uint64(uint64(this.m_header.M_block_count))

	/* this actually is not correct or useful, because this is the stored block size with the
	   file store aligned per-block header. which is not the amount of data you can actually store
		 in a block, thus multiplying it by the nodes per block is misleading. as we mention elsewhere
		 additional_nodes is not a filestore thing, it's an stree thing and really shouldn't be here
		 but it's good for verifying this is the backing file we think it should be, but it totally
		 doesn't belong here, because it refers to the value size, not the file store block size.
		 oh well. */
	var max_node_size = this.m_header.M_block_size * (this.m_header.M_nodes_per_block + 1) // +1 is for mother node
	m["node_size_in_bytes"] = tools.Prettylargenumber_uint64(uint64(max_node_size))

	m["physical_store_block_alignment"] = tools.Prettylargenumber_uint64(uint64(this.m_header.M_alignment))
	m["dirty"] = tools.Prettylargenumber_uint64(uint64(this.m_header.M_dirty))
	//_this is just informational, we don't need to know this, it gets calculated when we read/write offsets
	m["number_of_physical_bytes_used_for_a_block"] = tools.Prettylargenumber_uint64(this.calc_offset(1))
	/*_While_we're_here_we_can work out how much space is being wasted by alignment. why not. */
	var waste_per_block = this.calc_offset(1) - uint64(this.m_header.M_block_size)
	m["wasted_bytes_per_block"] = tools.Prettylargenumber_uint64(waste_per_block)
	var total_waste = waste_per_block * uint64(this.m_header.M_block_count)
	m["total_bytes_wasted_due_to_alignment_padding"] = tools.Prettylargenumber_uint64(total_waste)
	var total_waste_percent = total_waste * 100 / this.m_header.M_store_size_in_bytes
	m["total_waste_percent"] = tools.Prettylargenumber_uint64(total_waste_percent)

	bytesout, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return Error(this.log, "unable to marshal backing store information into json"), "{}"
	}
	return nil, string(bytesout)
}

func (this *File_store_aligned) Init() Ret {

	this.log.Info(this.m_store_filename, " will be formatted with an stree header.")

	var ret = this.calculate_usable_storage()
	if ret != nil {
		return ret
	}

	if this.m_initial_store_size_in_bytes == 0 {
		return Error(this.log, "invalid file store parameters, store size is zero.")
	}

	/* Clear out the dataset file and write new blank metadata. */
	this.log.Debug("initting file backing storage: " + this.m_store_filename)
	this.m_header.M_magic = ZENDEMIC_OBJECT_STORE_STREE_V_MAGIC_5k
	this.m_header.M_store_size_in_bytes = this.m_initial_store_size_in_bytes
	this.log.Debug("inital store size in bytes: ", tools.Prettylargenumber_uint64(this.m_initial_store_size_in_bytes))

	this.m_header.M_block_size = this.m_initial_block_size
	this.log.Debug("inital block size in bytes: ", tools.Prettylargenumber_uint64(uint64(this.m_initial_block_size)))
	this.m_header.M_nodes_per_block = this.m_initial_nodes_per_block
	this.log.Debug("inital nodes per block: ", this.m_initial_nodes_per_block)
	// this isn't stored for some reason, but we calculate it in  stree.Get_node_size_in_bytes()
	//	var max_node_size = max_value_length * (this.m_offspring_per_node + 1) // +1 is for mother node
	var max_node_size = this.m_header.M_block_size * (this.m_header.M_nodes_per_block + 1) // +1 is for mother node
	this.log.Debug("stree node size in bytes: ", max_node_size)

	this.m_header.M_block_count = this.calc_block_count()
	this.log.Debug("number of blocks available in backing store: ", tools.Prettylargenumber_uint64(uint64(this.m_header.M_block_count)))
	this.m_header.M_root_node = 0
	this.m_header.M_free_position = 1
	this.m_header.M_alignment = this.m_initial_file_store_block_alignment
	this.m_header.M_dirty = 1
	this.log.Debug("physical store block alignment: ", tools.Prettylargenumber_uint64(uint64(this.m_header.M_alignment)))
	// this is just informational, we don't need to know this, it gets calculated when we read/write offsets
	this.log.Debug("number of physical bytes used for a block: ", tools.Prettylargenumber_uint64(this.calc_offset(1)))
	/* While we're here we can work out how much space is being wasted by alignment. why not. */
	var waste_per_block = this.calc_offset(1) - uint64(this.m_header.M_block_size)
	this.log.Debug("wasted bytes per block: ", tools.Prettylargenumber_uint64(waste_per_block))
	var total_waste = waste_per_block * uint64(this.m_header.M_block_count)
	this.log.Debug("total bytes wasted due to alignment padding: ", tools.Prettylargenumber_uint64(total_waste))
	var total_waste_percent = total_waste * 100 / this.m_header.M_store_size_in_bytes
	this.log.Debug("total waste percent: ", total_waste_percent)

	var json string
	ret, json = this.Get_store_information()
	if ret != nil {
		return ret
	}
	this.log.Info(json)

	ret = this.Open_datastore() // caller of init closes this.
	if ret != nil {
		return ret
	}

	return this.write_header_to_disk(true)
}

func (this *File_store_aligned) write_header_to_disk(initting bool) Ret {
	/* everybody goes through here so we can calculate checksum */
	// do a checksum, hash whatever of the data in the header and add it to the end.

	var data *[]byte
	var ret Ret
	ret, data = this.m_header.serialize(this.log)
	if ret != nil {
		return ret
	}
	var m5 = md5.Sum(*data)
	var hashed_data = append(*data, m5[:]...)

	if initting {
		/* so the very first time we do this, we have to write out  CHECK_START_BLANK_BYTES
		so that the second time we come in, we can read a whole header check bytes block without
		getting EOF */
		var to_write = tools.Maxint(CHECK_START_BLANK_BYTES, int(this.m_initial_block_size))
		var pad_len = to_write - len(hashed_data)
		for pad_len > 0 { // slow and crappy but we only ever do it once.
			hashed_data = append(hashed_data, 0)
			pad_len = to_write - len(hashed_data)
		}
	}

	return this.write_raw_data(0, &hashed_data)
}

func (this *File_store_aligned) is_block_device(path string) (Ret, bool) {

	/* if it's not a block device, but a file that may not exist, lop off the filename and go for the path,
	   that thing won't be a block device either. if they give you the path of a block device that doesn't exist
	   well, it doesn't exist, it's not a block device. */
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		/* if it doesn't exist, it is not a block device, it is not a directory, it is not
		anything. So we should touch it, so it exists, so we can check its empty header. */
		var newfile, err = os.Create(path)
		if err != nil {
			return Error(this.log, "unable to create: ", path, " error: ", err), false
		}
		err = newfile.Close() // someday we will check for errors.
		if err != nil {
			return Error(this.log, "Error closing new file: ", path, " error: ", err), false
		}

		path = filepath.Dir(path) // get the path of the file we just created
	}

	/* if it's a block device, it might be a symlink, follow first. */

	var realpath, err = filepath.EvalSymlinks(path)
	if err != nil {
		return Error(this.log, "can't resolve symlinl: ", path, " error: ", err), false
	}
	path = realpath

	var st syscall.Stat_t
	err = syscall.Lstat(path, &st)
	if err != nil {
		return Error(this.log, "Error getting lstat of: ", path, " error: ", err), false
	}
	if st.Mode&S_IFMT == S_ISBLK { // got this from the kernel macro
		return nil, true
	}
	return nil, false
}

func (this *File_store_aligned) get_size_of_path(path string) (Ret, uint64) {
	var r, is_block = this.is_block_device(path)
	if r != nil {
		return r, 0
	}

	if is_block {
		/* seek to end and get position */
		file, err := os.Open(path)
		if err != nil {
			return Error(this.log, "Unable to open: ", path, " error: ", err), 0
		}

		defer func() {
			var err = file.Close()
			if err != nil {
				this.log.Error("Error closing block device for: ", path, " error: ", err)
			}
		}()
		pos, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			return Error(this.log, "Unable to seek to end of: ", path, " error: ", err), 0
		}
		return nil, uint64(pos)
	}
	// if it's a file... get the free space of the filesystem the file is on

	var directory = filepath.Dir(path)
	path = directory // get the stat of the directory the file is in, not the file itself.

	var stat unix.Statfs_t
	var err = unix.Statfs(path, &stat)
	if err != nil {
		return Error(this.log, "Unable to stat: ", path, " error: ", err), 0
	}
	var total = stat.Blocks * uint64(stat.Bsize)
	// now take 80% of each because we don't really want to fill the entire disk, unless we're a block device
	total = total * USABLE_SPACE_PERCENTAGE / 100

	return nil, total
}

func (this *File_store_aligned) Get_usable_storage_bytes(path string) (Ret, uint64) {
	/* 1/8/2021 using the disk size as the total from which to take 80%
	 * doesn't work too well. instead let's try free space and then we
	 * can take 80% of that,
	long total = localdiskstorage.getTotalSpace(); // size in bytes */
	// long total = localdiskstorage.getFreeSpace(); // size in bytes
	// okay that doesn't work either. so let's go with 80% of total disk space, block devices will work differently.
	//xxxz this doesn't work for loop devices.

	var ret, total = this.get_size_of_path(path)
	if ret != nil {
		return ret, 0
	}

	return nil, total
}

func (this *File_store_aligned) Is_backing_store_uninitialized() (Ret, bool) {
	/* Read the first 4k and see if it's all zeroes. */

	var ret = this.Open_datastore_readonly()
	if ret != nil {
		return ret, false
	}

	defer this.Shutdown()

	var bytes_read int
	var bresp = this.m_iopath.AllocBuffer(int(CHECK_START_BLANK_BYTES))
	var err error
	bytes_read, err = this.m_datastore.ReadAt(bresp, 0)
	if err != nil {
		/* special case for empty/short files, we will init. hopefully there's no weirdness with block devices
		yielding EOF */
		if err == io.EOF {
			this.log.Info("EOF reading header, empty file store, initializing new file store...")
			return nil, true
		}
		return Error(this.log, "Error reading from header block in data store block zero for length ", CHECK_START_BLANK_BYTES,
			" error: ", err), false
	}

	// short files will be empty and fail above, they should just be initted if it's a file.
	/* block devices should fail on this, and frankly if we can read something, and don't get EOF,
	   we don't know what it is, so error out, don't init it. */
	if bytes_read != int(CHECK_START_BLANK_BYTES) {
		return Error(this.log, "Unable to read from header block in data store block zero for length ", CHECK_START_BLANK_BYTES, ", only received ", bytes_read, " bytes"), false
	}

	for lp := 0; lp < int(CHECK_START_BLANK_BYTES); lp++ {
		if bresp[lp] != 0 {
			return nil, false
		}
	}
	return nil, true
}

func (this *File_store_aligned) Load_header_and_check_magic(check_device_params bool) Ret {
	/* read the first block and see if it's got our magic number, and validate size and blocks and all that. */
	/* for storage status, the values passed in device are bunk, so skip the checks
	   (this check_device_params) because they will fail. */

	// first thing we have to do is get the disk size to validate against because nobody but us and init does that.
	var ret = this.calculate_usable_storage()
	if ret != nil {
		return ret
	}

	var bytes_read uint32
	var data []byte
	ret, data = this.Read_raw_data(0)
	if ret != nil {
		return ret
	}

	// pull off the hash at the end before we do anything else
	if len(data) < crypto.MD5.Size() {
		return Error(this.log, "unable to read header, not enough data for checsum, length is only ", crypto.MD5.Size)
	}
	var header_data = data[0:int(this.m_header.Serialized_size())]
	var m5 = data[int(this.m_header.Serialized_size()) : this.m_header.Serialized_size()+uint32(crypto.MD5.Size())]

	var m5check = md5.Sum(header_data)
	if bytes.Compare(m5check[:], m5) != 0 {
		return Error(this.log, "unable to read header, hash check failed")
	}

	data = header_data
	if len(data) < int(this.m_header.Serialized_size()) {
		return Error(this.log, "unable to read header of ", this.m_header.Serialized_size(), " got back ", bytes_read)
	}

	ret = this.m_header.Deserialize(this.log, &data)
	if ret != nil {
		return ret
	}

	/* this means the header doesn't match what we expect, and we should init the backing storage,
	I can see where this could be a dangerously bad idea, so we're just going to error out, let
	the user deal with it. */
	if this.m_header.M_magic != ZENDEMIC_OBJECT_STORE_STREE_V_MAGIC_5k {
		return Error(this.log, "magic number doesn't match in backing storage")
	}

	if check_device_params {
		// see if they header matches what the caller specified
		if this.m_header.M_block_size != this.m_initial_block_size {
			return Error(this.log, "block size store cached in backing storage ", this.m_header.M_block_size,
				" doesn't match initial block size ", this.m_initial_block_size)
		}

		if this.m_header.M_store_size_in_bytes != this.m_initial_store_size_in_bytes {
			return Error(this.log, "store size in bytes cached in backing storage ", this.m_header.M_store_size_in_bytes,
				" doesn't match initial store size in bytes ", this.m_initial_store_size_in_bytes)
		}

		if this.m_header.M_nodes_per_block != this.m_initial_nodes_per_block {
			return Error(this.log, "nodes per block cached in backing storage ", this.m_header.M_nodes_per_block,
				" doesn't match initial nodes per block ", this.m_initial_nodes_per_block)
		}

		if uint64(this.m_header.M_block_count)*uint64(this.m_header.M_block_size) > this.m_header.M_store_size_in_bytes {
			return Error(this.log, "block count ", this.m_header.M_block_count, " times block_size ", this.m_header.M_block_size,
				" is greater than backing storage stored size ", this.m_header.M_store_size_in_bytes)
		}

		if this.m_header.M_alignment != this.m_initial_file_store_block_alignment {
			return Error(this.log, "alignment ", this.m_header.M_alignment,
				" doesn't match initial alignment ", this.m_initial_file_store_block_alignment)
		}
	} else { // if we should check device fields against the header or are we just reading the header to display.
		/* there are a few places that use the user/catalog supplied initial block size (for initial creation)
		   and therefore is wrong if just getting storage status, so we set it to what the on-disk header says.
		   can you see how hacky this is getting already? We should fix this. the on-disk should be authoritative always
		   and the initial setup should be made to work despite that. */
		this.m_initial_block_size = this.m_header.M_block_size
		this.m_initial_file_store_block_alignment = this.m_header.M_alignment
		this.m_initial_store_size_in_bytes = this.m_header.M_store_size_in_bytes
		this.m_initial_nodes_per_block = this.m_header.M_nodes_per_block
	}
	return nil // all is well.
}

func (this *File_store_aligned) calculate_usable_storage() Ret {
	var r Ret
	r, this.m_initial_store_size_in_bytes = this.Get_usable_storage_bytes(this.m_store_filename)
	if r != nil {
		return r
	}
	// f.log.Debug("usable storage bytes on storage device: ", f.m_store_filename, " = ",
	// 	tools.Prettylargenumber_uint64(f.m_initial_store_size_in_bytes))
	if this.m_initial_store_size_in_bytes == 0 {
		return Error(this.log, "get usable storage bytes returned zero. Can't allocate storage.")
	}
	return nil
}

func (this *File_store_aligned) Open_datastore_readonly() tools.Ret {
	if this.m_datastore != nil {
		return Error(this.log, "physical store already opened")
	}
	// var ret = this.calculate_usable_storage()
	// if ret != nil {
	// 	return ret
	// }

	/* so this is interesting. this is called for getting status and verifying the uninitializedness or not
	   of a backing store. The problem is the difference between an uninitialized backing store file
	   and an uninitialized backing store block device is that a block device exists and a file does not.
	   open readonly should never do any kind of write, so we can't touch the file, and we can't
	   really know when its new if it's a block device or a non existent file. So we return enoent
	   and let the caller deal with it for their particular situation. */
	var ret, found = tools.File_exists(this.log, this.m_store_filename)
	if ret != nil {
		return ret
	}
	if found == false {
		return ErrorWithCodeNoLog(this.log, int(syscall.ENOENT), "file does not exist: ", this.m_store_filename)
	}

	var err error
	this.m_datastore, err = this.m_iopath.OpenFile(this.m_store_filename, os.O_RDONLY, STREE_FILEMODE)
	if err != nil {
		this.m_datastore = nil // just in case

		return Error(this.log, "Unable to open physical store: ", this.m_store_filename, " error: ", err)
	}
	this.m_readonly = true
	return nil
}

func (this *File_store_aligned) Open_datastore() tools.Ret {
	if this.m_datastore != nil {
		return Error(this.log, "physical store already opened")
	}

	// var ret = this.calculate_usable_storage()
	// if ret != nil {
	// 	return ret
	// }

	var flags = os.O_RDWR
	if this.m_sync {
		flags |= os.O_SYNC // same as syscall.O_SYNC
	}
	var err error
	this.m_datastore, err = this.m_iopath.OpenFile(this.m_store_filename, flags, STREE_FILEMODE)
	if err != nil {
		this.m_datastore = nil // just in case
		return Error(this.log, "Unable to open physical store: ", this.m_store_filename, " error: ", err)
	}
	this.m_readonly = false
	return nil
}

func (this *File_store_aligned) Startup(force bool) Ret {
	/* start up (open) this file store for an existing initialized backing
	store and parse and validate the header. */

	if this.m_datastore != nil {
		return Error(this.log, "file store has already been initialized.")
	}

	var ret = this.Open_datastore()
	if ret != nil {
		return ret
	}

	/* 12/12/2021 we used to just make a new stree header if it was wrong, but this could be dangerous
	 * if we point it to a block device. so now we will only allow creating an stree if the first 4k
	 * of the device is all zeroes, thus there's no other filesystem or anything else there.
	 * Error in all other cases. */

	/* return error if no good, we do not check for uninit here
	   startup assumes it's been initted already. */

	ret = this.Load_header_and_check_magic(true) // check device params passed in from cmd line or catalog
	if ret != nil {
		return ret
	}
	// check dirty flag here and set it dirty
	if force == false {
		if this.m_header.M_dirty != 0 {
			return Error(this.log, "backing store was not cleanly shut down. add -f to force starting up anyawy.")
		}
	} else {
		if this.m_header.M_dirty != 0 {
			this.log.Info("backing store was not cleanly shut down, forcing startup anyway, data may be corrupt.")
		} else {
			this.log.Info("backing store is clean but force flag was passed unneccesarily.")
		}
	}
	this.m_header.M_dirty = 1
	ret = this.write_header_to_disk(false)
	return ret
}

func (this *File_store_aligned) Shutdown() Ret {
	if this.m_datastore == nil {
		return Error(this.log, "not initialized or already shut down.")
	}

	if this.m_readonly == false {
		// set dirty flag clean here, since we're shutting down cleanly, but only if not open readonly
		this.m_header.M_dirty = 0
		var ret = this.write_header_to_disk(false)
		if ret != nil {
			return ret
		}
	}
	// famously, nobody ever checks for error on close, but it's a good idea.
	var err = this.m_datastore.Close()
	if err != nil {
		return Error(this.log, "Unable to close datastore: ", this.m_store_filename, " error: ", err)
	}
	this.m_datastore = nil
	return nil
}

func (this *File_store_aligned) read_raw_data_length(block_num uint32, length uint32) (Ret, []byte) {

	/* 12/12/2021 the way we pad out to alignment is by making the block size round up to the alignment size before we do any math on it.
	 * since we only ever read one block at a time (hmmm...) we won't have to worry about there being gaps in the data */

	var offset uint64 = this.calc_offset(block_num)
	var bresp []byte = this.m_iopath.AllocBuffer(int(length)) // we offset block writes for alignment, but we only actually have to read our block size
	var bytes_read, err = this.m_datastore.ReadAt(bresp, int64(offset))
	if err != nil {
		return Error(this.log, "Error reading from data store at position  ", offset, " length ", length,
			" error: ", err), nil
	}
	/* if we had to pad the block for directio, shrink the slice to the actual size of data expected */
	if len(bresp) > int(length) {
		bresp = bresp[0:length]
		bytes_read = int(length)
	}

	if uint32(len(bresp)) != length {
		return Error(this.log, "Error reading from data store, tried to read ", length, " bytes, only read ", bytes_read), nil
	}

	return nil, bresp
}

func (this *File_store_aligned) Load_limit(block_num uint32, length uint32) (Ret, *[]byte) {
	/* Load only length bytes from block num, not the entire block.
	   must still round up to alignment though, in case of directio. */
	var bresp = make([]byte, length) // in read_raw_data
	var ret tools.Ret
	ret, bresp = this.read_raw_data_length(block_num, length)

	if ret != nil {
		return ret, nil
	}
	if len(bresp) != int(length) {
		return Error(this.log, "Unable to read from data store block ", block_num, " length ", length,
			", only received ", len(bresp), " bytes"), nil
	}
	return nil, &bresp

}

func (this *File_store_aligned) Read_raw_data(block_num uint32) (Ret, []byte) {

	/* 12/12/2021 the way we pad out to alignment is by making the block size round up to the alignment size before we do any math on it.
	 * since we only ever read one block at a time (hmmm...) we won't have to worry about there being gaps in the data */

	var offset uint64 = this.calc_offset(block_num)
	var length uint32 = this.m_initial_block_size             // this has to be initial_block size for the read of the header to see if the header matches
	var bresp []byte = this.m_iopath.AllocBuffer(int(length)) // we offset block writes for alignment, but we only actually have to read our block size
	var bytes_read, err = this.m_datastore.ReadAt(bresp, int64(offset))
	if err != nil {
		/* it is possible to get eof here if we read off the end of the file, this is not an error
		we just have to pad the rest of the response out with zeroes */
		var lp uint32
		if errors.Is(err, io.EOF) {
			for lp = uint32(bytes_read); lp < length; lp++ {
				bresp[lp] = 0 // fill the rest with zeroes
			}
			/* if we had to pad the block for directio, shrink the slice to the actual size of data expected */
			if len(bresp) > int(length) {
				bresp = bresp[0:length]
			}
			return nil, bresp
		}
		return Error(this.log, "Error reading from data store at position  ", offset, " length ", length,
			" error: ", err), nil
	}
	/* if we had to pad the block for directio, shrink the slice to the actual size of data expected */
	if len(bresp) > int(length) {
		bresp = bresp[0:length]
		bytes_read = int(length)
	}

	if uint32(len(bresp)) != length {
		return Error(this.log, "Error reading from data store, tried to read ", length, " bytes, only read ", bytes_read), nil
	}

	return nil, bresp
}

func (this *File_store_aligned) Load(block_num uint32) (Ret, *[]byte) {
	/* read in a node at this block_num and return it.
	 * As originally designed we'd always be writing a full block and therefore
	 * be able to read a full block always, but now we have to allow for short blocks
	 * as compressed data will not fill out the block. And if the last block written is
	 * short, the amount we read will come up short.
	 * So the better way to go to ensure correctness is not to allow short reads
	 * but to pad out to the block size when writing. so we'll do that in store() */
	var bresp []byte
	var ret tools.Ret
	ret, bresp = this.Read_raw_data(block_num)

	if ret != nil {
		return ret, nil
	}
	if len(bresp) != int(this.m_header.M_block_size) {
		return Error(this.log, "Unable to read from data store block ", block_num, " length ", this.m_header.M_block_size,
			", only received ", len(bresp), " bytes"), nil
	}
	return nil, &bresp
}

func (this *File_store_aligned) calc_offset(block_num uint32) uint64 {
	/* calculate the byte position offset of this block pos taking into account file_store_block_alignment */
	// we should just store this rather than recalculating it all the time. xxxz
	var alignedcount uint64 = uint64(this.m_initial_block_size) / uint64(this.m_initial_file_store_block_alignment)
	if this.m_initial_block_size%this.m_initial_file_store_block_alignment != 0 {
		alignedcount++
	}
	var pos uint64 = uint64(block_num) * (alignedcount * uint64(this.m_initial_file_store_block_alignment))
	return pos
}

func (this *File_store_aligned) write_raw_data(block_num uint32, data *[]byte) Ret {
	/* 12/12/2021 all writes must write to the end of the 4k boundary so the underlying block device/filesystem
	 * won't have to do a read update write. that's what makes this one different than the original file_store */
	if len(*data) > int(this.m_header.M_block_size) {
		return Error(this.log, "write_raw_data asked to write ", len(*data), " bytes but the block size is only ", this.m_header.M_block_size)
	}
	/* here we used to pad the data to write to the next stree block size, now we're going to pad to 4k */

	var to_write = data

	/* this is kinda crappy, since there are a bunch of different stree things that pass us buffers that we use to
	   write, we need to pass them an allocator so they can created aligned buffers, that's a bit of a mess right now
	   so we'll just copy it into a buffer we make. more copies, sigh */

	var aligned_data []byte = this.m_iopath.AllocBuffer(len(*to_write))
	var copied = copy(aligned_data, *data)
	if copied < len(*data) {
		return Error(this.log, "unable to copy aligned block of data. data length: ", len(*data), " amount copied: ", copied)
	}
	to_write = &aligned_data

	// we used to only copy if we were padding, now we copy every time.
	// if len(*data) < int(f.m_alignment) {
	// 	/* pad out to block size. by doing this we ensure that the load of the last
	// 	 * block in the file will always be able to read a full size block */
	// 	/* the stree part makes sure that the deserialize only actually returns the
	// 	   amount originally written. */
	// 	// copy the data to padded data which is filled out to the desired length and use that instead.
	// 	/* we have to do this as one big write not two, or it will fail on directio type writes and
	// 	   will defeat the purpose of trying to avoid the read-update-write cycle. */

	// 		 var padded_data []byte = f.m_iopath.AllocBuffer(int(f.m_alignment))
	// 		 var copied = copy(padded_data, *data)
	// 	if copied < len(*data) {
	// 		return Error(f.log, "unable to copy padded block of data. data length: ", len(*data), " amount copied: ", copied)
	// 	}
	// 	to_write = &padded_data
	// }

	// the position we write at starts at a multiple of m_alignment and the length will always be m_alignment
	/* to be more clear, we have our own logical concept of alignment over and above directio-type block alignment.
	   so if a stree node serializes to 13 bytes, we can say we want to align to 20 bytes, the first block will be
		 written at offset zero, and the second block will be written at offset 20. You must still have this alignment
		 be divisible by the directio alignment. but this allows you to align to larger than 4k blocks. If you're not
		 using directio, you can make it a prime number for all I care. */

	var offset uint64 = this.calc_offset(block_num)
	var length = len(*to_write)
	var written, err = this.m_datastore.WriteAt(*to_write, int64(offset))
	// someday configure if we flush immediately.
	if err != nil {
		return Error(this.log, "Error writing to data store at position  ", offset, " length ", length, " error: ", err)
	}
	if written != length {
		return Error(this.log, "Error writing to data store, tried to write ", length, " bytes, only wrote ", written)
	}
	return nil
}

func (this *File_store_aligned) Store(block_num uint32, data *[]byte) Ret {
	// store and load must be less than or equal to the block size.
	// we now allow for writing less than an entire block, and that's okay
	// as long as we can read it back and return exactly what we were given.
	if len(*data) > int(this.m_header.M_block_size) {
		return Error(this.log,
			"store asked to write ", len(*data), " bytes but the block size is only ", this.m_header.M_block_size)
	}
	return this.write_raw_data(block_num, data)
}

func (this *File_store_aligned) Get_root_node() (Ret, uint32) {
	return nil, this.m_header.M_root_node
}

func (this *File_store_aligned) Set_root_node(block_num uint32) Ret {

	this.m_header.M_root_node = block_num
	return this.write_header_to_disk(false)
}

func (this *File_store_aligned) Get_free_position() (Ret, uint32) {
	return nil, this.m_header.M_free_position
}

func (this *File_store_aligned) set_free_position(block_num uint32) Ret {
	/* 12/23/2020 at this point we know where the end of the file must be
	 * so we can truncate it and free up the space on disk.
	 * unless it's a block device, so we don't do that if it's a block device. */
	var shrinking bool = false
	if block_num < this.m_header.M_free_position {
		shrinking = true
	}
	this.m_header.M_free_position = block_num

	var ret = this.write_header_to_disk(false)

	if ret != nil {
		return ret
	}
	if shrinking == false {
		return nil
	}
	// xxxz store value of is-block-device and check it here.
	// now size the file appropriately, if it fails, no big deal, it's a block device. maybe work that out up front and we avoid running the error case on every shrink
	var newfilesize int64 = int64(block_num) * int64(this.m_header.M_block_size)
	os.Truncate(this.m_store_filename, int64(newfilesize))

	return nil
}

func (this *File_store_aligned) Get_total_blocks() (Ret, uint32) {
	// the max number of blocks we can fit in the backing store
	return nil, this.m_header.M_block_count

}

func (this *File_store_aligned) Allocate(amount uint32) (Ret, []uint32) { /* allocate i blocks from free position and return an array of the positions allocated */
	/* see if there's enough room to add these nodes and if so, return their
	   positions in the array and up the free position accordingly */
	/* if we ever go concurrent we're going to have to lock this and a lot of other things I suppose */
	if this.m_header.M_free_position+amount > this.m_header.M_block_count {
		return Error(this.log, "Not enough space available to allocate ", amount, " blocks."), nil
	}
	var lp uint32
	var rvals []uint32 = make([]uint32, amount)
	for lp = 0; lp < amount; lp++ {
		rvals[lp] = this.m_header.M_free_position
		this.m_header.M_free_position++
	}
	var ret = this.set_free_position(this.m_header.M_free_position)
	if ret != nil {
		return ret, nil
	}
	return nil, rvals

}

func (this *File_store_aligned) Deallocate() Ret {
	/* you can only deallocate one node at a time, at the moment,
	 *  and suffer the write hit for each one. sorry. */
	return this.set_free_position(this.m_header.M_free_position - 1)

}

func (this *File_store_aligned) Wipe() Ret {
	/* write zeros over the first block. */

	if this.m_datastore == nil {
		return Error(this.log, "Can't wipe stree file store: ", this.m_store_filename, ", filestore is shut down or not started.")
	}

	this.log.Info("wiping stree backing file store: ", this.m_store_filename)

	var zeros = make([]byte, CHECK_START_BLANK_BYTES)
	var ret = this.write_raw_data(0, &zeros)
	if ret != nil {
		return Error(this.log, "error trying to wipe: ", this.m_store_filename, " error: ", ret.Get_errmsg())
	}
	return nil
}

func (this *File_store_aligned) Dispose() Ret {
	/* delete the stree file. */
	if this.m_datastore != nil {
		return Error(this.log, "Can't dispose of stree file store: ", this.m_store_filename, ", filestore not shut down.")
	}

	var ret, is_block_device = this.is_block_device(this.m_store_filename)
	if ret != nil {
		return ret
	}

	if is_block_device == false {
		this.log.Info("deleting stree backing file store: ", this.m_store_filename)
		var err = os.Remove(this.m_store_filename)
		if err != nil {
			return Error(this.log, "error trying to delete: ", this.m_store_filename, " error: ", err)
		}

	} else {
		this.log.Info("backing store is a block device, nothing to delete.")
	}

	return nil
}
