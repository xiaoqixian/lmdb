### LMDB

##### (commit: 84659a5bb6a474b50ad1b090c54e1df623d40778)

#### why mmap?

Here's a [relevant question](https://stackoverflow.com/questions/258091/when-should-i-use-mmap-for-file-access) in StackOverflow. In POSIX environment, there are at least two ways of accessing files: the regular `read()` and `write()`  functions and mmap. And mmap has great advantages in the context of multiple processes accessing a same file. 

And that's because of shared memory. With `read()` and `write()` functions, when multiple processes read a same file, the operating system has to allocate memory for each process for file reading. But with mmap, all processes share a same memory segment.

##### `mdbenv_open`

When opening a db environment, we open a database file, get the file descriptor. Then we need to create a shared memory segment attached to it if the shared memory segment doesn't exist. Otherwise, we just get the memory segment.

If a shared memory segment is newly created, we can get a pointer to the shared memory segment with `shmat` and it is assigned to a `MDB_txn` struct. With the pointer, two mutexes are mainly initialized. We will introduce you these two mutexes later.

##### `mdbenv_open2`

`mdbenv_open` is mainly about opening a database file and creating a shared memory segment, after that, we use `mdbenv_open2` to assign some database environment attributions. 

We can determine if the database is newly created by reading it's fd. If nothing to read, it's new. 

For both new and old database, we use `mmap` to map contents in file to the virtual memory space (as LMDB is featured in memory mapping). It's just for a new database, maped address is decided by OS. If with `MDB_FIXEDMAP` , this address will be recorded in the environment header so when the same database file opened again (which is relatively an old database), we can read the environment header and get the address so we can map contents in the file at the same address. `MDB_FIXEDMAP` is a highly experimental feature, it depends on how the operating system has allocated memory to shared libraries and other uses.

##### `mdb_txn_begin`

No matter if we need to put a new key/value pair into the database or delete a key/value pair from the database. These modifications to the database are all considered as transactions. Multiple modifications can be done in a single transaction or can be divided into different transactions. 

All modifications to the database won't take effect until the transaction they belonged to is formally committed. A transaction can be also read only if you set the correct flag.

Use `mdb_txn_begin` to start a new transaction. When a transaction is created, it is assigned to a unique id which is also the number of current transactions created. 

Now let's take a look at the `struct MDB_txn`:

```c
struct MDB_txn {
	pgno_t		mt_root;		/* current / new root page */
	pgno_t		mt_next_pgno;	/* next unallocated page */
	pgno_t		mt_first_pgno;
	ulong		mt_txnid;
	MDB_env		*mt_env;	
	union {
		struct dirty_queue	*dirty_queue;	/* modified pages */
		MDB_reader	*reader;
	} mt_u;
#define MDB_TXN_RDONLY		 0x01		/* read-only transaction */
#define MDB_TXN_ERROR		 0x02		/* an error has occurred */
	unsigned int		 mt_flags;
};
```

For now we can just focus on the `mt_u` member, which is a union, means it can either be `dirty_queue` or `reader`. So when it's a read only transaction, `mt_u.reader` is valid, and it's the reader function pointer. Otherwise,  a dirty queue is maintained by the transaction to keep track of dirty pages and keep data up to date.

For a read only transaction, a specific value is set by `pthread_setspecific` which is of `MDB_reader` type. I'm not gonna talk about `pthread_setspecific` and `pthread_getspecific` specifically here. All you need to know is that with these functions, a thread is able to get the value without passing parameters. When a thread begins a transaction for the first time, a `MDB_reader` type value is registered with `pthread_setspecific`, when the same thread begins a read only transaction next time, it can get the `MDB_reader` directly with `pthread_getspecific`.

For a write transaction, there's nothing much to do. Just set `env->me_txn` as the current transaction, cause the environment need to keep track of the current write transaction.

##### `mdb_put`

`mdb_put` puts a key/value pair into the database. First of all, it searches the right page in the B+ tree with `mdb_search_page`. If the tree is empty, allocate a new page.

Next, it searches the smallest node greater than the key. It may happen that the same key already exist in the tree, in that case, if the `MDB_NOOVERWRITE` flag is set, return `EEXIST` code, otherwise the original node is deleted from the page. And you can't just overwrite on the same node cause you are not sure if the data size is the same. Otherwise data belongs to other nodes may be overwritten.

If get the right node, if there is no enough space left for a new leaf node, then split the leaf node with `mdb_split`, otherwise add a new node with `mdb_add_node`.

After all above work, if the return code is not `MDB_SUCCESS`, then the transaction has an error. Any transaction with error won't be committed.

##### `mdb_txn_commit`

Constraints of committable transactions:

- Can't be read only
- Must be the current write transaction recorded in the environment struct.
- Must not have errors

Any violation causes the transaction aborted.

All write transactions carries a dirty queue, every time make changes to a page, if the page is not dirty, then the clean page is replaced with a new dirty page. After then the changes will be made to the page, so the original pages will always keep clean in a transaction. All dirty pages will be appended to the dirty queue when they are allocated. 

So when commit a transaction, all dirty pages are written back to disk. LMDB uses iovec to support multiple writing sources. Page write back address is identified by page number.

After all dirty pages have been written back, the `fsync` function is called to synchronize file contents to memory. 

##### `mdb_search_page_root`

`mdb_search_page`  searches a page by a key, but `mdb_search_page` is just a wrapper of `mdb_search_page_root`.

From the root node, `mdb_search_page_root` finds all the right branch node till get to the right leaf node.

If modify parameter is 1, all branch nodes and the leaf node along the way will be replaced with new ones with page flag set `P_DIRTY`. 

##### `mdb_add_node`

Add a key/value pair into a node. If the the `F_BIGDATA` flag is set, or the data size is larger than `page_size / MDB_MINKEYS`, then the data is considered as a big data. All big data is stored in a single page. 

Let's see what's the structure of a memory page:

```c
typedef struct MDB_page {		/* represents a page of storage */
	pgno_t		mp_pgno;		/* page number */
#define	P_BRANCH	 0x01		/* branch page */
#define	P_LEAF		 0x02		/* leaf page */
#define	P_OVERFLOW	 0x04		/* overflow page */
#define	P_META		 0x08		/* meta page */
#define	P_HEAD		 0x10		/* header page */
#define	P_DIRTY		 0x20		/* dirty page */
	uint32_t	mp_flags;
#define mp_lower	mp_pb.pb.pb_lower
#define mp_upper	mp_pb.pb.pb_upper
#define mp_pages	mp_pb.pb_pages
	union page_bounds {
		struct {
			indx_t		pb_lower;		/* lower bound of free space */
			indx_t		pb_upper;		/* upper bound of free space */
		} pb;
		uint32_t	pb_pages;	/* number of overflow pages */
	} mp_pb;
	indx_t		mp_ptrs[1];		/* dynamic size */
} MDB_page;
```

A page is considered as a continuous logical memory space with a lower address and a upper address. An pointer array is stored in the page that increases from bottom to top, and that is `mp_ptrs` in `MDB_page`. `mp_ptrs` stores all pointers to key/value pairs stored in the same page, so users can quickly get a key/value pair by index. And the pointers stored in this array are sorted by key. So every time a new key/value pair inserted into the page, all pointers respond to the keys that are greater than the key inserted into have to be readjusted. 

Oppositely, all key/value pairs are stored from top to bottom, between `mp_ptrs` and key/value pairs are the free space, maintained by two variables: `pb_lower` and `pb_upper`. If the page passed in is a branch, then the `data` parameter is actually the child page number. Else if the page is a leaf, the `data` is the real data. 

#### Cursor

