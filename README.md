### LMDB

##### (commit: 84659a5bb6a474b50ad1b090c54e1df623d40778)

#### why mmap?

Here's a [relevant question](https://stackoverflow.com/questions/258091/when-should-i-use-mmap-for-file-access) in StackOverflow. In POSIX environment, there are at least two ways of accessing files: the regular `read()` and `write()`  functions and mmap. And mmap has great advantages in the context of multiple processes accessing a same file. 

And that's because of shared memory. With `read()` and `write()` functions, when multiple processes read a same file, the operating system has to allocate memory for each process for file reading. But with mmap, the file content is managed by the operating system. 

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

Let's find out how LMDB actually put a key/value pair into the database. 
