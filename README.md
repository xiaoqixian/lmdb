### LMDB

##### (commit: 84659a5bb6a474b50ad1b090c54e1df623d40778)

##### `mdbenv_open`

When opening a db environment, we open a database file, get the file descriptor. Then we need to create a shared memory segment attached to it if the shared memory segment doesn't exist. Otherwise, we just get the memory segment.

If a shared memory segment is newly created, we can get a pointer to the shared memory segment with `shmat` and it is assigned to a `MDB_txn` struct. With the pointer, two mutexes are mainly initialized. We will introduce you these two mutexes later.

##### `mdbenv_open2`

`mdbenv_open` is mainly about opening a database file and creating a shared memory segment, after that, we use `mdbenv_open2` to assign some database environment attributions. 

We can determine if the database is newly created by reading it's fd. If nothing to read, it's new. 

For both new and old database, we use `mmap` to map contents in file to the virtual memory space (as LMDB is featured in memory mapping). It's just for a new database, maped address is decided by OS. If with `MDB_FIXEDMAP` , this address will be recorded in the environment header so when the same database file opened again (which is relatively an old database), we can read the environment header and get the address so we can map contents in the file at the same address. `MDB_FIXEDMAP` is a highly experimental feature, it depends on how the operating system has allocated memory to shared libraries and other uses.



