/**********************************************
  > File Name		: mtest7.c
  > Author		    : lunar
  > Email			: lunar_ubuntu@qq.com
  > Created Time	: Thu 09 Dec 2021 04:02:24 PM CST
  > Location        : Shanghai
  > Copyright@ https://github.com/xiaoqixian
 **********************************************/

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "lmdb.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))


int main() {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor, *cur2;
	MDB_cursor_op op;
	int count;
	int *values;
	char sval[32] = "";

	srand(time(NULL));

    count = (rand()%384) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
        values[i] = rand()%1024;
    }

    E(mdb_env_create(&env));
    printf("DB environment created\n");
    
    E(mdb_env_set_maxreaders(env, 1));
    printf("set maxreaders as 1\n");

    E(mdb_env_set_mapsize(env, 10485760));
    E(mdb_env_open(env, "./testdb", MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));
    printf("open an environement\n");

    E(mdb_txn_begin(env, NULL, 0, &txn));
    printf("begin a transaction\n");

    E(mdb_dbi_open(txn, NULL, 0, &dbi));
    printf("open a database\n");

    key.mv_size = sizeof(int);
    key.mv_data = sval;
    
    sprintf(sval, "%03x %d foo bar", values[0], values[0]);
    data.mv_size = sizeof(sval);
    data.mv_data = sval;
    
    if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE))) {
        data.mv_size = sizeof(sval);
        data.mv_data = sval;
    }
    printf("put a key-value pair into the db\n");

    E(mdb_txn_commit(txn));
    printf("commit a transaction\n");
    E(mdb_env_stat(env, &mst));

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    return 0;
}
