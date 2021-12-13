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
#include "mdb.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))


int main() {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_db *db;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat *mst;
	MDB_cursor *cursor, *cur2;
	int count;
	int *values;

	srandom(time(NULL));

	    count = (random()%384) + 64;
	    values = (int *)malloc(count*sizeof(int));

		/*for(i = 0;i<count;i++) {*/
			/*values[i] = random()%1024;*/
		/*}*/
        values[0] = 5000;
        char key_val[32];
        char data_val[32];
        sprintf(key_val, "key of %d foo bar", values[0]);
        sprintf(data_val, "data of %d foo bar", values[0]);
    
		rc = mdbenv_create(&env);
		rc = mdbenv_set_mapsize(env, 10485760);
		rc = mdbenv_open(env, "./testdb", MDB_FIXEDMAP|MDB_NOSYNC, 0664);
        rc = mdb_txn_begin(env, 0, &txn);
        rc = mdb_open(env, txn, NULL, 0, &db);
   
        key.mv_size = sizeof(key_val);
        key.mv_data = key_val;
        data.mv_size = sizeof(data_val);
        data.mv_data = data_val;

        mdb_put(db, txn, &key, &data, MDB_NOOVERWRITE);
        rc = mdb_txn_commit(txn);
        mdb_cursor_open(db, NULL, &cursor);
        mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
        printf("key: %.*s, data: %.*s\n", key.mv_size, key.mv_data, data.mv_size, data.mv_data);
		mdb_close(db);
		mdbenv_close(env);
}
