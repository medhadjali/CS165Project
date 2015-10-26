#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"

typedef struct db_pool_entry {
	const char * name;
	db * obj;
	struct db_pool_entry * next;
} db_pool_entry;

/*
	This function adds a db to the db pool
*/
status add_db_pool(const char* name, db* db);


/*
	Check if the givem db name already exist in the db pool
*/
db* exist_db_pool(const char* name);


/*
	Gets the table pointer

*/

table* get_table(const char* name);


#endif // DB_H__
