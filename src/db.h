#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"
#include <stdio.h>


#define EOF_FOUND 1
#define OUT_OF_MEMORY 2

typedef struct db_pool_entry {
	const char * name;
	db * obj;
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


/*

Fill in one row of a table

*/
status table_add_relational(table* table, int* row, unsigned int size);

int dynamic_fgets(char** buf, int* size, FILE* file);


status load_file(char* fname);



#endif // DB_H__
