#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"
#include <stdio.h>


#define EOF_FOUND 1
#define OUT_OF_MEMORY 2



/*
	A structure that holds query results for later use byt fetch and tuple


*/


typedef struct store {
    const char* name; // the name of the store variable 
    result* data;		  // the tuples 
} store;


/*
	Check if the givem db name already exist in the db pool
*/
db* get_db(const char* name);


/*
	Gets the table pointer

*/

table* get_table(const char* name);

/*
	returns the column pointer and fills in table

*/
column* get_column(table **tab, const char* name);



/*

Fill in one row of a table

*/
status table_add_relational(table* table, int* row, unsigned int size);

int dynamic_fgets(char** buf, int* size, FILE* file);


status load_file(char* fname);

// Returns a text represeting the result
char * tuple_columns(column ** columns, int n_cols, int length);

#endif // DB_H__
