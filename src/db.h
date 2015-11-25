#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"
#include <stdio.h>
#include <stdint.h>

#define EOF_FOUND 1
#define OUT_OF_MEMORY 2



/*
	A structure that holds query results for later use byt fetch and tuple


*/


typedef struct store {
    const char* name; // the name of the store variable 
    result* data;		  // the tuples 
} store;

typedef enum HeaderType {
    DB,
    TABLE,
    COLUMN,
    END

} HeaderType;

// this is the section header in schame file 
typedef struct hfschema {
    HeaderType  type; 
    uint32_t length;
} hfschema;

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
status table_add_relational(table* table, int64_t* row, unsigned int size);

int dynamic_fgets(char** buf, int* size, FILE* file);


status load_file(char* fname);
void add_store_in_pool(const char* name, result* res);
// Returns a text represeting the result
char * tuple_columns(column ** columns, int n_cols, int length);
store* get_store(const char* name);
char * tuple_results(result ** rs, size_t n_res, size_t len);
status add_vectors(result ** rs, size_t n_res, size_t len, const char* target_store_name);
status sub_vectors(result * rs1, result * rs2, size_t len, const char* target_store_name);
void add_store_in_pool(const char* name, result* res) ;
status average_store(const char*  rs, const char* target_store);
status average_column(const char*  col, const char* target_store);
status min_store(const char*  rs, const char* target_store);
status min_column(const char*  col, const char* target_store);
status max_store(const char*  rs, const char* target_store);
status max_column(const char*  col, const char* target_store);
status sync_to_disk();
status graceful_shutdown();
status load_from_disk();

#endif // DB_H__

