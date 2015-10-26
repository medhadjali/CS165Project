#include "db.h"
#include <string.h>
#include "utils.h"


//db variable pool
struct db_pool_entry* db_pool = NULL;

// Add a DB mapping in the db pool (linked list)
status add_db_pool(const char* name, db* db) {
    log_info("Adding %s to the list of db pool", name);
    if (db_pool == NULL) {
        // hit when no DB exist
        db_pool = (db_pool_entry*) malloc(sizeof(db_pool_entry));
        db_pool->next=NULL;
        db_pool->obj = db;
        db_pool->name = name;
    } else {
        // search for the last entry
        db_pool_entry* tmp_nxt = db_pool;
        while (tmp_nxt->next != NULL) {
            tmp_nxt = tmp_nxt->next;
        }
        // append the new item
        tmp_nxt->next = (db_pool_entry*) malloc(sizeof(db_pool_entry));
        tmp_nxt->next->next=NULL;
        tmp_nxt->next->obj = db;
        tmp_nxt->next->name = name;

    }
    // Nothing was found!
    status s;
    s.code = OK;
    return s;
}

// Add a DB mapping in the db pool (linked list)
db* exist_db_pool(const char* name) {
    log_info("search %s in the list of db pool", name);
    if (db_pool == NULL) {
        return NULL;
    } else {
        // search for the last entry
        db_pool_entry*  tmp_nxt = db_pool;
        while (tmp_nxt != NULL) {
            if (strcmp(tmp_nxt->name, name) == 0)
            {
                return tmp_nxt->obj;
            }
            tmp_nxt = tmp_nxt->next;
        }
    }
    // Nothing was found!
    return NULL;
}


// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.
status create_db(const char* db_name, db** db) {
    
	// Check if the db_name given is already used in the db pool
    if (exist_db_pool(db_name)!=NULL) {
        log_info("%s already exists\n", db_name);
        status ret;
        ret.code = ERROR;
        ret.message = "Database already exists";
        return ret;
    }



    if (*db == NULL) {
        *db = malloc(sizeof(db));
    }

    (*db)->name = db_name;
    (*db)->table_count = 0;
    (*db)->tables = NULL;

    status s;
    s.code = OK;
    return s;
}

// create table 
status create_table(db* db, const char* name, size_t num_columns, table** table) {

	if (db->tables == NULL)
	{
		// here the db has no tables in it
		// let's create one
		db->tables = malloc(sizeof(*table));
		db->table_count = 1;

	} else {

		// check if the table exists and get the pointer to it
		for (size_t i = 0; i < db->table_count; ++i)
		{
			if (strcmp((db->tables[i])->name, name) == 0) {

				log_info("table %s already exists\n", name);
				status ret;
				ret.code = ERROR;
				ret.message = "Table already exists";
				return ret;
			}

		 }

		// we need to resize the tables array
		db->table_count += 1;
		db->tables = realloc(db->tables, db->table_count*sizeof(table));
	}


	if (*table == NULL) {
        *table = malloc(sizeof(table));
    }

    // fill in table values
    (*table)->name = name;
    (*table)->col_count = num_columns;
    (*table)->col = malloc(num_columns*sizeof(*column));
    //initialize columns
    for (int i = 0; i < num_columns; ++i)
    {
    	(*table)->col=NULL;
    }








    // finally associate this pointer to the end of db tables table of pointer
	db->tables[db->table_count-1] = (*table);

    status s;
    s.code = OK;
    return s;



}
table* get_table(const char* name) {




	return NULL;
}