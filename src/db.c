#include "db.h"
#include <string.h>
#include "utils.h"


//db variable pool
db** db_pool = NULL;
size_t db_pool_size = 0;

// Add a DB mapping in the db pool 
status add_db_pool(const char* name, db* dbo) {
    log_info("Adding %s to the list of db pool", name);
    if (db_pool == NULL) {
        // hit when no DB exist
        db_pool =  malloc(sizeof(db*));
        db_pool_size = 1;
    } else {
        // expand the array
        db_pool_size += 1;
        db_pool = realloc(db_pool, db_pool_size*sizeof(db*));
    }


    db_pool[db_pool_size-1] = dbo;

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
    }

    // search for the last entry
    for (size_t i = 0; i < db_pool_size; ++i)
    {
        if (strcmp(db_pool[i]->name, name) == 0)
        {
            return db_pool[i];
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


    char * db_name_cpy = malloc(strlen(db_name)+1);
    strncpy(db_name_cpy, db_name, strlen(db_name)+1);

    (*db)->name =  db_name_cpy;
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
    char * tbl_name_cpy =  malloc(strlen(name)+1);
    strncpy(tbl_name_cpy, name, strlen(name)+1);

	(*table)->name = tbl_name_cpy;
    (*table)->col_count = num_columns;
    (*table)->col = malloc(num_columns*sizeof(column*));
    //initialize columns
    for (size_t i = 0; i < num_columns; ++i)
    {
    	((*table)->col)[i]=NULL;
    }

    // finally associate this pointer to the end of db tables table of pointer
	db->tables[db->table_count-1] = (*table);

    status s;
    s.code = OK;
    return s;



}
table* get_table(const char* name) {

	if (name == NULL) {
		return NULL;
	}

	//take a copy because strtok will modify it
	char* str_cpy = malloc(strlen(name));
	strncpy(str_cpy, name, strlen(name));


    // this gives us the first part as db name
     char * db_name = strtok(str_cpy, ".");

    db* db = exist_db_pool(db_name);

    //free memory
    free(str_cpy);

    if (db == NULL) {
    	return NULL;
    }

    // here we got the DB

    if (db->tables == NULL) {
    	return NULL;
    }

    for (size_t i = 0; i < db->table_count; ++i)
    {
    	if (strcmp((db->tables[i])->name, name) == 0) {
    		// got it
    		return db->tables[i];

    	} 
    }


	return NULL;
}


status create_column(table *table, const char* name, column** col){

	// is the column name exists already ?
	if (table == NULL) {
		// How come table does not exist ?
		status s;
	    s.code = ERROR;
	    return s;
	}
	unsigned int i = 0;

	while (i < table->col_count)
	{
		if (table->col[i] == NULL)
		{
			// first unassigned column means we can add to it
			break;
		}
		if (strcmp(table->col[i]->name, name) == 0)
		{
			status s;
		    s.code = ERROR;
		    s.message = "[ERROR] Duplicate column name";
		    return s;
		}
		i++;
	}

	if (i == table->col_count) {
		status s;
		s.code = ERROR;
		s.message = "[ERROR] No more columns allowed";
		return s;
	}

    if (*col == NULL) {
        *col = malloc(sizeof(column));
    }


    char * col_name_cpy = malloc(strlen(name)+1);
    strncpy(col_name_cpy, name, strlen(name)+1);

    (*col)->name =  col_name_cpy;
    (*col)->data = NULL;
    (*col)->index = NULL;

	table->col[i] = *col;

    status s;
    s.code = OK;
    return s;

}
