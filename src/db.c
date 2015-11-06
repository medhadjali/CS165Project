#include "db.h"
#include <string.h>
#include "utils.h"
#include <unistd.h>

//kefta : db variable pool
db** db_pool = NULL;
size_t db_pool_size = 0;

// read large lines from a file


int dynamic_fgets(char** buf, int* size, FILE* file)
{
    char* offset;
    int old_size;

    if (!fgets(*buf, *size, file)) {
        return EOF_FOUND;
    }

    if ((*buf)[strlen(*buf) - 1] == '\n') {
        return 0;
    }

    do {
        /* we haven't read the whole line so grow the buffer */
        old_size = *size;
        *size *= 2;
        *buf = realloc(*buf, *size);
        if (NULL == *buf) {
            return OUT_OF_MEMORY;
        }
        offset = &((*buf)[old_size - 1]);

    } while ( fgets(offset, old_size + 1, file)
        && offset[strlen(offset) - 1] != '\n' );

    return 0;
}



// Kefta : Add a DB mapping in the db pool 
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

// Kefta :  Add a DB mapping in the db pool (linked list)
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

    // kefta BUG : str_cpy freed after parser exit
    char * db_name_cpy = malloc(strlen(db_name)+1);
    strncpy(db_name_cpy, db_name, strlen(db_name)+1);
    // 

    (*db)->name =  db_name_cpy;
    (*db)->table_count = 0;
    (*db)->tables = NULL;

    status s;
    s.code = OK;
    return s;
}

// create table 
status create_table(db* db, const char* name, size_t num_columns, table** tbl) {

	if (db->tables == NULL)
	{
		// here the db has no tables in it
		// let's create one
		db->tables = malloc(sizeof(table));
		db->table_count = 1;

	} else {

		// check if the table exists and get the pointer to it
		if ( get_table(name) != NULL) {
				log_info("table %s already exists\n", name);
				status ret;
				ret.code = ERROR;
				ret.message = "Table already exists";
				return ret;
		 }

		// we need to resize the tables array
		db->table_count += 1;
		db->tables = realloc(db->tables, db->table_count*sizeof(table));
	}

	(*tbl) = &(db->tables[db->table_count-1]);
    // fill in table values
    char * tbl_name_cpy =  malloc(strlen(name)+1);


    strncpy(tbl_name_cpy, name, strlen(name)+1);

	(*tbl)->name = tbl_name_cpy;
    (*tbl)->col_count = num_columns;
    (*tbl)->col = malloc(num_columns*sizeof(column));
    (*tbl)->length = 0;

    // initialize column names to NULL
    for (size_t i = 0; i < num_columns; ++i)
    {
    	((*tbl)->col[i]).name = NULL;
    }


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
    	if (strcmp((db->tables[i]).name, name) == 0) {
    		// got it
    		return &(db->tables[i]);

    	} 
    }


	return NULL;
}


status create_column(table *table, const char* name, column** col){

	// does the table exists already ?
	if (table == NULL) {
		// How come table does not exist ?
		status s;
	    s.code = ERROR;
	    return s;
	}
	unsigned int i = 0;

	while (i < table->col_count)
	{
		column* coll = &(table->col[i]);
		if (coll->name == NULL) {
			break;
		}
		if (strcmp(coll->name, name) == 0)
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



    char * col_name_cpy = malloc(strlen(name)+1);
    strncpy(col_name_cpy, name, strlen(name)+1);

	(*col) = &(table->col[i]);

    (*col)->name =  col_name_cpy;
    (*col)->data = NULL;
    (*col)->index = NULL;

    status s;
    s.code = OK;
    return s;

}



status table_add_relational(table* table, int* row, unsigned int size) {
    status s;

	// does the table exists already ?
	if (table == NULL) {
		// How come table does not exist ?
	    s.code = ERROR;
	    return s;
	}

    // Is the given row size fits the number of columns here

    if (size != table->col_count) {

        s.code = ERROR;
        s.message = "tuple size mismatch";
        return s;

    }

	// resize
	table->length += 1; 

	// reallocate and insert a new value for each column
	for (size_t i = 0; i < table->col_count  ; ++i)
	{

        column* col = &(table->col[i]); // this is the column
        // if this is the first time , allocate data
		if (col->data == NULL) {
			col->data = malloc(sizeof(int));

		} else { // otherwise resize it 
			col->data = realloc(col->data, table->length*sizeof(int));
		}
		(col->data)[table->length-1] = row[i];
	}
    s.code = OK;
    return s;
}

column* get_column(table *table, const char* name){

    // does the table exists already ?
    if (table == NULL) {
        // How come table does not exist ?
        return NULL;
    }
    unsigned int i = 0;

    while (i < table->col_count)
    {
        column* coll = &(table->col[i]);
        if (strcmp(coll->name, name) == 0)
        {
            return coll;
        }
        i++;
    }
    // Nothing found
    return NULL;

}

//  Load csv file

status load_file(char* fname) {

    // open the file
    status s;
    // char cwd[1024] ;
    // getcwd(cwd, sizeof(cwd));
    FILE *fstream = fopen(fname,"r");
    if(fstream == NULL)
    {
        s.code = ERROR;
        s.message = "file opening failed ";
        return s;
    }
    char* input;
    char* header;
    int input_size = 2;
    int rc;

    header = malloc(input_size);
    // read the header
    rc = dynamic_fgets(&header, &input_size, fstream);
    /* analyze the header */

    // create an array of char* 
    char * col = strtok(header, ",");

    if (col == NULL) {
        s.code = ERROR;
        s.message = "Nothing in the header of this file";
        return s;
    }


    char * dbn = strtok(col, ".");
    char * tbn = strtok(NULL, ".");
    // Generate the full name using <db_name>.<tbl_name>
    char tbln[strlen(dbn) + strlen(tbn) + 1];
    strncat(tbln, dbn, strlen(dbn));
    strncat(tbln, ".", 1);
    strncat(tbln, tbn, strlen(tbn));

    // look for the table
    table * tbl = get_table(tbln);

    if (tbl == NULL) {
        s.code = ERROR;
        s.message = "Unknown table";
        return s;
    }



    // assume the columns are in order
    // TODO: have fun dealing with non-ordered columns



    // done with it  
    free(header);
    input = malloc(input_size);

    int n_tuples = 0;
    // now read the tuples
    while (1) {
        rc = dynamic_fgets(&input, &input_size, fstream);

        if (rc != 0 ) { break; }
        int * tuples = malloc(sizeof(int));
        char * val = strtok(header, ",");
        if (val == NULL) {continue;}
        n_tuples++;
        tuples[0] = atoi(val);
        int  n_cols = 0;
        char * tmp = NULL;
        // read line CSV into array 
        while ((tmp = strtok(NULL, ",")) != NULL) {
            n_cols++;
            tuples = realloc(tuples, n_cols*sizeof(int));
            tuples[n_cols-1] = atoi(tmp);
        }

        // insert the tuples array into the table
        status r =  table_add_relational(tbl, tuples, n_cols);

        if (r.code != OK) {
            // How come table does not exist ?
            free(input);

            s.code = ERROR;
            return s;
        }

        
    }

    free(input);

    s.code = OK;
    return s;
}



