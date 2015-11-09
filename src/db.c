#include "db.h"
#include <string.h>
#include "utils.h"
#include <unistd.h>

//kefta : db variable pool
db** db_pool = NULL;
size_t db_pool_size = 0;




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



// Kefta :  Add a DB mapping in the db pool (linked list)
db* get_db(const char* name) {
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
status create_db(const char* db_name) {
    
    status ret;

    // Check if the db_name given is already used in the db pool
    if (get_db(db_name)!=NULL) {
        log_info("%s already exists\n", db_name);
        ret.code = ERROR;
        ret.message = "Database already exists";
        return ret;
    }

    // kefta BUG : str_cpy freed after parser exit
    char * db_name_cpy = malloc(strlen(db_name)+1);
    strcpy(db_name_cpy, db_name);
    // 

    db * dbo = malloc(sizeof(db));

    dbo->name =  db_name_cpy;
    dbo->table_count = 0;
    dbo->tables = NULL;

    // Add to DB pool

    db_pool = realloc(db_pool, ++db_pool_size*sizeof(db*));
    db_pool[db_pool_size-1] = dbo;


    ret.code = OK;
    return ret;
}

// create table 
status create_table(const char* db_name, const char* name, size_t num_columns) {


    status ret;

    // 
    db* db1 = (db*)get_db(db_name);
    if (db1 == NULL) {
        log_info("%s Database does not exist\n", db_name);
        ret.code = ERROR;
        ret.message = "[ERROR] Database does not exist !!"; 
        return ret;
    }

	// check if the table exists and get the pointer to it
	if ( get_table(name) != NULL) {
			log_info("table %s already exists\n", name);
			ret.code = ERROR;
			ret.message = "Table already exists";
			return ret;
	 }

	// otherwise we need to allocate the tables array
	db1->table_count += 1;
	db1->tables = realloc(db1->tables, db1->table_count*sizeof(table));

	table * tbl = &(db1->tables[db1->table_count-1]);
    // fill in table values
    char * tbl_name_cpy =  malloc(strlen(name)+1);
    strcpy(tbl_name_cpy, name);

	tbl->name = tbl_name_cpy;
    tbl->col_count = num_columns;
    tbl->col = malloc(num_columns*sizeof(column));
    tbl->length = 0;

    // initialize column names to NULL
    for (size_t i = 0; i < num_columns; ++i)
    {
    	(tbl->col[i]).name = NULL;
    }


    ret.code = OK;
    return ret;

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

    db* db = get_db(db_name);

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


status create_column(const char* tbl_name, const char* name){


    status s;
    s.message = NULL;
    s.code = OK;

    // TODO(USER): You MUST get the original table* associated with <tbl_name>
    table* tbl = get_table(tbl_name);
    if (tbl == NULL) {

        s.code = ERROR;
        s.message = "[ERROR] Table does not exist";
        return s;
    }

	unsigned int i = 0;

	while (i < tbl->col_count)
	{
		column* coll = &(tbl->col[i]);
		if (coll->name == NULL) {
			break;
		}
		if (strcmp(coll->name, name) == 0)
		{
		    s.code = ERROR;
		    s.message = "[ERROR] Duplicate column name";
		    return s;
		}
		i++;
	}

	if (i == tbl->col_count) {
		s.code = ERROR;
		s.message = "[ERROR] No more columns allowed";
		return s;
	}

    char * col_name_cpy = malloc(strlen(name)+1);
    strncpy(col_name_cpy, name, strlen(name)+1);

	column * col = &(tbl->col[i]);

    col->name =  col_name_cpy;
    col->data = NULL;
    col->index = NULL;

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
        col->data = realloc(col->data, table->length*sizeof(int));
		(col->data)[table->length-1] = row[i];
	}
    s.code = OK;
    return s;
}

column* get_column(table **tab, const char* col_name){
        
    // find the table from col_name, but first let's store a copy before giving it to strtok
    char* col_name_cpy =  malloc(strlen(col_name)+1);
    strncpy(col_name_cpy, col_name, strlen(col_name) + 1);
    char * db1 = strtok(col_name_cpy, ".");
    char * tbl1 = strtok(NULL, ".");

    char tbl_name[strlen(db1) + strlen(tbl1) + 2];
    tbl_name[0] = '\0';
    strncat(tbl_name, db1, strlen(db1));
    strncat(tbl_name, ".", 1);
    strncat(tbl_name, tbl1, strlen(tbl1));
    
    table* tbl = get_table(tbl_name);
    free(col_name_cpy);


    // does the table exists already ?
    if (tbl == NULL) {
        // How come table does not exist ?
        return NULL;
    }

    if (tab != NULL) {
        *tab = tbl;
    }

    unsigned int i = 0;

    while (i < tbl->col_count)
    {
        column* coll = &(tbl->col[i]);
        if (strcmp(coll->name, col_name) == 0)
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
        int * tuples = NULL;
        char * val = strtok(header, ",");
        
        if (val == NULL) {
            continue;
        }
        
        n_tuples++;
        tuples[0] = atoi(val);
        int  n_cols = 0;
        char * tmp = NULL;
        // read line CSV into array 
        while ((tmp = strtok(NULL, ",")) != NULL) {
            tuples = realloc(tuples, ++n_cols*sizeof(int));
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

// applies all chained comparators and returns 1 if v matches
int compare(comparator* c, int v) {

    comparator * cur = c;
    int result = 0;
    Junction junc = NONE;

    while (cur ) {

        int subresult = ((cur->type & LESS_THAN)    && (v <  cur->p_val)) 
                     || ((cur->type & EQUAL)        && (v == cur->p_val)) 
                     || ((cur->type & GREATER_THAN) && (v >  cur->p_val));

        // now apply the last junction 
        if      (junc == AND) {  result &= subresult; } 
        else if (junc == OR)  {  result |= subresult; }
        else                  {result = subresult;    } // notmally in case we first start 
            
        junc = cur->mode;
        cur = cur->next_comparator;
    }

    return result;

}

char * tuple_results(result ** rs, int n_res) {


    char  buf[1024];
    // size_t buf_len = 1024;
    // unsigned int written = 0;

    // for (int i = 0; i < res->num_tuples; ++i){
    //     // make a CSV out ofevery column value
    //     int idx = res->payload[i];
    //     for (int j = 0; j < res->tbl->col_count; ++j){

    //         column * c = &(res->tbl->col[j]);

    //         written += snprintf(buf + written, buf_len - written, (j != 0 ? ", %u" : "%u"), c[idx]);
    //     }
    //     written += snprintf(buf + written, 2, "\n");
    // }
    return buf;

}



char * tuple_columns(column ** columns, int n_cols, int length) {


    char* buf = malloc(1024*sizeof(char));

    size_t buf_len = 1024;
    unsigned int written = 0;

    for (int i = 0; i < length; ++i){
        // make a CSV out ofevery column value
        for (int j = 0; j < n_cols; ++j){

            written += snprintf(buf + written, buf_len - written, (j != 0 ? ", %u" : "%u"), columns[j]->data[i]);
        }
        written += snprintf(buf + written, 2, "\n");
    }
    return buf;

}

status query_execute(db_operator* op, result* results){

    status ret;
    ret.code = OK;

    if (op->type == SELECT ) {

        // bring the column tuple
        int * tple = ((op->columns)[0])->data;
        
        if (tple == NULL) {
            // should return empty result
            return ret;
        }

        // calculate the length of the tuple, just check if it's working otherwise use the second line
        //int len = sizeof(tple)/sizeof(tple[0]); 
        int len = ((op->tables)[0])->length;

        int * data = malloc(sizeof(int));
        int cnt = 0;  // contains the number of matches

        for (int i = 0; i < len; ++i)
        {
            // loop and check if the value matches comparator
            if (compare(op->c, tple[i])) {
                // resize results array
                data = realloc(data, ++cnt*sizeof(int));
                // add i (the column index) to the list 
                data[cnt] = i;
            }
        }

        // store the num_tuples
        results->num_tuples = cnt;
        results->payload = data;

    }
    return ret;
}

int dbo_factory(db_operator ** dbo) {

    // allocate dbo
    (*dbo) = (db_operator*) malloc(sizeof(db_operator));

    if ((*dbo) == NULL) {
        return 0;
    }

    // initialize its DANGEROUS  variables
    (*dbo)->tables = NULL;
    (*dbo)->columns = NULL;
    (*dbo)->c = NULL;
    (*dbo)->store_name = NULL;


    return 1;
}



// should itirate and free sub items
void free_db_operator(db_operator* dbo ) {


    if (dbo == NULL) {
        return;
    }
    // free table dbl pointer array
    if (dbo->tables != NULL) {
        free(dbo->tables);
    }
    // free column dbl pointer array

    if (dbo->columns != NULL) {
        free(dbo->tables);
    }

    // free the comparator linked list
    comparator * curr = dbo->c;

    while (curr != NULL) {             
        curr = curr->next_comparator;        
        free (curr);                           
    }

} 
