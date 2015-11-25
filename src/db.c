#include "db.h"
#include <string.h>
#include "utils.h"
#include <unistd.h>

//kefta : db variable pool
db** db_pool = NULL;
size_t db_pool_size = 0;

// Here we maintain a global pool for storing variables
store* store_pool = NULL;
size_t store_pool_size = 0;


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
status create_table(const char* name, size_t num_columns, table ** rtbl) {

    status ret;

    // take a permanent copy of table_name
    char * tbl_name =  malloc(strlen(name)+1);
    strcpy(tbl_name, name);

    // extract db_name
    char* db_name = strtok((char* )name, ".");

    // 
    db* db1 = (db*)get_db(db_name);
    if (db1 == NULL) {
        log_info("%s Database does not exist\n", db_name);
        ret.code = ERROR;
        ret.message = "[ERROR] Database does not exist !!"; 
        free(tbl_name);
        return ret;
    }

	// check if the table exists and get the pointer to it
	if ( get_table(tbl_name) != NULL) {
		log_info("table %s already exists\n", tbl_name);
		ret.code = ERROR;
		ret.message = "Table already exists";
        free(tbl_name);
		return ret;
	 }

	// otherwise we need to allocate the tables array
	db1->table_count += 1;
	db1->tables = realloc(db1->tables, db1->table_count*sizeof(table));

	table * tbl = &(db1->tables[db1->table_count-1]);
    
    // fill in table values
	tbl->name = tbl_name;
    tbl->col_count = num_columns;
    tbl->col = malloc(num_columns*sizeof(column));
    tbl->length = 0;

    // initialize column names to NULL
    for (size_t i = 0; i < num_columns; ++i)
    {
    	(tbl->col[i]).name = NULL;
    }

    // return also table reference
    if (rtbl != NULL) {
        (*rtbl) = tbl;
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


status create_column(const char* col_name, unsigned int is_sorted, column** rcol){


    status s;
    s.message = NULL;
    s.code = OK;


    // extract the table name from col_name

    char  tmp[strlen(col_name)+1];
    tmp[0] = '\0';
    strncpy(tmp, col_name, strlen(col_name));
    char * db_name = strtok(tmp, ".");
    char * ttbl_name = strtok(NULL, ".");


    // generate the table name
    char tbl_name[strlen(db_name) + strlen(ttbl_name) + 2];
    tbl_name[0] = '\0'; // Fixes random allocation in MacOS
    strncat(tbl_name, db_name, strlen(db_name));
    strncat(tbl_name, ".", 1);
    strncat(tbl_name, ttbl_name, strlen(tbl_name));



    // get the original table* associated with <tbl_name>
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
		if (strcmp(coll->name, col_name) == 0)
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

    char * col_name_cpy = malloc(strlen(col_name)+1);
    strncpy(col_name_cpy, col_name, strlen(col_name)+1);

	column * col = &(tbl->col[i]);

    col->name =  col_name_cpy;
    col->data = NULL;
    col->index = NULL;

    // return the column reference
    if (rcol != NULL ) {
        (*rcol) = col;
    }

    return s;

}



status table_add_relational(table* table, int64_t* row, unsigned int size) {
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
        col->data = realloc(col->data, table->length*sizeof(int64_t));
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

//  Kefta: As the name suggests : Load  a csv file

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
        

        int  n_cols = 1; // minimum is 1 column

        int64_t * tuples = malloc(sizeof(int64_t));
        char * val = strtok(header, ",");
        
        if (val == NULL) {
            continue;
        }
        
        n_tuples++;
        tuples[0] = atoi(val);
        char * tmp = NULL;
        // read line CSV into array 
        while ((tmp = strtok(NULL, ",")) != NULL) {
            tuples = realloc(tuples, ++n_cols*sizeof(int64_t));
            tuples[n_cols-1] = atol(tmp);
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
int compare(comparator* c, int64_t v) {

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
        else                  {result = subresult;    } // notmally in case we first started 
            
        junc = cur->mode;
        cur = cur->next_comparator;
    }

    return result;

}



char * tuple_results(result ** rs, size_t n_res, size_t len) {


    char* buf = malloc(1024*sizeof(char));
    buf[0]='\0';

    size_t buf_len = 1024;
    unsigned int written = 0;

    // Assuming all results have the same length otherwise it will CRASH !!
    for (size_t i = 0; i < len; ++i){
        // make a CSV out of every column value
        for (size_t j = 0; j < n_res; ++j){  

            written += snprintf(buf + written, buf_len - written, (j != 0 ? ", %ld" : "%ld"), rs[j]->payload[i]);
        }
        if (i+1 < len) {
            written += snprintf(buf + written, 2, "\n");
        }
    }
    return buf;

}



char * tuple_columns(column ** columns, int n_cols, int length) {


    char* buf = malloc(1024*sizeof(char));

    size_t buf_len = 1024;
    unsigned int written = 0;

    for (int i = 0; i < length; ++i){
        // make a CSV out ofevery column value
        for (int j = 0; j < n_cols; ++j){

            written += snprintf(buf + written, buf_len - written, (j != 0 ? ", %ld" : "%ld"), columns[j]->data[i]);
        }
        if (i+1 < length) {
            written += snprintf(buf + written, 2, "\n");
        }
    }
    return buf;

}

status query_execute(db_operator* op){

    status ret;
    ret.code = OK;

    if (op->type == SELECT 
        && op->table != NULL
        && op->column != NULL 
        && op->store_name != NULL) {  // it's a SELECT  FROM table WHERE 

        // bring the column tuple
        int64_t * tple = (op->column)->data;
        
        if (tple == NULL) {
            // should return empty result
            return ret;
        }

        // calculate the length of the tuple, just check if it's working otherwise use the second line
        //int len = sizeof(tple)/sizeof(tple[0]); 
        int len = op->table->length;

        int64_t * data = NULL;
        int cnt = 0;  // contains the number of matches

        for (int i = 0; i < len; ++i)
        {
            // loop and check if the value matches comparator
            if (compare(op->c, tple[i])) {
                // resize results array
                data = realloc(data, ++cnt*sizeof(int64_t));
                // add i (the column index) to the list 
                data[cnt-1] = i;
            }
        }

        result* res = malloc(sizeof(result));
        // store the num_tuples
        res->num_tuples = cnt;
        res->payload = data;

        add_store_in_pool(op->store_name, res);


    } else if (op->type == SELECT    // FETCHSELECT operation
                && op->vec1 != NULL
                && op->vec2 != NULL
                && op->store_name != NULL) {

        // bring the column tuple
        int64_t * tple = op->vec2->payload;
        
        if (tple == NULL || op->vec2->num_tuples == 0) {
            // should return empty result
            return ret;
        }

        // calculate the length of the tuple, just check if it's working otherwise use the second line
        int len = op->vec2->num_tuples;

        int64_t * data = NULL;
        int cnt = 0;  // contains the number of matches

        for (int i = 0; i < len; ++i)
        {
            // loop and check if the value matches comparator
            if (compare(op->c, tple[i])) {
                // resize results array
                data = realloc(data, ++cnt*sizeof(int64_t));
                // add i (the column index) to the list 
                data[cnt-1] = op->vec1->payload[i];
            }
        }

        result* res = malloc(sizeof(result));
        // store the num_tuples
        res->num_tuples = cnt;
        res->payload = data;

        add_store_in_pool(op->store_name, res);



     } else if (op->type == PROJECT
            && op->column != NULL 
            && op->vec1 != NULL
            && op->store_name != NULL) {


        // bring the column tuple
        int64_t * tple = (op->column)->data;
        int64_t* data = malloc(op->vec1->num_tuples*sizeof(int64_t));

        // Assuming all results have the same length otherwise it will CRASH !!
        for (size_t i = 0; i < op->vec1->num_tuples; ++i){
            data[i] = tple[op->vec1->payload[i]]; //projection
        }

        result* res = malloc(sizeof(result));
        // store the num_tuples
        res->num_tuples = op->vec1->num_tuples;
        res->payload = data;    

        add_store_in_pool(op->store_name, res);

    }
    return ret;
}

int dbo_factory(db_operator ** dbo) {

    // allocate dbo
    (*dbo) = (db_operator*) malloc(sizeof(db_operator));

    if ((*dbo) == NULL) {
        return 0;
    }

    // initialize its DANGEROUS variables
    (*dbo)->table = NULL;
    (*dbo)->column = NULL;
    (*dbo)->c = NULL;
    (*dbo)->store_name = NULL;


    return 1;
}



// should itirate and free sub items
void free_db_operator(db_operator* dbo ) {


    if (dbo == NULL) {
        return;
    }

    // free the comparator linked list
    comparator * curr = dbo->c;

    while (curr != NULL) {             
        curr = curr->next_comparator;        
        free (curr);                           
    }

    // free the store name if exists
    if (dbo->store_name != NULL) {
        free(dbo->store_name);
    }

} 


// Kefta :  get result variable from db pool 
store* get_store(const char* name) {

    // search for the last entry
    for (size_t i = 0; i < store_pool_size; ++i)
    {
        if (strcmp(store_pool[i].name, name) == 0)
        {
            return &store_pool[i];
        }
    }
    // Nothing was found!
    return NULL;
}



status average_column(const char* col_name, const char* target_store){
    status ret;
    table *tab;
    column* col = get_column(&tab, col_name);
    if (col == NULL) {
        ret.code = ERROR;
        ret.message = "Column name does not exist";
        return ret;
    }


    if (tab->length==0) {
        ret.code = ERROR;
        ret.message = "ERROR";
        return ret;
    }

    int64_t sum = 0;
    int64_t* data = malloc(1*sizeof(int64_t));

    for (size_t i = 0; i < tab->length; ++i)
    {
        sum+=col->data[i];
    }
    data[0]  = (double)sum/tab->length;  
     //@168 as instructed we keep things simple int

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = 1;
    res->payload = data;    // here we reequest  store=fetch(..)

    add_store_in_pool(target_store, res);

    ret.code = OK;
    return ret;
}

status average_store(const char* store_name, const char* target_store) {
    status ret;

    store* stor = get_store(store_name);
    if (stor == NULL) {
        ret.code = ERROR;
        ret.message = "Store name does not exist";
        return ret;
    }
    result * rs = stor->data;

    if (rs->num_tuples==0) {
        ret.code = ERROR;
        ret.message = "ERROR";
        return ret;
    }

    int64_t sum = 0;
    int64_t* data = malloc(1*sizeof(int64_t));

    for (size_t i = 0; i < rs->num_tuples; ++i)
    {
        sum+=(int64_t)rs->payload[i];
    }
    data[0]  = (double)sum/rs->num_tuples;  
     //@168 as instructed we keep things simple int

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = 1;
    res->payload = data;    // here we reequest  store=fetch(..)

    add_store_in_pool(target_store, res);

    ret.code = OK;
    return ret;
}


status min_store(const char* store_name, const char* target_store) {
    status ret;

    store* stor = get_store(store_name);
    if (stor == NULL) {
        ret.code = ERROR;
        ret.message = "Store name does not exist";
        return ret;
    }
    result * rs = stor->data;

    if (rs->num_tuples==0) {
        ret.code = ERROR;
        ret.message = "ERROR";
        return ret;
    }

    int64_t min = rs->payload[0];
    int64_t* data = malloc(1*sizeof(int64_t));

    for (size_t i = 1; i < rs->num_tuples; ++i)
    {
        if (rs->payload[i] < min) {
            min = rs->payload[i];
        }
    }
    data[0]  = min;  
     //@168 as instructed we keep things simple int

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = 1;
    res->payload = data;    // here we request  store=fetch(..)

    add_store_in_pool(target_store, res);

    ret.code = OK;
    return ret;
}

status max_store(const char* store_name, const char* target_store) {
    status ret;

    store* stor = get_store(store_name);
    if (stor == NULL) {
        ret.code = ERROR;
        ret.message = "Store name does not exist";
        return ret;
    }
    result * rs = stor->data;

    if (rs->num_tuples==0) {
        ret.code = ERROR;
        ret.message = "ERROR";
        return ret;
    }

    int64_t max = rs->payload[0];
    int64_t* data = malloc(1*sizeof(int64_t));

    for (size_t i = 1; i < rs->num_tuples; ++i)
    {
        if (rs->payload[i] > max) {
            max = rs->payload[i];
        }
    }
    data[0]  = max;  
     //@168 as instructed we keep things simple int

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = 1;
    res->payload = data;    // here we reequest  store=fetch(..)

    add_store_in_pool(target_store, res);

    ret.code = OK;
    return ret;
}



status add_vectors(result ** rs, size_t n_res, size_t len, const char* target_store_name){

    status ret;

    // TODO: Assuming all results have the same length otherwise it will CRASH !!
    int64_t* target_sum = (int64_t*) malloc(len*sizeof(int64_t));

    for (size_t i = 0; i < len; ++i){
        int64_t sm = 0;
        for (size_t j = 0; j < n_res; ++j){  
            sm += rs[j]->payload[i];
        }
        target_sum[i] = sm;
    }

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = len;
    res->payload = target_sum;    // here we reequest  store=fetch(..)

    add_store_in_pool(target_store_name, res);
    return ret;
}


status sub_vectors(result * rs1, result * rs2, size_t len, const char* target_store_name){

    status ret;

    // TODO: Assuming all results have the same length otherwise it will CRASH !!
    int64_t* target_sub = (int64_t*) malloc(len*sizeof(int64_t));

    for (size_t i = 0; i < len; ++i){
        target_sub[i] = rs1->payload[i] - rs2->payload[i];
    }

    result* res = malloc(sizeof(result));
    // store the num_tuples
    res->num_tuples = len;
    res->payload = target_sub;    // here we reequest  store=fetch(..)

    add_store_in_pool(target_store_name, res);
    return ret;
}









void add_store_in_pool(const char* name, result* res) {
    // So let' store the result into store pool
    // first thing is allocate memory
    store_pool = realloc(store_pool, ++store_pool_size*sizeof(store)); 

    // needed to keep a copy of the store nam.
    char* target_store_name = malloc(strlen(name)+1);
    strncpy(target_store_name, name, strlen(name) + 1);
  
    
    // then copy params
    store_pool[store_pool_size-1].name = target_store_name;
    store_pool[store_pool_size-1].data = res;


}


status persist_column_data(column* col, size_t length) {


    status ret;
    // creating the column data filename
    uint32_t col_fname_length = strlen(col->name) + 8;
    char colfn[col_fname_length];  //append .coldata extension to column name
    colfn[0] = '\0';
    strncat(colfn, col->name, strlen(col->name));
    strncat(colfn, ".coldata", 8);

    // try open the column file otherwise create it

    FILE *ptr_colfile;

    ptr_colfile=fopen(colfn,"wb");
    if (!ptr_colfile)
    {
        ret.code = ERROR;
        ret.message = "Unable to create column file!";

        return ret;
    }  
    fwrite(&length, sizeof(size_t), 1,ptr_colfile);
    fwrite(col->data, sizeof(int64_t), length,ptr_colfile);

    fclose(ptr_colfile);

    ret.code = OK;
    return ret; 

}


status load_column_data(column* col, size_t * length) {


    status ret;
    // creating the column data filename
    uint32_t col_fname_length = strlen(col->name) + 8;
    char colfn[col_fname_length];  //append .coldata extension to column name
    colfn[0] = '\0';
    strncat(colfn, col->name, strlen(col->name));
    strncat(colfn, ".coldata", 8);

    // try open the column file otherwise create it

    FILE *ptr_colfile;

    ptr_colfile=fopen(colfn,"rb");
    if (!ptr_colfile)
    {
        ret.code = ERROR;
        ret.message = "Unable to create column file!";

        return ret;
    }


    fread(length, sizeof(size_t), 1,ptr_colfile);
    // TODO : protect memory from random size allocation :)
    col->data = malloc((*length)*sizeof(int64_t));

    fread(col->data, sizeof(int64_t), (*length),ptr_colfile);

    fclose(ptr_colfile);

    ret.code = OK;
    return ret; 

}


// sync db to disk
status sync_to_disk(){

   status ret;


    // the structure is :
    // 1 -  db1.schema  : contains the description of database tables and columns [db_name_length: uint<4>, db_name: char<1024>, tables_count: uint<4>][table][table]..[col][col] ...
    //                table header = [table_name char<1024>][number of columns: uint<4>][length : uint<4>] 
    // 2-   db1.tbl1.col1.data : contains col1 data [header][data]
    //                          header contains : [col_name : char<1024>][data....]

    //int cursor = 0;
    FILE *ptr_schemafile;

    ptr_schemafile=fopen(".schema","wb");
    if (!ptr_schemafile)
    {
        ret.code = ERROR;
        ret.message = "Unable to create schema file!";
        return ret;
    }

    struct hfschema hdr;


    // write the databases
    for (size_t i = 0; i < db_pool_size; ++i)
    {
        db* dbi = db_pool[i];

        hdr.type = DB;  
        hdr.length = strlen(dbi->name) ;
        
        // write the header : [DB][LENGTH]
        fwrite(&hdr, sizeof(struct hfschema), 1,ptr_schemafile);
        // write the db name string 
        fwrite(dbi->name, strlen(dbi->name), 1, ptr_schemafile);
        // then insert tables 


        for (size_t j = 0; j < dbi->table_count; ++j)
        {
            struct hfschema thdr;

            table* tblj = &(dbi->tables[j]);

            thdr.type = TABLE; 
            thdr.length = 2*sizeof(uint32_t) + strlen(tblj->name);  // payload length
            // write the header : [TABLE][LENGTH]
            fwrite(&thdr, sizeof(struct hfschema), 1,ptr_schemafile);
            // write the payload :  [COL_COUNT][LENGTH][TABLE_NAME] 
            fwrite(&(tblj->col_count), sizeof(uint32_t), 1, ptr_schemafile);            
            fwrite(&(tblj->length), sizeof(uint32_t), 1, ptr_schemafile);            
            fwrite(tblj->name, strlen(tblj->name), 1, ptr_schemafile);            

            // then insert columns

            for (size_t k = 0; k < tblj->col_count; ++k)
            {
                struct hfschema chdr;

                column* colk = &(tblj->col[k]);

                chdr.type = COLUMN; 
                chdr.length = strlen(colk->name) + sizeof(uint8_t) ;  // payload length
                // write the header : [COLUMN][LENGTH]
                fwrite(&chdr, sizeof(struct hfschema), 1,ptr_schemafile);
                // write the payload :  [IS_SORTED][COLUMN_NAME]
                uint8_t is_sorted = 0; // TODO: retrieve the value from col.is_sorted 
                // write the value  
                fwrite(&is_sorted, sizeof(uint8_t), 1, ptr_schemafile);            
                // after that write the column name
                fwrite(colk->name, strlen(colk->name), 1, ptr_schemafile);

                // create each column's data file.
                status cret = persist_column_data(colk, tblj->length);
                (void) cret;

            }            
        }
    }


    hdr.type = END;  
    hdr.length = 0 ;
    fwrite(&hdr, sizeof(struct hfschema), 1,ptr_schemafile);
    
    fclose(ptr_schemafile);

    ret.code = OK;
    return ret;  
}


// sync db to disk
status load_from_disk(){

    status ret;
    ret.code = OK;

    FILE *ptr_schemafile;

    ptr_schemafile=fopen(".schema","rb");
    if (!ptr_schemafile)
    {
        ret.code = ERROR;
        ret.message = "Unable to load .schema file!";
        return ret;
    }

    struct hfschema hdr;

    while(1) {
        
        if (0 == fread(&hdr, sizeof(struct hfschema), 1, ptr_schemafile)) {
            ret.code = ERROR;
            break;
        }



        if (hdr.type == DB) {
            // read the next hdr.length bytes as the name of the DB
            // memory leak attack :)
            // if (hdr.length>1024) {
            //     hdr.length = 1024;
            // }
            char db_name[hdr.length+1];
            fread(&db_name, sizeof(char), hdr.length, ptr_schemafile);
            db_name[hdr.length] = '\0';

            status rr = create_db(db_name);
            (void)rr;


        } else if (hdr.type == TABLE) {

            uint32_t col_count = 0, leng = 0;
            // read the number of columns 
            fread(&col_count, sizeof(uint32_t), 1, ptr_schemafile);            
            // read the table length
            fread(&leng, sizeof(uint32_t), 1, ptr_schemafile);            

            //read the table name
            size_t tbl_len = hdr.length-2*sizeof(uint32_t);
            char tbl_name[tbl_len+1];
            fread(&tbl_name, sizeof(char), tbl_len, ptr_schemafile);
            tbl_name[tbl_len] = '\0';


            table * tbl = NULL;
            status rr = create_table(tbl_name, col_count, &tbl);


            if (rr.code != OK) {
                ret.code = rr.code;
                ret.message = "Corrupted database storage";
                break;
            }

            // set the table length
            tbl->length = leng;


        } else if (hdr.type == COLUMN) {

            uint8_t is_sorted = 0;
            // read the column sorted 
            fread(&is_sorted, sizeof(uint8_t), 1, ptr_schemafile);            

            // read the column full name in the format db1.tbl1.col1
            size_t col_len = hdr.length-sizeof(uint8_t);
            char col_name[col_len+1];
            fread(&col_name, sizeof(char), col_len, ptr_schemafile);
            col_name[col_len] = '\0';
            
            column *col;
            status rr = create_column(col_name, is_sorted, &col);
            
            if (rr.code != OK) {
                ret.code = rr.code;
                ret.message = "Corrupted database storage";
                break;
            }

            size_t length = 0;

            rr = load_column_data(col, &length);

        } else if (hdr.type == END) {
            break;
        } 
    }

    fclose(ptr_schemafile);
    return ret;  
}


status graceful_shutdown(){

    status ret;

    // sync db to disk
    ret =  sync_to_disk();

    ret.code = OK;
    return ret; 




}


