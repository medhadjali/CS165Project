#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>

#include "db.h"

// Trims the whitespace
char* trim(char *str);



// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);

// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
status parse_command_string(char* str, dsl** commands, db_operator* op)
{
    log_info("Parsing: %s", str);

    // Trim the string of any spaces.
    char* trim_str = trim(str);

    // Create a regular expression to parse the string
    regex_t regex;
    int ret;

    // Track the number of matches; a string must match all
    int n_matches = 1;
    regmatch_t m;

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        dsl* d = commands[i];
        if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
            log_err("Could not compile regex\n");
        }

        // Bind regular expression associated with the string
        ret = regexec(&regex, trim_str, n_matches, &m, 0);

        // If we have a match, then figure out which one it is!
        if (ret == 0) {
            log_info("Found Command: %d\n", i);
            // Here, we actually strip the command as appropriately
            // based on the DSL to get the variable names.
            return parse_dsl(trim_str, d, op);
        }
    }

    // Nothing was found!
    status s;
    s.code = ERROR;
    s.message = "[ERROR] Unkown command, check DSL syntax";
    return s;
}

status parse_dsl(char* str, dsl* d, db_operator* op)
{
    // Use the delimiters to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char delimiter[2] = ",";
    // char end_line[2] = "\n";
    char eq_sign[2] = "=";
    char apos[2] = "\"";

    status ret;
    ret.message = NULL;
    ret.code = COMPLETE;
;

    if        (d->g == CREATE_DB) {
        // Create a working copy, +1 for '\0'      
        char* str_cpy = malloc(strlen(str));
        strncpy(str_cpy, str, strlen(str));

        // This gives us everything inside the (db, <db_name>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "db", but we don't need to use it
        char* db_indicator = strtok(args, delimiter);
        (void) db_indicator;

        // This gives us <db_name>
        char* db_name = trim(strtok(NULL, delimiter)); //BUG correction to consider trim spaces in given db_name


        log_info("create_db(%s)\n", db_name);

        // Here, we can create the DB using our parsed info!

        status s = create_db(db_name);
        if (s.code != OK) {
            // Something went wrong 
            ret.code = s.code;
            ret.message = s.message;
        }
    } else if (d->g == CREATE_TABLE) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "table"
        char* tbl_indicator = strtok(args, delimiter);
        (void) tbl_indicator;

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, delimiter);

        // This gives us <db_name>, we will need this to create the full name
        char* db_name = strtok(NULL, delimiter);

        // Generate the full name using <db_name>.<tbl_name>
        char full_name[strlen(tbl_name) + strlen(db_name)+2];
        full_name[0] = '\0'; // Fixes random allocation in MacOS
        strncat(full_name, db_name, strlen(db_name));
        strncat(full_name, ".", 1);
        strncat(full_name, tbl_name, strlen(tbl_name));


        // This gives us count
        char* count_str = strtok(NULL, delimiter);
        int count = 0;
        if (count_str != NULL) {
            count = atoi(count_str);
        }
        (void) count;



        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);

        // Here, we can create the table using our parsed info!

        status s = create_table(db_name, full_name, count);
        if (s.code != OK) {
            // Something went wrong
            ret.code = s.code;
            ret.message = s.message;
        }

        // Free the str_cpy
        free(str_cpy);

        // No db_operator required, since no query plan
    } else if (d->g == CREATE_COLUMN) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "col"
        char* col_indicator = strtok(args, delimiter);
        (void) col_indicator;

        // This gives us <col_name>, we will need this to create the full name
        char* col_name = strtok(NULL, delimiter);

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, delimiter);

        // Generate the full name using <db_name>.<tbl_name>
        char full_name[strlen(tbl_name) + strlen(col_name) + 2];
        full_name[0] = '\0'; // Fixes random allocation in MacOS
        strncat(full_name, tbl_name, strlen(tbl_name));
        strncat(full_name, ".", 1);
        strncat(full_name, col_name, strlen(col_name)+1);
        

        // char* col_full_name = malloc(strlen(full_name));
        // strncpy(col_full_name, full_name, strlen(full_name));

        // This gives us the "unsorted"
        char* sorting_str = strtok(NULL, delimiter);
        (void) sorting_str;

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Here, we can create the column using our parsed info!

        status s = create_column(tbl_name, full_name);
        if (s.code != OK) {
            // Something went wrong
            ret.code = s.code;
            ret.message = s.message;

        }

        free(str_cpy);

    } else if (d->g == RELATIONAL_INSERT) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (<db_name>.<table_name>, int ... )
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        // Create a working copy, +1 for '\0'
        char* args_cpy = malloc(strlen(args));
        strncpy(args_cpy, args, strlen(args));

        // This gives us <tbl_name>, we will need this to find the table*
        char* tbl_name = strtok(args, delimiter);

        // find the table*
        table* tbl1 = get_table(tbl_name);
        if (tbl1 == NULL) {

            free(str_cpy);

            ret.code = ERROR;
            ret.message = "[ERROR] Table does not exist";
            return ret;
        }

        int row[tbl1->col_count];
        strtok(args_cpy, delimiter);
        // fill a row 
        for (size_t i = 0; i < tbl1->col_count; ++i)
        {
            char * token = strtok(NULL, delimiter);
            row[i] = (token == NULL)?0:atoi(token);
        }

        // add it to column
        status s =  table_add_relational(tbl1, row, tbl1->col_count);


        if (s.code != OK) {
            // Something went wrong
            ret.code = s.code;
            ret.message = s.message;
        }

        // Free the str_cpy
        free(str_cpy);
        free(args_cpy);


        // No db_operator required, since no query plan
    } else if (d->g == SELECT_BETWEEN) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (<db_name>.<table_name>, int ... )
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        strtok(str_cpy, eq_sign);
        char * tmp = trim(str_cpy);
        char* store_name = malloc(strlen(tmp)+1);
        strncpy(store_name, tmp, strlen(tmp) + 1);
        
        // This gives us <tbl_name>, we will need this to find the table*
        char* col_name = strtok(args, delimiter);
        char* min_str = strtok(NULL, delimiter);
        char* max_str = strtok(NULL, delimiter);


        int min = (min_str == NULL)?0:atoi(min_str);
        int max = (max_str == NULL)?0:atoi(max_str);

        
        table* tbl = NULL;

        // search the column inside the table
        column* col = get_column(&tbl, col_name);

        if (col == NULL){

            free(str_cpy);
            status s;
            s.code = ERROR;
            s.message = "[ERROR] Column does not exist";
            return s;
        }

        // Free the str_cpy
        free(str_cpy);

        // now we have all elements, we can fill in the dbo
        // let's start by considering only one table and column
        // TODO : go back when queries become more complex and make this handle more tables and columns at the same time
        
        op->type = SELECT;
        op->tables = malloc(1*sizeof(table*));
        (op->tables)[0] = tbl;
        op->columns = malloc(1*sizeof(column*));
        (op->columns)[0] = col;
        
        comparator* c_min = malloc(sizeof(comparator));

        c_min->p_val = min;
        c_min->type = GREATER_THAN | EQUAL;
        c_min->col = col;
        c_min->mode = AND;

        comparator* c_max = malloc(sizeof(comparator));

        c_max->p_val = max;
        c_max->type = LESS_THAN | EQUAL;
        c_max->col = col;
        c_max->next_comparator = NULL;

        c_min->next_comparator = c_max;

        op->c = c_min;
        op->store_name = store_name;

        ret.code = OK;
    } else if (d->g == LOAD_FILE) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (<db_name>.<table_name>, int ... )
        strtok(str_cpy, apos);
        char* fname = trim(strtok(NULL, apos));

        status s = load_file(fname);
        if (s.code != OK) {
            ret.code = s.code;
            ret.message = s.message;
        }
    } else if (d->g == TUPLE_COLUMN_COMMAND) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us the column names inside () separated by comma
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        char* scols = trim(strtok(args, open_paren));

        int  n_cols = 0;
        char * tmp = strtok(scols, ",");
        column ** cols = NULL;
        // read line CSV into array 
        // maintains the current column length
        unsigned int clen = 0; 

        do {
            trim(tmp);
            // search the column inside the table
            table* tabl = NULL;

            column* col = get_column(&tabl, tmp);
            if (col == NULL) {
                free(str_cpy);
                free(cols);
                ret.code = ERROR;
                ret.message = "Column does not exist";
                return ret;
            }

            // fill in the columns length
            // Unfortunately I cannot calculate the length of data. we do bookkeeping ourselves
            if (clen==0) {
                clen = tabl->length;
            } else {
                if (clen > tabl->length) {
                    // keep the minimum safe length ! this should not happen ! but who knows :)
                    // less information is better than crashing on overflow
                    clen = tabl->length;
                }
            }
            cols = realloc(cols, ++n_cols*sizeof(column*));
            cols[n_cols-1] = col;
        } while ((tmp = strtok(NULL, ",")) != NULL);

        char * out = tuple_columns(cols, n_cols, clen);
        ret.code = COMPLETE;
        ret.message = out;

        // Free the str_cpy
        free(str_cpy);

    } else if (d->g == TUPLE_VARIABLE_COMMAND) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us the column name inside ()
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);
        char* col_name = trim(strtok(args, open_paren));



    } else {

        // No match ..

        ret.code = ERROR;
        ret.message = "Unhandled operation exception";
    }

    return ret;

}

// Trims the whitespace
char* trim(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = 0;
    return str;
}



