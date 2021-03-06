// dsl.h
//
// CS165, Fall 2015
//
// This file defines some of the regular expressions to match the incoming DSL.
// For now, we implement regular expressions to match the create functions for
// you, and you are free to update this for all the other DSL commands.

#ifndef DSL_H__
#define DSL_H__

#include <stdlib.h>

// Currently we have 4 DSL commands to parse.
// TODO(USER): you will need to increase this to track the commands you support.
#define NUM_DSL_COMMANDS (18)

// This helps group similar DSL commands together.
// For example, some queries can be parsed together:
//
// create(col, <col_name>, <tbl_name>, unsorted)
// create(col, <col_name>, <tbl_name>, sorted)
//
// and
//
// p = select(col1, 5, 5) (point query)
// p = select(col1, 5, 10) (range query)
typedef enum DSLGroup {
    SHUTDOWN,
    COMMENT,
    CREATE_DB,
    CREATE_TABLE,
    CREATE_COLUMN,
    LOAD_FILE,
    RELATIONAL_INSERT,
    SELECT_BETWEEN,
    SELECTFETCH_BETWEEN,
    TUPLE_VARIABLE_COMMAND,
    TUPLE_COLUMN_COMMAND,
    FETCH_COMMAND,
    AVG_COMMAND,
    MIN_COMMAND,
    MAX_COMMAND,
    ADD_COMMAND,
    SUB_COMMAND

    // TODO(USER): Add more here...
} DSLGroup;

// A dsl is defined as the DSL listed on the project website.
// We use this to track the relevant string to parse, and its group.
typedef struct dsl {
    const char* c;
    DSLGroup g;
} dsl;

// This returns an array of all the DSL commands that you can match with.
dsl** dsl_commands_init(void);

// We define these in the dsl.c file.
// TODO(USER) Add more extern strings for new commands
extern const char* comment_line;
extern const char* create_db_command;
extern const char* create_table_command;
extern const char* create_col_command_sorted;
extern const char* create_col_command_unsorted;
extern const char* insert_relational_command;
extern const char* select_between_command;
extern const char* selectfetch_between_command;
extern const char* load_command;
extern const char* tuple_variable_command;
extern const char* tuple_column_command;
extern const char* fetch_command;
extern const char* shutdown_command;
extern const char* avg_command;
extern const char* add_command;
extern const char* min_command;
extern const char* max_command;
#endif // DSL_H__
