#include "dsl.h"

// Create Commands

// Matches: comment line starting with -- ;
const char* shutdown_command = "^[\\t]*shutdown[\\t]*";

// Matches: comment line starting with -- ;
const char* comment_line = "^--.*";

// Matches: create(db, <db_name>);
const char* create_db_command = "^create\\(db\\,[\\ ]*\"[a-zA-Z0-9_]+\"[\\ ]*\\)";

// Matches: create(table, <table_name>, <db_name>, <col_count>);
const char* create_table_command = "^create\\(tbl\\,[\\t]*\"[a-zA-Z0-9_\\.]+\"\\,[\\t]*[a-zA-Z0-9_\\.]+\\,[\\t]*[0-9]+\\)";

// Matches: create(col, <col_name>, <tbl_var>, sorted);
const char* create_col_command_sorted = "^create\\(col\\,[\\t]*\"[a-zA-Z0-9_\\.]\"+,[\\t]*[a-zA-Z0-9_\\.]+\\,[\\t]*sorted)";

// Matches: load(<file_name>);
const char* load_command = "^load\\([\\t]*\"[\\t]*[-a-zA-Z0-9._/]+[\\t]*\"[\\t]*)";

// Matches: create(col, <col_name>, <tbl_var>, unsorted);
const char* create_col_command_unsorted = "^create\\(col\\,[\\t]*\"[a-zA-Z0-9_\\.]+\"\\,[\\t]*[a-zA-Z0-9_\\.]+\\,[\\t]*unsorted)";

// Matches: relational_insert(<db_name>.<table_name>, int ... )
const char* relational_insert_command = "^relational_insert\\([\\t]*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_\\.]+(\\,[\\t]*[0-9-]+)+)";

// Matches: select(<col_name>, <min> , <max>)
const char* select_between_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*select\\([\\t]*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,[\\t]*(null|[0-9-]+)[\\t]*\\,[\\t]*(null|[0-9-]+)[\\t]*)";

// Matches: select(<vec1>, <vec2>,  <min> , <max>)
const char* select_fetch_between_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*select\\([\\t]*[a-zA-Z0-9_]+[\\t]*\\,[\\t]*[a-zA-Z0-9_]+[\\t]*\\,[\\t]*(null|[0-9-]+)[\\t]*\\,[\\t]*(null|[0-9-]+)[\\t]*)";


// Matches: tuple(<variable1>, <variable2>, ..);
const char* tuple_variable_command = "^tuple\\([\t]*[a-zA-Z0-9_]+[\t]*(\\,[\t]*[a-zA-Z0-9_]+[\t]*)*\\)";

// Matches: tuple(<column1>, <column2>, ..);
const char* tuple_column_command = "^tuple\\([\t]*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+[\t]*(\\,[\t]*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+[\t]*)*\\)";

// Matches: fetch(<col_name>, <store_name>);
const char* fetch_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*fetch\\([\\t]*[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+[\\t]*\\,[\\t]*[a-zA-Z0-9_]+[\\t]*\\)";

// Matches: avg(<variable1>);  matches columns also.
const char* avg_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*avg\\([\t]*[.a-zA-Z0-9_]+[\t]*\\)";

// Matches: add(<variable1>); TODO : match columns
const char* add_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*add\\([\t]*[a-zA-Z0-9_]+[\t]*(\\,[\t]*[a-zA-Z0-9_]+[\t]*)+\\)";

// Matches: sub(<variable1>, <variable2>); TODO : match columns
const char* sub_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*sub\\([\t]*[a-zA-Z0-9_]+[\t]*\\,[\t]*[a-zA-Z0-9_]+[\t]*\\)";

// Matches: min(<variable1>); matches columns also.
const char* min_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*min\\([\t]*[.a-zA-Z0-9_]+[\t]*\\)";

// Matches: max(<variable1>); matches columns also.
const char* max_command = "^[a-zA-Z0-9_]+[\\t]*\\=[\\t]*max\\([\t]*[.a-zA-Z0-9_]+[\t]*\\)";



// TODO(USER): You will need to update the commands here for every single command you add.
dsl** dsl_commands_init(void)
{
    dsl** commands = calloc(NUM_DSL_COMMANDS, sizeof(dsl*));

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        commands[i] = malloc(sizeof(dsl));
    }

    // Assign the create commands
    commands[0]->c = create_db_command;
    commands[0]->g = CREATE_DB;

    commands[1]->c = create_table_command;
    commands[1]->g = CREATE_TABLE;

    commands[2]->c = create_col_command_sorted;
    commands[2]->g = CREATE_COLUMN;

    commands[3]->c = create_col_command_unsorted;
    commands[3]->g = CREATE_COLUMN;

    commands[4]->c = relational_insert_command;
    commands[4]->g = RELATIONAL_INSERT;

    commands[5]->c = select_between_command;
    commands[5]->g = SELECT_BETWEEN;

    commands[6]->c = load_command;
    commands[6]->g = LOAD_FILE;

    commands[7]->c = tuple_column_command;
    commands[7]->g = TUPLE_COLUMN_COMMAND;

    commands[8]->c = tuple_variable_command;
    commands[8]->g = TUPLE_VARIABLE_COMMAND;
    
    commands[9]->c = fetch_command;
    commands[9]->g = FETCH_COMMAND;
    
    commands[10]->c = comment_line;
    commands[10]->g = COMMENT;

    commands[11]->c = shutdown_command;
    commands[11]->g = SHUTDOWN;

    commands[12]->c = avg_command;
    commands[12]->g = AVG_COMMAND;

    commands[13]->c = min_command;
    commands[13]->g = MIN_COMMAND;

    commands[14]->c = max_command;
    commands[14]->g = MAX_COMMAND;

    commands[15]->c = add_command;
    commands[15]->g = ADD_COMMAND;

    commands[16]->c = sub_command;
    commands[16]->g = SUB_COMMAND;

    commands[17]->c = select_fetch_between_command;
    commands[17]->g = SELECTFETCH_BETWEEN;

    return commands;
}
