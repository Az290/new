#ifndef TABLE_H
#define TABLE_H
int evaluate_condition(const char *condition, char *col_names[], char *col_types[], char *values[], int col_count);
char* create_table(const char *db_name, const char *table_name, const char *columns);
char* insert_into_table(const char *db_name, const char *table_name, const char *values);
char* drop_table(const char *db_name, const char *table_name);
char* show_table(const char *db_name);
char* view_table(const char *db_name, const char *table_name);
char* select_from_table(const char *db_name, const char * table_name, const char *columns, const char *condition);
char* delete_from_table(const char *db_name, const char *table_name, const char *condition);
char* update_table(const char *db_name, const char *table_name, const char *set_clause, const char *condition);
char* alter_table(const char *db_name, const char *table_name, const char *operation, const char *column_info, const char *new_name);
char* describe_table(const char *db_name, const char *table_name);
#endif

