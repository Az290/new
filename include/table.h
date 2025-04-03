#ifndef TABLE_H
#define TABLE_H

char* create_table(const char *db_name, const char *table_name, const char *columns);
char* insert_into_table(const char *db_name, const char *table_name, const char *values);
char* drop_table(const char *db_name, const char *table_name);
char* show_table(const char *db_name);
char* view_table(const char *db_name, const char *table_name);
char* select_from_table(const char *db_name, const char * table_name, const char *columns, const char *condition);
#endif

