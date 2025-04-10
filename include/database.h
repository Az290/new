#ifndef DATABASE_H
#define DATABASE_H


char* create_database(const char *dbname);
char* show_database();
char* drop_database(const char *dbname);
char* rename_database(const char *old_dbname, const char *new_dbname);
char* check_database_exists(const char *dbname);
#endif

