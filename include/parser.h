#ifndef PARSER_H
#define PARSER_H
typedef struct {
    int socket;
    char current_db[256];
} ClientSession;
char* execute_command(const char *input, ClientSession *session);
#endif


