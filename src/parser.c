#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "../include/parser.h"
#include "../include/database.h"
#include "../include/table.h"
#include <stddef.h>
#include <json-c/json.h>

typedef char* (*CommandFunc)(int argc, char **argv, ClientSession *session);
typedef struct {
    char *command;
    CommandFunc func;
} Command;

// Hàm trim để loại bỏ khoảng trắng thừa
static void trim(char *str) {
    char *start = str;
    while (*start == ' ') start++;
    char *end = str + strlen(str) - 1;
    while (end >= start && *end == ' ') *end-- = '\0';
    memmove(str, start, strlen(start) + 1);
}

// ==== Hàm kiểm tra session->current_db ====
static char* check_session_db(ClientSession *session) {
    if (strlen(session->current_db) == 0) {
        return strdup("{\"error\": \"Chua chon database!\"}");
    }
    return NULL;
}

// ==== Các hàm xử lý cú pháp ====
static char** handle_select(const char *input, int *argc) {
    char **args = (char **)calloc(10, sizeof(char *)); // Khởi tạo mảng với NULL
    if (!args) {
        char *error = strdup("{\"error\": \"Khong the cap phat bo nho cho args.\"}");
        args = (char **)malloc(sizeof(char *));
        args[0] = error;
        *argc = 1;
        return args;
    }
    *argc = 0;

    // Debug: In câu lệnh đầu vào
    printf("DEBUG: handle_select input: '%s'\n", input);

    // Thêm "select" vào args
    args[(*argc)++] = strdup("select");

    // Tìm từ khóa "from"
    char *from_pos = strstr(input, "from");
    if (!from_pos) {
        args[0] = strdup("{\"error\": \"Khong tim thay 'from' trong cau lenh SELECT!\"}");
        *argc = 1;
        return args;
    }

    // Lấy danh sách cột (từ sau "select" đến trước "from")
    size_t col_len = from_pos - (input + strlen("select") + 1);
    char *columns = (char *)malloc(col_len + 1);
    if (!columns) {
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho danh sach cot.\"}");
        *argc = 1;
        return args;
    }
    strncpy(columns, input + strlen("select") + 1, col_len);
    columns[col_len] = '\0';
    trim(columns);
    if (strlen(columns) == 0) {
        free(columns);
        args[0] = strdup("{\"error\": \"Can chi dinh danh sach cot sau SELECT.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = columns;
    printf("DEBUG: Parsed columns: '%s'\n", columns);

    // Lấy tên bảng (từ sau "from" đến trước "where" hoặc cuối chuỗi)
    char *table_start = from_pos + 5; // Bỏ qua "from "
    while (*table_start == ' ') table_start++; // Bỏ qua khoảng trắng
    char *table_end = strstr(table_start, "where");
    if (!table_end) table_end = (char *)(input + strlen(input)); // Nếu không có "where", lấy đến cuối chuỗi
    size_t table_len = table_end - table_start;
    char *table_name = (char *)malloc(table_len + 1);
    if (!table_name) {
        free(columns);
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten bang.\"}");
        *argc = 1;
        return args;
    }
    strncpy(table_name, table_start, table_len);
    table_name[table_len] = '\0';
    trim(table_name);
    if (strlen(table_name) == 0) {
        free(columns);
        free(table_name);
        args[0] = strdup("{\"error\": \"Can chi dinh ten bang sau 'from'!\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = table_name;
    printf("DEBUG: Parsed table name: '%s'\n", table_name);

    // Lấy điều kiện WHERE (nếu có)
    char *where_pos = strstr(input, "where");
    if (where_pos) {
        args[(*argc)++] = strdup("where");

        char *cond_start = where_pos + 6; // Bỏ qua "where "
        while (*cond_start == ' ') cond_start++; // Bỏ qua khoảng trắng
        size_t cond_len = strlen(cond_start);
        char *condition = (char *)malloc(cond_len + 1);
        if (!condition) {
            free(columns);
            free(table_name);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho dieu kien.\"}");
            *argc = 1;
            return args;
        }
        strncpy(condition, cond_start, cond_len);
        condition[cond_len] = '\0';
        trim(condition);
        if (strlen(condition) == 0) {
            free(columns);
            free(table_name);
            free(condition);
            args[0] = strdup("{\"error\": \"Dieu kien WHERE khong duoc de trong.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = condition;
        printf("DEBUG: Parsed condition: '%s'\n", condition);
    } else {
        printf("DEBUG: No WHERE clause found\n");
    }

    printf("DEBUG: handle_select argc: %d\n", *argc);
    for (int i = 0; i < *argc; i++) {
        printf("DEBUG: args[%d]: '%s'\n", i, args[i]);
    }

    return args;
}

static char** handle_delete(const char *input, int *argc) {
    char **args = (char **)calloc(10, sizeof(char *)); // Khởi tạo mảng với NULL
    if (!args) {
        char *error = strdup("{\"error\": \"Khong the cap phat bo nho cho args.\"}");
        args = (char **)malloc(sizeof(char *));
        args[0] = error;
        *argc = 1;
        return args;
    }
    *argc = 0;

    char buffer[256];
    strncpy(buffer, input, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    char *token = strtok(buffer, " ");
    if (!token) {
        args[0] = strdup("{\"error\": \"Khong co lenh nao duoc nhap.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = strdup("delete"); // "delete"

    token = strtok(NULL, " ");
    if (!token || strcmp(token, "from") != 0) {
        args[0] = strdup("{\"error\": \"Cu phap DELETE phai co 'from' sau DELETE.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = strdup("from");

    token = strtok(NULL, " ");
    if (!token) {
        args[0] = strdup("{\"error\": \"Can chi dinh ten bang sau 'from'.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = strdup(token);

    char *where_pos = strstr(input, "where");
    if (where_pos) {
        token = strtok(NULL, " ");
        if (!token || strcmp(token, "where") != 0) {
            args[0] = strdup("{\"error\": \"Cu phap WHERE khong hop le.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = strdup("where");

        char *cond_start = where_pos + 6; // Bỏ qua "where "
        while (*cond_start == ' ') cond_start++; // Bỏ qua khoảng trắng
        size_t cond_len = strlen(cond_start);
        char *condition = (char *)malloc(cond_len + 1);
        if (!condition) {
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho dieu kien.\"}");
            *argc = 1;
            return args;
        }
        strncpy(condition, cond_start, cond_len);
        condition[cond_len] = '\0';
        trim(condition);
        if (strlen(condition) == 0) {
            free(condition);
            args[0] = strdup("{\"error\": \"Dieu kien WHERE khong duoc de trong.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = condition;
    }

    return args;
}

// handle update
static char** handle_update(const char *input, int *argc) {
    char **args = (char **)calloc(10, sizeof(char *));
    if (!args) {
        char *error = strdup("{\"error\": \"Khong the cap phat bo nho cho args.\"}");
        args = (char **)malloc(sizeof(char *));
        args[0] = error;
        *argc = 1;
        return args;
    }
    *argc = 0;

    // Debug: In câu lệnh đầu vào
    printf("DEBUG: handle_update input: '%s'\n", input);

    // Thêm "update" vào args
    args[(*argc)++] = strdup("update");

    // Tìm từ khóa "set"
    char *set_pos = strstr(input, "set");
    if (!set_pos) {
        args[0] = strdup("{\"error\": \"Khong tim thay 'set' trong cau lenh UPDATE!\"}");
        *argc = 1;
        return args;
    }

    // Lấy tên bảng (từ sau "update" đến trước "set")
    size_t table_len = set_pos - (input + strlen("update") + 1);
    char *table_name = (char *)malloc(table_len + 1);
    if (!table_name) {
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten bang.\"}");
        *argc = 1;
        return args;
    }
    strncpy(table_name, input + strlen("update") + 1, table_len);
    table_name[table_len] = '\0';
    trim(table_name);
    if (strlen(table_name) == 0) {
        free(table_name);
        args[0] = strdup("{\"error\": \"Can chi dinh ten bang sau UPDATE.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = table_name;
    printf("DEBUG: Parsed table name: '%s'\n", table_name);

    // Thêm "set" vào args
    args[(*argc)++] = strdup("set");

    // Lấy set_clause (từ sau "set" đến trước "where" hoặc cuối chuỗi)
    char *set_start = set_pos + 4; // Bỏ qua "set "
    while (*set_start == ' ') set_start++; // Bỏ qua khoảng trắng
    char *set_end = strstr(set_start, "where");
    if (!set_end) set_end = (char *)(input + strlen(input)); // Nếu không có "where", lấy đến cuối chuỗi
    size_t set_len = set_end - set_start;
    char *set_clause = (char *)malloc(set_len + 1);
    if (!set_clause) {
        free(table_name);
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho set clause.\"}");
        *argc = 1;
        return args;
    }
    strncpy(set_clause, set_start, set_len);
    set_clause[set_len] = '\0';
    trim(set_clause);
    if (strlen(set_clause) == 0) {
        free(table_name);
        free(set_clause);
        args[0] = strdup("{\"error\": \"Can chi dinh set clause sau SET.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = set_clause;
    printf("DEBUG: Parsed set clause: '%s'\n", set_clause);

    // Lấy điều kiện WHERE (nếu có)
    char *where_pos = strstr(input, "where");
    if (where_pos) {
        args[(*argc)++] = strdup("where");

        char *cond_start = where_pos + 6; // Bỏ qua "where "
        while (*cond_start == ' ') cond_start++; // Bỏ qua khoảng trắng
        size_t cond_len = strlen(cond_start);
        char *condition = (char *)malloc(cond_len + 1);
        if (!condition) {
            free(table_name);
            free(set_clause);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho dieu kien.\"}");
            *argc = 1;
            return args;
        }
        strncpy(condition, cond_start, cond_len);
        condition[cond_len] = '\0';
        trim(condition);
        if (strlen(condition) == 0) {
            free(table_name);
            free(set_clause);
            free(condition);
            args[0] = strdup("{\"error\": \"Dieu kien WHERE khong duoc de trong.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = condition;
        printf("DEBUG: Parsed condition: '%s'\n", condition);
    } else {
        printf("DEBUG: No WHERE clause found\n");
    }

    printf("DEBUG: handle_update argc: %d\n", *argc);
    for (int i = 0; i < *argc; i++) {
        printf("DEBUG: args[%d]: '%s'\n", i, args[i]);
    }

    return args;
}

// handle alter table
static char** handle_alter_table(const char *input, int *argc) {
    char **args = (char **)calloc(10, sizeof(char *));
    if (!args) {
        char *error = strdup("{\"error\": \"Khong the cap phat bo nho cho args.\"}");
        args = (char **)malloc(sizeof(char *));
        args[0] = error;
        *argc = 1;
        return args;
    }
    *argc = 0;

    // Debug: In câu lệnh đầu vào
    printf("DEBUG: handle_alter_table input: '%s'\n", input);

    // Thêm "alter" vào args
    args[(*argc)++] = strdup("alter");

    // Tìm từ khóa "table"
    char *table_pos = strstr(input, "table");
    if (!table_pos) {
        args[0] = strdup("{\"error\": \"Khong tim thay 'table' trong cau lenh ALTER!\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = strdup("table");

    // Lấy tên bảng (từ sau "alter table" đến trước operation)
    char *table_start = table_pos + 6; // Bỏ qua "table "
    while (*table_start == ' ') table_start++; // Bỏ qua khoảng trắng
    char *operation_pos = strstr(table_start, "add");
    if (!operation_pos) operation_pos = strstr(table_start, "drop");
    if (!operation_pos) operation_pos = strstr(table_start, "rename");
    if (!operation_pos) operation_pos = (char *)(input + strlen(input)); // Nếu không tìm thấy operation, lấy đến cuối chuỗi
    size_t table_len = operation_pos - table_start;
    char *table_name = (char *)malloc(table_len + 1);
    if (!table_name) {
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten bang.\"}");
        *argc = 1;
        return args;
    }
    strncpy(table_name, table_start, table_len);
    table_name[table_len] = '\0';
    trim(table_name);
    if (strlen(table_name) == 0) {
        free(table_name);
        args[0] = strdup("{\"error\": \"Can chi dinh ten bang sau ALTER TABLE.\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = table_name;
    printf("DEBUG: Parsed table name: '%s'\n", table_name);

    // Lấy operation (add, drop, rename)
    char *operation_start = operation_pos;
    while (*operation_start == ' ') operation_start++; // Bỏ qua khoảng trắng
    char *operation_end = operation_start;
    while (*operation_end != ' ' && *operation_end != '\0') operation_end++; // Tìm khoảng trắng tiếp theo hoặc cuối chuỗi
    size_t operation_len = operation_end - operation_start;
    char *operation = (char *)malloc(operation_len + 1);
    if (!operation) {
        free(table_name);
        args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho operation.\"}");
        *argc = 1;
        return args;
    }
    strncpy(operation, operation_start, operation_len);
    operation[operation_len] = '\0';
    trim(operation);
    if (strlen(operation) == 0) {
        free(table_name);
        free(operation);
        args[0] = strdup("{\"error\": \"Can chi dinh operation (add, drop, rename).\"}");
        *argc = 1;
        return args;
    }
    args[(*argc)++] = operation;
    printf("DEBUG: Parsed operation: '%s'\n", operation);

    // Xử lý tùy theo operation
    if (strcmp(operation, "add") == 0) {
        // Lấy col_name và col_type
        char *col_start = operation_end;
        while (*col_start == ' ') col_start++; // Bỏ qua khoảng trắng
        char *col_end = col_start;
        while (*col_end != ' ' && *col_end != '\0') col_end++; // Tìm khoảng trắng tiếp theo hoặc cuối chuỗi
        size_t col_len = col_end - col_start;
        char *col_name = (char *)malloc(col_len + 1);
        if (!col_name) {
            free(table_name);
            free(operation);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten cot.\"}");
            *argc = 1;
            return args;
        }
        strncpy(col_name, col_start, col_len);
        col_name[col_len] = '\0';
        trim(col_name);
        if (strlen(col_name) == 0) {
            free(table_name);
            free(operation);
            free(col_name);
            args[0] = strdup("{\"error\": \"Can chi dinh ten cot.\"}");
            *argc = 1;
            return args;
        }
        printf("DEBUG: Parsed column name: '%s'\n", col_name);

        // Lấy col_type
        char *type_start = col_end;
        while (*type_start == ' ') type_start++; // Bỏ qua khoảng trắng
        size_t type_len = strlen(type_start);
        char *col_type = (char *)malloc(type_len + 1);
        if (!col_type) {
            free(table_name);
            free(operation);
            free(col_name);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho kieu du lieu.\"}");
            *argc = 1;
            return args;
        }
        strncpy(col_type, type_start, type_len);
        col_type[type_len] = '\0';
        trim(col_type);
        if (strlen(col_type) == 0) {
            free(table_name);
            free(operation);
            free(col_name);
            free(col_type);
            args[0] = strdup("{\"error\": \"Can chi dinh kieu du lieu cho cot moi.\"}");
            *argc = 1;
            return args;
        }
        printf("DEBUG: Parsed column type: '%s'\n", col_type);

        // Gộp col_name và col_type thành column_info
        char *column_info = (char *)malloc(strlen(col_name) + strlen(col_type) + 2); // +2 cho khoảng trắng và ký tự null
        if (!column_info) {
            free(table_name);
            free(operation);
            free(col_name);
            free(col_type);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho column_info.\"}");
            *argc = 1;
            return args;
        }
        sprintf(column_info, "%s %s", col_name, col_type);
        args[(*argc)++] = column_info;
        printf("DEBUG: Combined column_info: '%s'\n", column_info);

        // Lưu col_type vào args để sử dụng nếu cần
        args[(*argc)++] = col_type;

        // Giải phóng col_name vì đã gộp vào column_info
        free(col_name);
    } else if (strcmp(operation, "drop") == 0) {
        // Lấy col_name
        char *col_start = operation_end;
        while (*col_start == ' ') col_start++; // Bỏ qua khoảng trắng
        char *col_end = col_start;
        while (*col_end != ' ' && *col_end != '\0') col_end++; // Tìm khoảng trắng tiếp theo hoặc cuối chuỗi
        size_t col_len = col_end - col_start;
        char *col_name = (char *)malloc(col_len + 1);
        if (!col_name) {
            free(table_name);
            free(operation);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten cot.\"}");
            *argc = 1;
            return args;
        }
        strncpy(col_name, col_start, col_len);
        col_name[col_len] = '\0';
        trim(col_name);
        if (strlen(col_name) == 0) {
            free(table_name);
            free(operation);
            free(col_name);
            args[0] = strdup("{\"error\": \"Can chi dinh ten cot.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = col_name;
        printf("DEBUG: Parsed column name: '%s'\n", col_name);
    } else if (strcmp(operation, "rename") == 0) {
        // Lấy col_name
        char *col_start = operation_end;
        while (*col_start == ' ') col_start++; // Bỏ qua khoảng trắng
        char *to_pos = strstr(col_start, "to");
        if (!to_pos) {
            free(table_name);
            free(operation);
            args[0] = strdup("{\"error\": \"Cu phap RENAME phai co 'to' sau ten cot.\"}");
            *argc = 1;
            return args;
        }
        size_t col_len = to_pos - col_start;
        char *col_name = (char *)malloc(col_len + 1);
        if (!col_name) {
            free(table_name);
            free(operation);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten cot.\"}");
            *argc = 1;
            return args;
        }
        strncpy(col_name, col_start, col_len);
        col_name[col_len] = '\0';
        trim(col_name);
        if (strlen(col_name) == 0) {
            free(table_name);
            free(operation);
            free(col_name);
            args[0] = strdup("{\"error\": \"Can chi dinh ten cot.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = col_name;
        printf("DEBUG: Parsed column name: '%s'\n", col_name);

        // Lấy new_name (sau "to")
        char *new_name_start = to_pos + 3; // Bỏ qua "to "
        while (*new_name_start == ' ') new_name_start++; // Bỏ qua khoảng trắng
        size_t new_name_len = strlen(new_name_start);
        char *new_name = (char *)malloc(new_name_len + 1);
        if (!new_name) {
            free(table_name);
            free(operation);
            free(col_name);
            args[0] = strdup("{\"error\": \"Khong the cap phat bo nho cho ten cot moi.\"}");
            *argc = 1;
            return args;
        }
        strncpy(new_name, new_name_start, new_name_len);
        new_name[new_name_len] = '\0';
        trim(new_name);
        if (strlen(new_name) == 0) {
            free(table_name);
            free(operation);
            free(col_name);
            free(new_name);
            args[0] = strdup("{\"error\": \"Can chi dinh ten cot moi sau 'to'.\"}");
            *argc = 1;
            return args;
        }
        args[(*argc)++] = strdup("to");
        args[(*argc)++] = new_name;
        printf("DEBUG: Parsed new column name: '%s'\n", new_name);
    } else {
        free(table_name);
        free(operation);
        args[0] = strdup("{\"error\": \"Operation khong duoc ho tro, chi ho tro 'add', 'drop', 'rename'.\"}");
        *argc = 1;
        return args;
    }

    printf("DEBUG: handle_alter_table argc: %d\n", *argc);
    for (int i = 0; i < *argc; i++) {
        printf("DEBUG: args[%d]: '%s'\n", i, args[i]);
    }

    return args;
}
// ==== Các hàm xử lý lệnh ====
char* handle_showdb(int argc, char **argv, ClientSession *session) { 
    return show_database();
}

char* handle_createdb(int argc, char **argv, ClientSession *session) {
    if (argc < 2) {
        return strdup("{\"error\": \"Can chi dinh ten database!\"}");
    }
    return create_database(argv[1]);
}

char* handle_usedb(int argc, char **argv, ClientSession *session) {
    if (argc < 2) {
        return strdup("{\"error\": \"Can chi dinh database!\"}");
    }
    // Kiểm tra sự tồn tại của database
    char* check_result = check_database_exists(argv[1]);
    json_object *check_obj = json_tokener_parse(check_result);
    if (json_object_object_get_ex(check_obj, "error", NULL)) {
        free(check_result);
        json_object_put(check_obj);
        return check_result;
    }
    free(check_result);
    json_object_put(check_obj);
    // Cập nhật session->current_db
    strncpy(session->current_db, argv[1], sizeof(session->current_db) - 1);
    session->current_db[sizeof(session->current_db) - 1] = '\0';
    char response[256];
    snprintf(response, sizeof(response), "{\"status\": \"success\", \"message\": \"Da chon database %s\"}", argv[1]);
    return strdup(response);
}

char* handle_dropdb(int argc, char **argv, ClientSession *session) {
    if (argc < 2) {
        return strdup("{\"error\": \"Can chi dinh database de xoa!\"}");
    }
    return drop_database(argv[1]);
}

char* handle_renamedb(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Can chi dinh ten cu va ten moi!\"}");
    }
    return rename_database(argv[1], argv[2]);
}

char* handle_createtbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Can chi dinh ten bang va cau truc bang!\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;
    return create_table(session->current_db, argv[1], argv[2]);
}

char* handle_inserttbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Can chi dinh ten bang va du lieu can chen!\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;
    return insert_into_table(session->current_db, argv[1], argv[2]);
}

char* handle_showtbl(int argc, char **argv, ClientSession *session) {
    if (argc == 1) {
        char *error = check_session_db(session);
        if (error) return error;
        return show_table(session->current_db);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_droptbl(int argc, char **argv, ClientSession *session) {
    if (argc == 2) {
        char *error = check_session_db(session);
        if (error) return error;
        return drop_table(session->current_db, argv[1]);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_viewtbl(int argc, char **argv, ClientSession *session) {
    if (argc == 2) {
        char *error = check_session_db(session);
        if (error) return error;
        return view_table(session->current_db, argv[1]);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_selecttbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Cu phap select khong hop le, can it nhat ten bang va cot.\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;
    return select_from_table(session->current_db, argv[2], argv[1], argc > 3 ? argv[4] : "");
}

char* handle_deletetbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Cu phap DELETE khong hop le, can it nhat ten bang.\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;

    if (strcmp(argv[1], "from") != 0) {
        return strdup("{\"error\": \"Cu phap DELETE phai co 'from' sau DELETE.\"}");
    }

    const char *table_name = argv[2];
    const char *condition = (argc > 3 && strcmp(argv[3], "where") == 0 && argc > 4) ? argv[4] : NULL;

    return delete_from_table(session->current_db, table_name, condition);
}

char* handle_updatetbl(int argc, char **argv, ClientSession *session) {
    if (argc < 4) {
        return strdup("{\"error\": \"Cu phap UPDATE khong hop le, can it nhat ten bang va SET.\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;

    if (strcmp(argv[2], "set") != 0) {
        return strdup("{\"error\": \"Cu phap UPDATE phai co 'set' sau ten bang.\"}");
    }

    const char *table_name = argv[1];
    const char *set_clause = argv[3];
    const char *condition = (argc > 4 && strcmp(argv[4], "where") == 0 && argc > 5) ? argv[5] : NULL;

    return update_table(session->current_db, table_name, set_clause, condition);
}
char* handle_altertbl(int argc, char **argv, ClientSession *session) {
    if (argc < 4) {
        return strdup("{\"error\": \"Cu phap ALTER TABLE khong hop le.\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;

    if (strcmp(argv[1], "table") != 0) {
        return strdup("{\"error\": \"Cu phap ALTER phai co 'table' sau ALTER.\"}");
    }

    const char *table_name = argv[2];
    const char *operation = argv[3];
    const char *col_name = (argc > 4) ? argv[4] : NULL;
    const char *col_type_or_new_name = (argc > 5) ? argv[5] : NULL;
    const char *new_name = (argc > 6 && strcmp(argv[5], "to") == 0) ? argv[6] : NULL;

    if (strcmp(operation, "add") == 0 && col_name && col_type_or_new_name) {
        // Gộp col_name và col_type thành column_info
        char column_info[100];
        snprintf(column_info, sizeof(column_info), "%s %s", col_name, col_type_or_new_name);
        return alter_table(session->current_db, table_name, "add", column_info, NULL);
    } else if (strcmp(operation, "drop") == 0 && col_name) {
        return alter_table(session->current_db, table_name, "drop", col_name, NULL);
    } else if (strcmp(operation, "rename") == 0 && col_name && new_name) {
        return alter_table(session->current_db, table_name, "rename", col_name, new_name);
    } else {
        return strdup("{\"error\": \"Cu phap ALTER TABLE khong hop le.\"}");
    }
}

char* handle_describetbl(int argc, char **argv, ClientSession *session) {
    if (argc != 2) {
        return strdup("{\"error\": \"Cu phap DESCRIBE khong hop le, can chi dinh ten bang.\"}");
    }
    char *error = check_session_db(session);
    if (error) return error;

    return describe_table(session->current_db, argv[1]);
}

// ==== Danh sách các lệnh ====
Command commands[] = {
    {"credb", handle_createdb},
    {"use", handle_usedb},
    {"dropdb", handle_dropdb},
    {"renamedb", handle_renamedb},
    {"cretbl", handle_createtbl},
    {"insert", handle_inserttbl},
    {"showdb", handle_showdb},
    {"showtbl", handle_showtbl},
    {"droptbl", handle_droptbl},
    {"view", handle_viewtbl},
    {"select", handle_selecttbl},
    {"delete", handle_deletetbl},
    {"update", handle_updatetbl},
    {"alter", handle_altertbl},
    {"describe", handle_describetbl}
};

int num_commands = sizeof(commands) / sizeof(commands[0]);

// ==== Hàm phân tách lệnh & gọi hàm tương ứng ====
char* execute_command(const char *input, ClientSession *session) {
    char *args[10];
    int argc = 0;

    // Xử lý đặc biệt cho SELECT
    if (strncmp(input, "select", 6) == 0) {
        char **parsed_args = handle_select(input, &argc);
        if (argc == 1 && strstr(parsed_args[0], "\"error\"")) {
            char *result = strdup(parsed_args[0]);
            free(parsed_args[0]);
            free(parsed_args);
            return result;
        }
        for (int i = 0; i < num_commands; i++) {
            if (strcmp(parsed_args[0], commands[i].command) == 0) {
                char *result = commands[i].func(argc, parsed_args, session);
                // Chỉ giải phóng các phần tử được cấp phát
                if (argc > 1 && parsed_args[1]) free(parsed_args[1]); // Giải phóng danh sách cột
                if (argc > 4 && parsed_args[4]) free(parsed_args[4]); // Giải phóng điều kiện (nếu có)
                free(parsed_args[0]); // Giải phóng "select"
                if (argc > 2 && parsed_args[2]) free(parsed_args[2]); // Giải phóng table_name
                if (argc > 3 && parsed_args[3]) free(parsed_args[3]); // Giải phóng "where"
                free(parsed_args);
                return result;
            }
        }
        // Giải phóng nếu không tìm thấy lệnh
        if (argc > 1 && parsed_args[1]) free(parsed_args[1]);
        if (argc > 4 && parsed_args[4]) free(parsed_args[4]);
        free(parsed_args[0]);
        if (argc > 2 && parsed_args[2]) free(parsed_args[2]);
        if (argc > 3 && parsed_args[3]) free(parsed_args[3]);
        free(parsed_args);
        return strdup("{\"error\": \"Lenh khong hop le.\"}");
    }

    // Xử lý đặc biệt cho DELETE
    if (strncmp(input, "delete", 6) == 0) {
        char **parsed_args = handle_delete(input, &argc);
        if (argc == 1 && strstr(parsed_args[0], "\"error\"")) {
            char *result = strdup(parsed_args[0]);
            free(parsed_args[0]);
            free(parsed_args);
            return result;
        }
        for (int i = 0; i < num_commands; i++) {
            if (strcmp(parsed_args[0], commands[i].command) == 0) {
                char *result = commands[i].func(argc, parsed_args, session);
                if (argc > 4 && parsed_args[4]) free(parsed_args[4]); // Giải phóng điều kiện (nếu có)
                free(parsed_args[0]); // Giải phóng "delete"
                free(parsed_args[1]); // Giải phóng "from"
                free(parsed_args[2]); // Giải phóng table_name
                if (argc > 3 && parsed_args[3]) free(parsed_args[3]); // Giải phóng "where"
                free(parsed_args);
                return result;
            }
        }
        if (argc > 4 && parsed_args[4]) free(parsed_args[4]);
        free(parsed_args[0]);
        free(parsed_args[1]);
        free(parsed_args[2]);
        if (argc > 3 && parsed_args[3]) free(parsed_args[3]);
        free(parsed_args);
        return strdup("{\"error\": \"Lenh khong hop le.\"}");
    }

    // Xử lý đặc biệt cho UPDATE
    if (strncmp(input, "update", 6) == 0) {
        char **parsed_args = handle_update(input, &argc);
        if (argc == 1 && strstr(parsed_args[0], "\"error\"")) {
            char *result = strdup(parsed_args[0]);
            free(parsed_args[0]);
            free(parsed_args);
            return result;
        }
        for (int i = 0; i < num_commands; i++) {
            if (strcmp(parsed_args[0], commands[i].command) == 0) {
                char *result = commands[i].func(argc, parsed_args, session);
                free(parsed_args[1]); // Giải phóng table_name
                free(parsed_args[2]); // Giải phóng "set"
                free(parsed_args[3]); // Giải phóng set_clause
                if (argc > 5 && parsed_args[5]) free(parsed_args[5]); // Giải phóng điều kiện (nếu có)
                free(parsed_args[0]); // Giải phóng "update"
                if (argc > 4 && parsed_args[4]) free(parsed_args[4]); // Giải phóng "where"
                free(parsed_args);
                return result;
            }
        }
        free(parsed_args[1]);
        free(parsed_args[2]);
        free(parsed_args[3]);
        if (argc > 5 && parsed_args[5]) free(parsed_args[5]);
        free(parsed_args[0]);
        if (argc > 4 && parsed_args[4]) free(parsed_args[4]);
        free(parsed_args);
        return strdup("{\"error\": \"Lenh khong hop le.\"}");
    }

    // Xử lý đặc biệt cho ALTER TABLE
    if (strncmp(input, "alter", 5) == 0) {
        char **parsed_args = handle_alter_table(input, &argc);
        if (argc == 1 && strstr(parsed_args[0], "\"error\"")) {
            char *result = strdup(parsed_args[0]);
            free(parsed_args[0]);
            free(parsed_args);
            return result;
        }
        for (int i = 0; i < num_commands; i++) {
            if (strcmp(parsed_args[0], commands[i].command) == 0) {
                char *result = commands[i].func(argc, parsed_args, session);
                free(parsed_args[1]); // Giải phóng "table"
                free(parsed_args[2]); // Giải phóng table_name
                free(parsed_args[3]); // Giải phóng operation
                if (argc > 4 && parsed_args[4]) free(parsed_args[4]); // Giải phóng col_name
                if (argc > 5 && parsed_args[5]) free(parsed_args[5]); // Giải phóng col_type hoặc "to"
                if (argc > 6 && parsed_args[6]) free(parsed_args[6]); // Giải phóng new_name
                free(parsed_args[0]); // Giải phóng "alter"
                free(parsed_args);
                return result;
            }
        }
        free(parsed_args[1]);
        free(parsed_args[2]);
        free(parsed_args[3]);
        if (argc > 4 && parsed_args[4]) free(parsed_args[4]);
        if (argc > 5 && parsed_args[5]) free(parsed_args[5]);
        if (argc > 6 && parsed_args[6]) free(parsed_args[6]);
        free(parsed_args[0]);
        free(parsed_args);
        return strdup("{\"error\": \"Lenh khong hop le.\"}");
    }

    // Xử lý các lệnh khác
    char buffer[256];
    strncpy(buffer, input, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    char *token = strtok(buffer, " ");
    if (!token) {
        return strdup("{\"error\": \"Khong co lenh nao duoc nhap.\"}");
    }
    args[argc++] = token;

    token = strtok(NULL, " ");
    if (token) {
        args[argc++] = token;
    }
    char *values_start = strchr(input, '(');
    char *values_end = strrchr(input, ')');
    if (values_start && values_end && values_end > values_start) {
        size_t values_len = values_end - values_start + 1;
        char *values = (char *)malloc(values_len + 1);
        if (!values) {
            return strdup("{\"error\": \"Khong the cap phat bo nho cho gia tri.\"}");
        }
        strncpy(values, values_start, values_len);
        values[values_len] = '\0';
        args[argc++] = values;
    } else {
        while ((token = strtok(NULL, " ")) && argc < 10) {
            args[argc++] = token;
        }
    }

    if (argc == 0) {
        return strdup("{\"error\": \"Khong co lenh nao duoc nhap.\"}");
    }
    for (int i = 0; i < num_commands; i++) {
        if (strcmp(args[0], commands[i].command) == 0) {
            char *result = commands[i].func(argc, args, session);
            if (argc > 2 && values_start && values_end) {
                free(args[2]);
            }
            return result;
        }
    }
    return strdup("{\"error\": \"Lenh khong hop le, vui long kiem tra lai cu phap.\"}");
}
