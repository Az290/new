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
    if (strlen(session->current_db) == 0) {
        return strdup("{\"error\": \"Chua chon database!\"}");
    }
    return create_table(session->current_db, argv[1], argv[2]);
}

char* handle_inserttbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Can chi dinh ten bang va du lieu can chen!\"}");
    }
    if (strlen(session->current_db) == 0) {
        return strdup("{\"error\": \"Chua chon database!\"}");
    }
    return insert_into_table(session->current_db, argv[1], argv[2]);
}

char* handle_showtbl(int argc, char **argv, ClientSession *session) {
    if (argc == 1) {
        if (strlen(session->current_db) == 0) {
            return strdup("{\"error\": \"Chua chon database!\"}");
        }
        return show_table(session->current_db);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_droptbl(int argc, char **argv, ClientSession *session) {
    if (argc == 2) {
        if (strlen(session->current_db) == 0) {
            return strdup("{\"error\": \"Chua chon database!\"}");
        }
        return drop_table(session->current_db, argv[1]);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_viewtbl(int argc, char **argv, ClientSession *session) {
    if (argc == 2) {
        if (strlen(session->current_db) == 0) {
            return strdup("{\"error\": \"Chua chon database!\"}");
        }
        return view_table(session->current_db, argv[1]);
    } else {
        return strdup("{\"error\": \"Cu phap khong hop le.\"}");
    }
}

char* handle_selecttbl(int argc, char **argv, ClientSession *session) {
    if (argc < 3) {
        return strdup("{\"error\": \"Cu phap select khong hop le, can it nhat ten bang va cot.\"}");
    }
    if (strlen(session->current_db) == 0) {
        return strdup("{\"error\": \"Chua chon database!\"}");
    }
    return select_from_table(session->current_db, argv[2], argv[1], argc > 3 ? argv[3] : "");
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
    {"select", handle_selecttbl}
};

int num_commands = sizeof(commands) / sizeof(commands[0]);

// ==== Hàm phân tách lệnh & gọi hàm tương ứng ====
char* execute_command(const char *input, ClientSession *session) {
    char *args[10];
    int argc = 0;
    char buffer[256];
    strncpy(buffer, input, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';

    // Tách lệnh đầu tiên
    char *token = strtok(buffer, " ");
    if (!token) {
        return strdup("{\"error\": \"Khong co lenh nao duoc nhap.\"}");
    }
    args[argc++] = token;

    // Xử lý đặc biệt cho SELECT
    if (strcmp(args[0], "select") == 0) {
        char *col_start = strchr(input, '"');
        char *col_end = col_start ? strchr(col_start + 1, '"') : NULL;
        if (!col_start || !col_end) {
            return strdup("{\"error\": \"Danh sach cot phai nam trong dau \\\" \\\"!\"}");
        }
        size_t col_len = col_end - col_start - 1;
        char *columns = (char *)malloc(col_len + 1);
        strncpy(columns, col_start + 1, col_len);
        columns[col_len] = '\0';
        args[argc++] = columns;

        char *from_pos = strstr(input, "from");
        if (!from_pos) {
            free(columns);
            return strdup("{\"error\": \"Khong tim thay 'from' trong cau lenh SELECT!\"}");
        }
        token = strtok(from_pos + 5, " ");
        if (!token) {
            free(columns);
            return strdup("{\"error\": \"Can chi dinh ten bang sau 'from'!\"}");
        }
        args[argc++] = token;

        char *where_pos = strstr(input, "where");
        if (where_pos) {
            char *cond_start = strchr(where_pos, '"');
            char *cond_end = cond_start ? strchr(cond_start + 1, '"') : NULL;
            if (cond_start && cond_end) {
                size_t cond_len = cond_end - cond_start - 1;
                char *condition = (char *)malloc(cond_len + 1);
                strncpy(condition, cond_start + 1, cond_len);
                condition[cond_len] = '\0';
                args[argc++] = condition;
            }
        }

        for (int i = 0; i < num_commands; i++) {
            if (strcmp(args[0], commands[i].command) == 0) {
                char *result = commands[i].func(argc, args, session);
                free(args[1]); // Giải phóng danh sách cột
                if (argc > 3) free(args[3]); // Giải phóng điều kiện (nếu có)
                return result;
            }
        }
        free(args[1]);
        if (argc > 3) free(args[3]);
        return strdup("{\"error\": \"Lenh khong hop le.\"}");
    }

    // Xử lý các lệnh khác
    token = strtok(NULL, " ");
    if (token) {
        args[argc++] = token;
    }
    char *values_start = strchr(input, '(');
    char *values_end = strrchr(input, ')');
    if (values_start && values_end && values_end > values_start) {
        size_t values_len = values_end - values_start + 1;
        char *values = (char *)malloc(values_len + 1);
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
