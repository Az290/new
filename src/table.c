#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <json-c/json.h>
#include "../include/datatype.h"
#include "../include/table.h"
#include "../include/database.h"

#define DATA_DIR "../data/"
#define MAX_COLS 10
#define MAX_ROWS 100
#define MAX_LEN 50

static void trim(char *str) {
    char *start = str;
    while (*start == ' ') start++;
    char *end = str + strlen(str) - 1;
    while (end >= start && *end == ' ') *end-- = '\0';
    memmove(str, start, strlen(start) + 1);
}

char* create_table(const char *db_name, const char *table_name, const char *columns) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char db_path[256], table_meta[256], table_data[256], tables_list[256];
    snprintf(db_path, sizeof(db_path), "%s%s", DATA_DIR, db_name);
    snprintf(table_meta, sizeof(table_meta), "%s/%s.meta", db_path, table_name);
    snprintf(table_data, sizeof(table_data), "%s/%s.tbl", db_path, table_name);
    snprintf(tables_list, sizeof(tables_list), "%s/tables.list", db_path);

    struct stat st;
    if (stat(db_path, &st) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Database khong ton tai"));
        goto end;
    }

    if (access(table_meta, F_OK) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang da ton tai"));
        goto end;
    }

    if (columns[0] != '(' || columns[strlen(columns) - 1] != ')') {
        json_object_object_add(root, "error", json_object_new_string("Dinh dang cot khong hop le"));
        goto end;
    }

    char *clean_columns = malloc(strlen(columns) - 1);
    if (!clean_columns) {
        json_object_object_add(root, "error", json_object_new_string("Khong du bo nho"));
        goto end;
    }
    strncpy(clean_columns, columns + 1, strlen(columns) - 2);
    clean_columns[strlen(columns) - 2] = '\0';

    FILE *meta_file = fopen(table_meta, "w");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Loi khi tao file meta"));
        free(clean_columns);
        goto end;
    }

    char column_name[50], data_type[50];
    char *token = strtok(clean_columns, ",");
    while (token) {
        trim(token);
        if (sscanf(token, "%49[^:]:%49s", column_name, data_type) != 2) {
            json_object_object_add(root, "error", json_object_new_string("Dinh dang cot sai"));
            fclose(meta_file);
            free(clean_columns);
            remove(table_meta);
            remove(table_data);
            goto end;
        }
        if (strcmp(data_type, "int") != 0 && strcmp(data_type, "text") != 0 && strcmp(data_type, "date") != 0) {
            json_object_object_add(root, "error", json_object_new_string("Kieu du lieu khong hop le"));
            fclose(meta_file);
            free(clean_columns);
            remove(table_meta);
            remove(table_data);
            goto end;
        }
        fprintf(meta_file, "%s %s\n", column_name, data_type);
        token = strtok(NULL, ",");
    }
    fclose(meta_file);
    free(clean_columns);

    FILE *data_file = fopen(table_data, "w");
    if (!data_file) {
        json_object_object_add(root, "error", json_object_new_string("Loi khi tao file data"));
        remove(table_data);
        goto end;
    }
    fclose(data_file);

    FILE *list_file = fopen(tables_list, "a");
    if (!list_file) {
        json_object_object_add(root, "error", json_object_new_string("Loi khi cap nhat danh sach bang"));
        goto end;
    }
    fprintf(list_file, "%s\n", table_name);
    fclose(list_file);

    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string("Bang da duoc tao thanh cong"));

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

char* insert_into_table(const char *db_name, const char *table_name, const char *values) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char table_meta[256], table_data[256];
    snprintf(table_meta, sizeof(table_meta), "%s%s/%s.meta", DATA_DIR, db_name, table_name);
    snprintf(table_data, sizeof(table_data), "%s%s/%s.tbl", DATA_DIR, db_name, table_name);

    FILE *meta_file = fopen(table_meta, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong mo duoc file meta"));
        goto end;
    }

    char columns[10][50];
    DataType types[10];
    int col_count = 0;
    char col_name[50], type_name[10];
    while (fscanf(meta_file, "%49s %9s", col_name, type_name) == 2) {
        strncpy(columns[col_count], col_name, sizeof(columns[col_count]));
        if (strcmp(type_name, "int") == 0) types[col_count] = TYPE_INT;
        else if (strcmp(type_name, "text") == 0) types[col_count] = TYPE_TEXT;
        else if (strcmp(type_name, "date") == 0) types[col_count] = TYPE_DATE;
        else {
            json_object_object_add(root, "error", json_object_new_string("Kieu du lieu khong hop le"));
            fclose(meta_file);
            goto end;
        }
        col_count++;
    }
    fclose(meta_file);

    if (values[0] != '(' || values[strlen(values) - 1] != ')') {
        json_object_object_add(root, "error", json_object_new_string("Dinh dang gia tri khong hop le"));
        goto end;
    }

    char *clean_values = malloc(strlen(values) - 1);
    if (!clean_values) {
        json_object_object_add(root, "error", json_object_new_string("Khong du bo nho"));
        goto end;
    }
    strncpy(clean_values, values + 1, strlen(values) - 2);
    clean_values[strlen(values) - 2] = '\0';

    char *data_tokens[10];
    int value_count = 0;
    char *token = strtok(clean_values, ",");
    while (token && value_count < 10) {
        trim(token);
        data_tokens[value_count++] = token;
        token = strtok(NULL, ",");
    }

    if (value_count != col_count) {
        json_object_object_add(root, "error", json_object_new_string("So luong gia tri khong khop voi so cot"));
        free(clean_values);
        goto end;
    }

    for (int i = 0; i < col_count; i++) {
        if (!validate_data(types[i], data_tokens[i])) {
            json_object_object_add(root, "error", json_object_new_string("Du lieu khong hop le"));
            free(clean_values);
            goto end;
        }
    }

    FILE *data_file = fopen(table_data, "a");
    if (!data_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu"));
        free(clean_values);
        goto end;
    }

    for (int i = 0; i < col_count; i++) {
        if (types[i] == TYPE_TEXT) {
            fprintf(data_file, "%.*s\t", (int)(strlen(data_tokens[i]) - 2), data_tokens[i] + 1);
        } else {
            fprintf(data_file, "%s\t", data_tokens[i]);
        }
    }
    fprintf(data_file, "\n");
    fclose(data_file);
    free(clean_values);

    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string("Du lieu da duoc them vao bang"));

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

char* show_table(const char *db_name) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char tables_list[256];
    snprintf(tables_list, sizeof(tables_list), "%s%s/tables.list", DATA_DIR, db_name);

    FILE *list_file = fopen(tables_list, "r");
    if (!list_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file tables.list"));
        goto end;
    }

    json_object *tables = json_object_new_array();
    char table_name[256];
    int has_tables = 0;
    while (fgets(table_name, sizeof(table_name), list_file)) {
        table_name[strcspn(table_name, "\n")] = 0;
        json_object_array_add(tables, json_object_new_string(table_name));
        has_tables = 1;
    }
    fclose(list_file);

    if (!has_tables) {
        json_object_object_add(root, "message", json_object_new_string("Database khong co bang nao"));
    } else {
        json_object_object_add(root, "tables", tables);
    }

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

char* drop_table(const char *db_name, const char *table_name) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char db_path[256];
    snprintf(db_path, sizeof(db_path), "%s%s", DATA_DIR, db_name);

    char table_meta[256];
    snprintf(table_meta, sizeof(table_meta), "%s/%s.meta", db_path, table_name);

    char table_data[256];
    snprintf(table_data, sizeof(table_data), "%s/%s.tbl", db_path, table_name);

    char tables_list[256];
    snprintf(tables_list, sizeof(tables_list), "%s/tables.list", db_path);

    if (access(table_meta, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai"));
        goto end;
    }

    if (unlink(table_meta) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Khong the xoa file meta"));
        goto end;
    }

    if (unlink(table_data) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Khong the xoa file data"));
        goto end;
    }

    FILE *list_file = fopen(tables_list, "r");
    if (!list_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file tables.list"));
        goto end;
    }

    char buffer[256];
    char updated_list[1024] = "";
    while (fgets(buffer, sizeof(buffer), list_file)) {
        buffer[strcspn(buffer, "\n")] = 0;
        if (strcmp(buffer, table_name) != 0) {
            strcat(updated_list, buffer);
            strcat(updated_list, "\n");
        }
    }
    fclose(list_file);

    list_file = fopen(tables_list, "w");
    if (!list_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the ghi lai file tables.list"));
        goto end;
    }
    fputs(updated_list, list_file);
    fclose(list_file);

    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string("Bang da duoc xoa thanh cong"));

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

char* view_table(const char *db_name, const char *table_name) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char table_meta_path[256], table_data_path[256];
    snprintf(table_meta_path, sizeof(table_meta_path), "%s%s/%s.meta", DATA_DIR, db_name, table_name);
    snprintf(table_data_path, sizeof(table_data_path), "%s%s/%s.tbl", DATA_DIR, db_name, table_name);

    FILE *meta_file = fopen(table_meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta"));
        goto end;
    }

    char columns[MAX_COLS][MAX_LEN];
    char types[MAX_COLS][MAX_LEN];
    int col_count = 0;
    while (fscanf(meta_file, "%s %s", columns[col_count], types[col_count]) == 2) {
        col_count++;
        if (col_count >= MAX_COLS) break;
    }
    fclose(meta_file);

    FILE *data_file = fopen(table_data_path, "r");
    if (!data_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu"));
        goto end;
    }

    json_object *cols = json_object_new_array();
    for (int i = 0; i < col_count; i++) {
        json_object_array_add(cols, json_object_new_string(columns[i]));
    }
    json_object_object_add(root, "columns", cols);

    json_object *rows = json_object_new_array();
    char line[1024];
    while (fgets(line, sizeof(line), data_file)) {
        line[strcspn(line, "\n")] = 0;
        json_object *row = json_object_new_array();
        char *token = strtok(line, "\t");
        int i = 0;
        while (token && i < col_count) {
            json_object_array_add(row, json_object_new_string(token));
            token = strtok(NULL, "\t");
            i++;
        }
        json_object_array_add(rows, row);
    }
    fclose(data_file);
    json_object_object_add(root, "rows", rows);

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

char* select_from_table(const char *db_name, const char *table_name, const char *columns, const char *condition) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char meta_path[256], data_path[256];
    snprintf(meta_path, sizeof(meta_path), "%s%s/%s.meta", DATA_DIR, db_name, table_name);
    snprintf(data_path, sizeof(data_path), "%s%s/%s.tbl", DATA_DIR, db_name, table_name);

    if (access(meta_path, F_OK) != 0 || access(data_path, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai"));
        goto end;
    }

    FILE *meta_file = fopen(meta_path, "r");
    char col_names[10][50];
    int col_count = 0;
    while (fscanf(meta_file, "%s %*s", col_names[col_count]) == 1) {
        col_count++;
    }
    fclose(meta_file);

    char *col_list[10];
    int display_cols = 0;

    if (strcmp(columns, "*") == 0) {
        for (int i = 0; i < col_count; i++) {
            col_list[i] = col_names[i];
        }
        display_cols = col_count;
    } else {
        char temp_columns[256];
        strcpy(temp_columns, columns);
        char *token = strtok(temp_columns, ", ");
        while (token && display_cols < 10) {
            col_list[display_cols++] = token;
            token = strtok(NULL, ", ");
        }
    }

    json_object *selected_cols = json_object_new_array();
    for (int i = 0; i < display_cols; i++) {
        json_object_array_add(selected_cols, json_object_new_string(col_list[i]));
    }
    json_object_object_add(root, "columns", selected_cols);

    json_object *rows = json_object_new_array();
    FILE *data_file = fopen(data_path, "r");
    char line[1024];
    while (fgets(line, sizeof(line), data_file)) {
        line[strcspn(line, "\n")] = 0;
        char *values[10];
        int i = 0;
        char *token = strtok(line, "\t");
        while (token && i < col_count) {
            values[i++] = token;
            token = strtok(NULL, "\t");
        }

        int row_matches = 1;
        if (condition && strlen(condition) > 0) {
            char cond_col[50], cond_op[3], cond_val[50];
            sscanf(condition, "%s %s %s", cond_col, cond_op, cond_val);
            for (int j = 0; j < col_count; j++) {
                if (strcmp(col_names[j], cond_col) == 0) {
                    if (strcmp(cond_op, ">") == 0) {
                        row_matches = atoi(values[j]) > atoi(cond_val);
                    } else if (strcmp(cond_op, "=") == 0) {
                        row_matches = strcmp(values[j], cond_val) == 0;
                    }
                    break;
                }
            }
        }

        if (row_matches) {
            json_object *row = json_object_new_array();
            for (int j = 0; j < display_cols; j++) {
                for (int k = 0; k < col_count; k++) {
                    if (strcmp(col_list[j], col_names[k]) == 0) {
                        json_object_array_add(row, json_object_new_string(values[k]));
                        break;
                    }
                }
            }
            json_object_array_add(rows, row);
        }
    }
    fclose(data_file);
    json_object_object_add(root, "rows", rows);

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}
