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

void trim(char *str) {
    int start = 0;
    int end = strlen(str) - 1;

    // Loại bỏ khoảng trắng và ký tự xuống dòng ở đầu
    while (start <= end && (isspace(str[start]) || str[start] == '\r' || str[start] == '\n')) {
        start++;
    }

    // Loại bỏ khoảng trắng và ký tự xuống dòng ở cuối
    while (end >= start && (isspace(str[end]) || str[end] == '\r' || str[end] == '\n')) {
        end--;
    }

    // Dịch chuyển chuỗi và thêm ký tự kết thúc
    memmove(str, str + start, end - start + 1);
    str[end - start + 1] = '\0';
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
        if (i != col_count) {
            json_object_put(row);
            fclose(data_file);
            json_object_object_add(root, "error", json_object_new_string("Du lieu trong file khong hop le"));
            goto end;
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

// Hàm xử lý điều kiện
int evaluate_condition(const char *condition, char *col_names[], char *col_types[], char *values[], int col_count) {
    printf("DEBUG: evaluate_condition called with condition: '%s'\n", condition ? condition : "NULL");

    if (!condition || strlen(condition) == 0) {
        printf("DEBUG: No condition provided, returning true (1)\n");
        return 1; // Không có điều kiện, trả về true
    }

    char temp_cond[256];
    strncpy(temp_cond, condition, sizeof(temp_cond) - 1);
    temp_cond[sizeof(temp_cond) - 1] = '\0';
    trim(temp_cond);
    printf("DEBUG: After trimming, condition: '%s'\n", temp_cond);

    // Tách điều kiện thành các phần (AND, OR)
    char *conditions[10] = {NULL};
    int cond_count = 0;
    char current_cond[256] = "";
    char *temp_ptr = temp_cond;
    char *next_and = strstr(temp_ptr, " and ");
    char *next_or = strstr(temp_ptr, " or ");

    while (next_and || next_or) {
        char *next_op = next_and;
        if (!next_op || (next_or && next_or < next_op)) next_op = next_or;

        size_t len = next_op - temp_ptr;
        strncpy(current_cond, temp_ptr, len);
        current_cond[len] = '\0';
        trim(current_cond);

        if (strlen(current_cond) > 0) {
            if (cond_count >= 10) {
                printf("DEBUG: Too many conditions (max 10), returning false (0)\n");
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            conditions[cond_count] = strdup(current_cond);
            if (!conditions[cond_count]) {
                printf("DEBUG: Memory allocation failed for condition, returning false (0)\n");
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            printf("DEBUG: Added condition[%d]: '%s'\n", cond_count, conditions[cond_count]);
            cond_count++;
        }

        if (next_op == next_and) {
            conditions[cond_count] = strdup("and");
        } else {
            conditions[cond_count] = strdup("or");
        }
        if (!conditions[cond_count]) {
            printf("DEBUG: Memory allocation failed for operator, returning false (0)\n");
            for (int j = 0; j < cond_count; j++) {
                if (conditions[j]) free(conditions[j]);
            }
            return 0;
        }
        printf("DEBUG: Added operator condition[%d]: '%s'\n", cond_count, conditions[cond_count]);
        cond_count++;

        temp_ptr = next_op + (next_op == next_and ? 5 : 4);
        next_and = strstr(temp_ptr, " and ");
        next_or = strstr(temp_ptr, " or ");
    }

    // Thêm điều kiện cuối cùng
    if (strlen(temp_ptr) > 0) {
        if (cond_count >= 10) {
            printf("DEBUG: Too many conditions (max 10) at the end, returning false (0)\n");
            for (int j = 0; j < cond_count; j++) {
                if (conditions[j]) free(conditions[j]);
            }
            return 0;
        }
        strncpy(current_cond, temp_ptr, sizeof(current_cond) - 1);
        current_cond[sizeof(current_cond) - 1] = '\0';
        trim(current_cond);
        conditions[cond_count] = strdup(current_cond);
        if (!conditions[cond_count]) {
            printf("DEBUG: Memory allocation failed for last condition, returning false (0)\n");
            for (int j = 0; j < cond_count; j++) {
                if (conditions[j]) free(conditions[j]);
            }
            return 0;
        }
        printf("DEBUG: Added last condition[%d]: '%s'\n", cond_count, conditions[cond_count]);
        cond_count++;
    }
    printf("DEBUG: Total conditions parsed: %d\n", cond_count);

    int result = 1;
    for (int i = 0; i < cond_count; i++) {
        printf("DEBUG: Processing condition[%d]: '%s'\n", i, conditions[i]);
        if (strcmp(conditions[i], "and") == 0) {
            if (i + 1 >= cond_count) {
                printf("DEBUG: Invalid AND condition (no next condition), returning false (0)\n");
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            result = result && evaluate_condition(conditions[i + 1], col_names, col_types, values, col_count);
            printf("DEBUG: After AND, result: %d\n", result);
            i++;
        } else if (strcmp(conditions[i], "or") == 0) {
            if (i + 1 >= cond_count) {
                printf("DEBUG: Invalid OR condition (no next condition), returning false (0)\n");
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            result = result || evaluate_condition(conditions[i + 1], col_names, col_types, values, col_count);
            printf("DEBUG: After OR, result: %d\n", result);
            i++;
        } else {
            char cond_col[50], cond_op[10], cond_val[50];
            // Tách điều kiện thành column, operator, value
            char *eq_pos = strchr(conditions[i], '=');
            char *gt_pos = strchr(conditions[i], '>');
            char *lt_pos = strchr(conditions[i], '<');
            char *like_pos = strstr(conditions[i], " like ");
            char *in_pos = strstr(conditions[i], " in ");

            char *op_pos = eq_pos;
            char *op_end = eq_pos + 1;
            strcpy(cond_op, "=");
            if (!op_pos || (gt_pos && gt_pos < op_pos)) {
                op_pos = gt_pos;
                op_end = gt_pos + 1;
                strcpy(cond_op, ">");
            }
            if (!op_pos || (lt_pos && lt_pos < op_pos)) {
                op_pos = lt_pos;
                op_end = lt_pos + 1;
                strcpy(cond_op, "<");
            }
            if (!op_pos || (like_pos && like_pos < op_pos)) {
                op_pos = like_pos;
                op_end = like_pos + 6;
                strcpy(cond_op, "like");
            }
            if (!op_pos || (in_pos && in_pos < op_pos)) {
                op_pos = in_pos;
                op_end = in_pos + 4;
                strcpy(cond_op, "in");
            }

            if (!op_pos) {
                printf("DEBUG: Invalid condition format in '%s', returning false (0)\n", conditions[i]);
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }

            // Tách column
            size_t col_len = op_pos - conditions[i];
            strncpy(cond_col, conditions[i], col_len);
            cond_col[col_len] = '\0';
            trim(cond_col);
            printf("DEBUG: Parsed column: '%s'\n", cond_col);

            // Tách value
            strncpy(cond_val, op_end, sizeof(cond_val) - 1);
            cond_val[sizeof(cond_val) - 1] = '\0';
            trim(cond_val);
            printf("DEBUG: Parsed operator: '%s', value: '%s'\n", cond_op, cond_val);

            int col_idx = -1;
            int is_int_type = 0;
            for (int j = 0; j < col_count; j++) {
                if (!col_names[j] || !col_types[j]) {
                    printf("DEBUG: col_names[%d] or col_types[%d] is NULL, returning false (0)\n", j, j);
                    for (int k = 0; k < cond_count; j++) {
                        if (conditions[k]) free(conditions[k]);
                    }
                    return 0;
                }
                printf("DEBUG: Comparing column '%s' with col_names[%d] = '%s'\n", cond_col, j, col_names[j]);
                if (strcmp(col_names[j], cond_col) == 0) {
                    col_idx = j;
                    if (strcmp(col_types[j], "int") == 0) {
                        is_int_type = 1;
                    }
                    break;
                }
            }
            if (col_idx == -1) {
                printf("DEBUG: Column '%s' not found, returning false (0)\n", cond_col);
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            printf("DEBUG: Found column '%s' at index %d, is_int_type: %d\n", cond_col, col_idx, is_int_type);

            if (!values[col_idx]) {
                printf("DEBUG: Value for column '%s' is NULL, returning false (0)\n", cond_col);
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
            printf("DEBUG: Value for column '%s': '%s'\n", cond_col, values[col_idx]);

            char clean_val[50];
            strncpy(clean_val, cond_val, sizeof(clean_val) - 1);
            clean_val[sizeof(clean_val) - 1] = '\0';
            if (clean_val[0] == '"' && clean_val[strlen(clean_val) - 1] == '"') {
                clean_val[strlen(clean_val) - 1] = '\0';
                memmove(clean_val, clean_val + 1, strlen(clean_val));
                printf("DEBUG: Removed quotes from value, clean_val: '%s'\n", clean_val);
            }

            if (strcmp(cond_op, ">") == 0) {
                if (is_int_type) {
                    result = atoi(values[col_idx]) > atoi(clean_val);
                    printf("DEBUG: Comparing (int) %d > %d, result: %d\n", atoi(values[col_idx]), atoi(clean_val), result);
                } else {
                    result = strcmp(values[col_idx], clean_val) > 0;
                    printf("DEBUG: Comparing (string) '%s' > '%s', result: %d\n", values[col_idx], clean_val, result);
                }
            } else if (strcmp(cond_op, "=") == 0) {
                if (is_int_type) {
                    result = atoi(values[col_idx]) == atoi(clean_val);
                    printf("DEBUG: Comparing (int) %d = %d, result: %d\n", atoi(values[col_idx]), atoi(clean_val), result);
                } else {
                    result = strcmp(values[col_idx], clean_val) == 0;
                    printf("DEBUG: Comparing (string) '%s' = '%s', result: %d\n", values[col_idx], clean_val, result);
                }
            } else if (strcmp(cond_op, "like") == 0) {
                if (clean_val[0] == '%' && clean_val[strlen(clean_val) - 1] != '%') {
                    char *pattern = clean_val + 1;
                    result = (strstr(values[col_idx], pattern) == values[col_idx] + strlen(values[col_idx]) - strlen(pattern));
                    printf("DEBUG: LIKE (ends with) '%s' in '%s', result: %d\n", pattern, values[col_idx], result);
                } else if (clean_val[0] != '%' && clean_val[strlen(clean_val) - 1] == '%') {
                    char *pattern = strdup(clean_val);
                    if (!pattern) {
                        printf("DEBUG: Memory allocation failed for pattern, returning false (0)\n");
                        for (int j = 0; j < cond_count; j++) {
                            if (conditions[j]) free(conditions[j]);
                        }
                        return 0;
                    }
                    pattern[strlen(pattern) - 1] = '\0';
                    result = (strncmp(values[col_idx], pattern, strlen(pattern)) == 0);
                    printf("DEBUG: LIKE (starts with) '%s' in '%s', result: %d\n", pattern, values[col_idx], result);
                    free(pattern);
                } else {
                    result = strcmp(values[col_idx], clean_val) == 0;
                    printf("DEBUG: LIKE (exact match) '%s' = '%s', result: %d\n", values[col_idx], clean_val, result);
                }
            } else if (strcmp(cond_op, "in") == 0) {
                char in_vals[256];
                strncpy(in_vals, clean_val, sizeof(in_vals) - 1);
                in_vals[sizeof(in_vals) - 1] = '\0';
                if (in_vals[0] != '(' || in_vals[strlen(in_vals) - 1] != ')') {
                    printf("DEBUG: Invalid IN format '%s', returning false (0)\n", in_vals);
                    for (int j = 0; j < cond_count; j++) {
                        if (conditions[j]) free(conditions[j]);
                    }
                    return 0;
                }
                in_vals[strlen(in_vals) - 1] = '\0';
                char *val = strtok(in_vals + 1, ",");
                result = 0;
                while (val) {
                    trim(val);
                    printf("DEBUG: Checking IN value '%s' against '%s'\n", val, values[col_idx]);
                    if (strcmp(values[col_idx], val) == 0) {
                        result = 1;
                        printf("DEBUG: Match found in IN, result: %d\n", result);
                        break;
                    }
                    val = strtok(NULL, ",");
                }
                printf("DEBUG: Final IN result: %d\n", result);
            } else {
                printf("DEBUG: Unsupported operator '%s', returning false (0)\n", cond_op);
                for (int j = 0; j < cond_count; j++) {
                    if (conditions[j]) free(conditions[j]);
                }
                return 0;
            }
        }
    }

    printf("DEBUG: Final result of evaluate_condition: %d\n", result);
    for (int j = 0; j < cond_count; j++) {
        if (conditions[j]) free(conditions[j]);
    }
    return result;
}
// SELECT
char* select_from_table(const char *db_name, const char *table_name, const char *columns, const char *condition) {
    // Tạo JSON object chính
    json_object *root = json_object_new_object();
    if (!root) {
        return strdup("{\"error\": \"Khong the tao JSON object.\"}");
    }

    // Kiểm tra database
    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database."));
        goto end;
    }

    // Tạo đường dẫn file meta và file dữ liệu
    char meta_path[256];
    snprintf(meta_path, sizeof(meta_path), "../data/%s/%s.meta", db_name, table_name);
    char data_path[256];
    snprintf(data_path, sizeof(data_path), "../data/%s/%s.tbl", db_name, table_name);

    // Kiểm tra file meta
    FILE *meta_file = fopen(meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai."));
        goto end;
    }

    // Đọc file meta để lấy danh sách cột
    char line[256];
    int col_count = 0;
    char *col_names[32] = {NULL};
    char *col_types[32] = {NULL};
    int line_number = 0;
    while (fgets(line, sizeof(line), meta_file)) {
        line_number++;
        trim(line);
        if (strlen(line) == 0) continue;

        char col_name[32], col_type[32];
        int scanned = sscanf(line, "%31s %31s", col_name, col_type);
        if (scanned != 2) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                if (col_names[i]) free(col_names[i]);
                if (col_types[i]) free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("File meta khong hop le."));
            goto end;
        }
        col_names[col_count] = strdup(col_name);
        col_types[col_count] = strdup(col_type);
        if (!col_names[col_count] || !col_types[col_count]) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                if (col_names[i]) free(col_names[i]);
                if (col_types[i]) free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("Khong the cap phat bo nho cho col_names hoac col_types."));
            goto end;
        }
        col_count++;
    }
    fclose(meta_file);

    // Kiểm tra danh sách cột
    if (col_count == 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong co cot nao."));
        goto end;
    }

    // Xử lý danh sách cột cần lấy
    int selected_cols[32];
    int selected_col_count = 0;
    if (strcmp(columns, "*") == 0) {
        for (int i = 0; i < col_count; i++) {
            selected_cols[i] = i;
        }
        selected_col_count = col_count;
    } else {
        char temp_cols[256];
        strncpy(temp_cols, columns, sizeof(temp_cols) - 1);
        temp_cols[sizeof(temp_cols) - 1] = '\0';
        char *token = strtok(temp_cols, ",");
        while (token) {
            trim(token);
            int found = 0;
            for (int i = 0; i < col_count; i++) {
                if (strcmp(col_names[i], token) == 0) {
                    selected_cols[selected_col_count++] = i;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                for (int i = 0; i < col_count; i++) {
                    free(col_names[i]);
                    free(col_types[i]);
                }
                json_object_object_add(root, "error", json_object_new_string("Cot khong ton tai."));
                goto end;
            }
            token = strtok(NULL, ",");
        }
    }

    // Tạo mảng "columns"
    json_object *cols_array = json_object_new_array();
    if (!cols_array) {
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the tao JSON array cho columns."));
        goto end;
    }
    for (int i = 0; i < selected_col_count; i++) {
        json_object_array_add(cols_array, json_object_new_string(col_names[selected_cols[i]]));
    }
    json_object_object_add(root, "columns", cols_array);

    // Đọc file dữ liệu
    FILE *data_file = fopen(data_path, "r");
    if (!data_file) {
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu."));
        goto end;
    }

    // Tạo mảng "rows"
    json_object *rows_array = json_object_new_array();
    if (!rows_array) {
        fclose(data_file);
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the tao JSON array cho rows."));
        goto end;
    }

    while (fgets(line, sizeof(line), data_file)) {
        trim(line);
        if (strlen(line) == 0) continue;

        char *values[32] = {NULL};
        int value_count = 0;
        char *token = strtok(line, "\t");
        while (token && value_count < col_count) {
            values[value_count++] = token;
            token = strtok(NULL, "\t");
        }

        // Đánh giá điều kiện
        if (evaluate_condition(condition, col_names, col_types, values, col_count)) {
            json_object *row = json_object_new_array();
            if (!row) {
                fclose(data_file);
                for (int i = 0; i < col_count; i++) {
                    free(col_names[i]);
                    free(col_types[i]);
                }
                json_object_put(rows_array);
                json_object_object_add(root, "error", json_object_new_string("Khong the tao JSON array cho row."));
                goto end;
            }
            for (int i = 0; i < selected_col_count; i++) {
                int idx = selected_cols[i];
                json_object_array_add(row, json_object_new_string(values[idx] ? values[idx] : ""));
            }
            json_object_array_add(rows_array, row);
        }
    }
    fclose(data_file);

    json_object_object_add(root, "rows", rows_array);

end:
    // Chuyển JSON thành chuỗi và giải phóng bộ nhớ
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    if (!result) {
        for (int i = 0; i < col_count; i++) {
            if (col_names[i]) free(col_names[i]);
            if (col_types[i]) free(col_types[i]);
        }
        json_object_put(root);
        return strdup("{\"error\": \"Khong the cap phat bo nho cho ket qua.\"}");
    }

    json_object_put(root);

    for (int i = 0; i < col_count; i++) {
        if (col_names[i]) free(col_names[i]);
        if (col_types[i]) free(col_types[i]);
    }

    return result;
}
// DELETE

char* delete_from_table(const char *db_name, const char *table_name, const char *condition) {
    // Tạo JSON object chính
    json_object *root = json_object_new_object();
    if (!root) {
        return strdup("{\"error\": \"Khong the tao JSON object.\"}");
    }

    // Kiểm tra database
    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database."));
        goto end;
    }

    // Tạo đường dẫn file meta, file dữ liệu và file tạm
    char meta_path[256], data_path[256], temp_path[256];
    snprintf(meta_path, sizeof(meta_path), "../data/%s/%s.meta", db_name, table_name);
    snprintf(data_path, sizeof(data_path), "../data/%s/%s.tbl", db_name, table_name);
    snprintf(temp_path, sizeof(temp_path), "../data/%s/%s.tbl.tmp", db_name, table_name);

    // Kiểm tra file meta và file dữ liệu
    if (access(meta_path, F_OK) != 0 || access(data_path, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai."));
        goto end;
    }

    // Đọc file meta
    FILE *meta_file = fopen(meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta."));
        goto end;
    }

    char line[256];
    int col_count = 0;
    char *col_names[32] = {NULL};
    char *col_types[32] = {NULL};
    int line_number = 0;
    while (fgets(line, sizeof(line), meta_file)) {
        line_number++;
        trim(line);
        if (strlen(line) == 0) continue;

        char col_name[32], col_type[32];
        int scanned = sscanf(line, "%31s %31s", col_name, col_type);
        if (scanned != 2) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                if (col_names[i]) free(col_names[i]);
                if (col_types[i]) free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("File meta khong hop le."));
            goto end;
        }
        col_names[col_count] = strdup(col_name);
        col_types[col_count] = strdup(col_type);
        if (!col_names[col_count] || !col_types[col_count]) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                if (col_names[i]) free(col_names[i]);
                if (col_types[i]) free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("Khong the cap phat bo nho cho col_names hoac col_types."));
            goto end;
        }
        col_count++;
    }
    fclose(meta_file);

    // Kiểm tra danh sách cột
    if (col_count == 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong co cot nao."));
        goto end;
    }

    // Đọc file dữ liệu
    FILE *data_file = fopen(data_path, "r");
    if (!data_file) {
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu."));
        goto end;
    }

    // Tạo file tạm
    FILE *temp_file = fopen(temp_path, "w");
    if (!temp_file) {
        fclose(data_file);
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file tam."));
        goto end;
    }

    // Xử lý xóa hàng
    int rows_deleted = 0;
    while (fgets(line, sizeof(line), data_file)) {
        trim(line);
        if (strlen(line) == 0) continue;

        char *values[32] = {NULL};
        int value_count = 0;
        char *token = strtok(line, "\t");
        while (token && value_count < col_count) {
            values[value_count++] = token;
            token = strtok(NULL, "\t");
        }

        // Kiểm tra số lượng giá trị
        if (value_count != col_count) {
            fclose(data_file);
            fclose(temp_file);
            remove(temp_path);
            for (int i = 0; i < col_count; i++) {
                free(col_names[i]);
                free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("Du lieu trong file khong hop le."));
            goto end;
        }

        // Đánh giá điều kiện
        if (!evaluate_condition(condition, col_names, col_types, values, col_count)) {
            // Giữ lại hàng không thỏa mãn điều kiện
            for (int j = 0; j < col_count; j++) {
                if (!values[j]) {
                    fclose(data_file);
                    fclose(temp_file);
                    remove(temp_path);
                    for (int i = 0; i < col_count; i++) {
                        free(col_names[i]);
                        free(col_types[i]);
                    }
                    json_object_object_add(root, "error", json_object_new_string("Gia tri trong file du lieu khong hop le."));
                    goto end;
                }
                if (fprintf(temp_file, "%s", values[j]) < 0) {
                    fclose(data_file);
                    fclose(temp_file);
                    remove(temp_path);
                    for (int i = 0; i < col_count; i++) {
                        free(col_names[i]);
                        free(col_types[i]);
                    }
                    json_object_object_add(root, "error", json_object_new_string("Loi khi ghi file tam."));
                    goto end;
                }
                if (j < col_count - 1) {
                    if (fprintf(temp_file, "\t") < 0) {
                        fclose(data_file);
                        fclose(temp_file);
                        remove(temp_path);
                        for (int i = 0; i < col_count; i++) {
                            free(col_names[i]);
                            free(col_types[i]);
                        }
                        json_object_object_add(root, "error", json_object_new_string("Loi khi ghi file tam."));
                        goto end;
                    }
                }
            }
            if (fprintf(temp_file, "\n") < 0) {
                fclose(data_file);
                fclose(temp_file);
                remove(temp_path);
                for (int i = 0; i < col_count; i++) {
                    free(col_names[i]);
                    free(col_types[i]);
                }
                json_object_object_add(root, "error", json_object_new_string("Loi khi ghi file tam."));
                goto end;
            }
        } else {
            rows_deleted++;
        }
    }

    fclose(data_file);
    fclose(temp_file);

    // Thay thế file gốc bằng file tạm
    if (rename(temp_path, data_path) != 0) {
        remove(temp_path);
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the cap nhat file du lieu."));
        goto end;
    }

    // Tạo thông báo kết quả
    char message[256];
    snprintf(message, sizeof(message), "Da xoa %d hang thanh cong.", rows_deleted);
    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string(message));

end:
    // Chuyển JSON thành chuỗi và giải phóng bộ nhớ
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    if (!result) {
        for (int i = 0; i < col_count; i++) {
            if (col_names[i]) free(col_names[i]);
            if (col_types[i]) free(col_types[i]);
        }
        json_object_put(root);
        return strdup("{\"error\": \"Khong the cap phat bo nho cho ket qua.\"}");
    }

    json_object_put(root);

    // Giải phóng bộ nhớ
    for (int i = 0; i < col_count; i++) {
        if (col_names[i]) free(col_names[i]);
        if (col_types[i]) free(col_types[i]);
    }

    return result;
}
// Hàm kiểm tra giá trị có hợp lệ với kiểu dữ liệu không
static int is_valid_value(const char *value, const char *type) {
    if (strcmp(type, "int") == 0) {
        char *endptr;
        strtol(value, &endptr, 10);
        return *endptr == '\0';
    } else if (strcmp(type, "text") == 0) {
        return 1; // text luôn hợp lệ
    } else if (strcmp(type, "date") == 0) {
        // Kiểm tra định dạng date (YYYY-MM-DD)
        int year, month, day;
        if (sscanf(value, "%d-%d-%d", &year, &month, &day) != 3) return 0;
        if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31) return 0;
        return 1;
    }
    return 0;
}
// UPDATE
char* update_table(const char *db_name, const char *table_name, const char *set_clause, const char *condition) {
    // Tạo JSON object chính
    json_object *root = json_object_new_object();
    if (!root) {
        return strdup("{\"error\": \"Khong the tao JSON object.\"}");
    }

    // Kiểm tra database
    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database."));
        goto end;
    }

    // Tạo đường dẫn file meta và file dữ liệu
    char meta_path[256], data_path[256], temp_data_path[256];
    snprintf(meta_path, sizeof(meta_path), "../data/%s/%s.meta", db_name, table_name);
    snprintf(data_path, sizeof(data_path), "../data/%s/%s.tbl", db_name, table_name);
    snprintf(temp_data_path, sizeof(temp_data_path), "../data/%s/%s.tbl.tmp", db_name, table_name);

    // Kiểm tra file meta
    if (access(meta_path, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai."));
        goto end;
    }

    // Đọc file meta
    FILE *meta_file = fopen(meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta."));
        goto end;
    }

    char line[256];
    int col_count = 0;
    char *col_names[32] = {NULL};
    char *col_types[32] = {NULL};
    while (fgets(line, sizeof(line), meta_file)) {
        trim(line);
        if (strlen(line) == 0) continue;

        char col_name[32], col_type[32];
        if (sscanf(line, "%31s %31s", col_name, col_type) != 2) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                free(col_names[i]);
                free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("File meta khong hop le."));
            goto end;
        }
        col_names[col_count] = strdup(col_name);
        col_types[col_count] = strdup(col_type);
        if (!col_names[col_count] || !col_types[col_count]) {
            fclose(meta_file);
            for (int i = 0; i < col_count; i++) {
                free(col_names[i]);
                free(col_types[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("Khong the cap phat bo nho cho col_names hoac col_types."));
            goto end;
        }
        printf("DEBUG: update_table - Loaded column %d: name='%s', type='%s'\n", col_count, col_names[col_count], col_types[col_count]);
        col_count++;
    }
    fclose(meta_file);

    // Kiểm tra danh sách cột
    if (col_count == 0) {
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Bang khong co cot nao."));
        goto end;
    }

    // Phân tích set_clause
    char set_copy[256];
    strncpy(set_copy, set_clause, sizeof(set_copy) - 1);
    set_copy[sizeof(set_copy) - 1] = '\0';

    char *set_items[32] = {NULL};
    int set_count = 0;
    char *token = strtok(set_copy, ",");
    while (token && set_count < 32) {
        trim(token);
        set_items[set_count++] = token;
        token = strtok(NULL, ",");
    }

    // Kiểm tra từng cặp column = value trong set_clause
    int col_indices[32];
    char *new_values[32];
    for (int i = 0; i < set_count; i++) {
        char *set_item = set_items[i];
        char *eq_pos = strchr(set_item, '=');
        if (!eq_pos) {
            for (int j = 0; j < col_count; j++) {
                free(col_names[j]);
                free(col_types[j]);
            }
            json_object_object_add(root, "error", json_object_new_string("Cu phap SET khong hop le, can 'column = value'."));
            goto end;
        }

        // Tách column và value
        size_t col_len = eq_pos - set_item;
        char col_name[32] = "";
        strncpy(col_name, set_item, col_len);
        col_name[col_len] = '\0';
        trim(col_name);

        char *value = eq_pos + 1;
        while (*value == ' ') value++; // Bỏ qua khoảng trắng
        trim(value);
        if (value[0] == '"' && value[strlen(value) - 1] == '"') {
            value[strlen(value) - 1] = '\0';
            value++;
        }

        // Tìm cột trong danh sách cột
        int col_idx = -1;
        for (int j = 0; j < col_count; j++) {
            if (strcmp(col_names[j], col_name) == 0) {
                col_idx = j;
                break;
            }
        }
        if (col_idx == -1) {
            char error_msg[256];
            snprintf(error_msg, sizeof(error_msg), "Cot %s khong ton tai.", col_name);
            for (int j = 0; j < col_count; j++) {
                free(col_names[j]);
                free(col_types[j]);
            }
            json_object_object_add(root, "error", json_object_new_string(error_msg));
            goto end;
        }

        // Kiểm tra giá trị hợp lệ với kiểu dữ liệu
        if (!is_valid_value(value, col_types[col_idx])) {
            char error_msg[256];
            snprintf(error_msg, sizeof(error_msg), "Gia tri khong hop le voi kieu %s.", col_types[col_idx]);
            for (int j = 0; j < col_count; j++) {
                free(col_names[j]);
                free(col_types[j]);
            }
            json_object_object_add(root, "error", json_object_new_string(error_msg));
            goto end;
        }

        col_indices[i] = col_idx;
        new_values[i] = strdup(value);
    }

    // Cập nhật file dữ liệu
    FILE *data_file = fopen(data_path, "r");
    if (!data_file) {
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        for (int i = 0; i < set_count; i++) {
            free(new_values[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu."));
        goto end;
    }

    FILE *temp_data_file = fopen(temp_data_path, "w");
    if (!temp_data_file) {
        fclose(data_file);
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        for (int i = 0; i < set_count; i++) {
            free(new_values[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file tam."));
        goto end;
    }

    int updated_rows = 0;
    while (fgets(line, sizeof(line), data_file)) {
        trim(line);
        if (strlen(line) == 0) continue;

        char *values[32] = {NULL};
        int value_count = 0;
        char *token = strtok(line, "\t");
        while (token && value_count < col_count) {
            values[value_count++] = token;
            token = strtok(NULL, "\t");
        }

        if (value_count != col_count) {
            fclose(data_file);
            fclose(temp_data_file);
            remove(temp_data_path);
            for (int i = 0; i < col_count; i++) {
                free(col_names[i]);
                free(col_types[i]);
            }
            for (int i = 0; i < set_count; i++) {
                free(new_values[i]);
            }
            json_object_object_add(root, "error", json_object_new_string("Du lieu trong file khong hop le."));
            goto end;
        }

        // Đánh giá điều kiện WHERE
        if (evaluate_condition(condition, col_names, col_types, values, col_count)) {
            updated_rows++;
            // Cập nhật các cột được chỉ định trong set_clause
            for (int i = 0; i < set_count; i++) {
                values[col_indices[i]] = new_values[i];
            }
        }

        // Ghi lại hàng vào file tạm
        for (int j = 0; j < col_count; j++) {
            fprintf(temp_data_file, "%s", values[j]);
            if (j < col_count - 1) fprintf(temp_data_file, "\t");
        }
        fprintf(temp_data_file, "\n");
    }

    fclose(data_file);
    fclose(temp_data_file);

    if (rename(temp_data_path, data_path) != 0) {
        remove(temp_data_path);
        for (int i = 0; i < col_count; i++) {
            free(col_names[i]);
            free(col_types[i]);
        }
        for (int i = 0; i < set_count; i++) {
            free(new_values[i]);
        }
        json_object_object_add(root, "error", json_object_new_string("Khong the cap nhat file du lieu."));
        goto end;
    }

    char message[256];
    snprintf(message, sizeof(message), "Da cap nhat %d hang thanh cong.", updated_rows);
    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string(message));

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);

    for (int i = 0; i < col_count; i++) {
        free(col_names[i]);
        free(col_types[i]);
    }
    for (int i = 0; i < set_count; i++) {
        free(new_values[i]);
    }

    return result;
}
// ALTER
char* alter_table(const char *db_name, const char *table_name, const char *operation, const char *column_info, const char *new_name) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char meta_path[256], data_path[256], temp_meta_path[256], temp_data_path[256];
    snprintf(meta_path, sizeof(meta_path), "%s%s/%s.meta", DATA_DIR, db_name, table_name);
    snprintf(data_path, sizeof(data_path), "%s%s/%s.tbl", DATA_DIR, db_name, table_name);
    snprintf(temp_meta_path, sizeof(temp_meta_path), "%s%s/%s.meta.tmp", DATA_DIR, db_name, table_name);
    snprintf(temp_data_path, sizeof(temp_data_path), "%s%s/%s.tbl.tmp", DATA_DIR, db_name, table_name);

    if (access(meta_path, F_OK) != 0 || access(data_path, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai"));
        goto end;
    }

    FILE *meta_file = fopen(meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta"));
        goto end;
    }
    char col_names[10][50];
    char col_types[10][50];
    int col_count = 0;
    char line[256];
    while (fgets(line, sizeof(line), meta_file)) {
        trim(line);
        if (strlen(line) == 0) continue;
        if (sscanf(line, "%49s %49s", col_names[col_count], col_types[col_count]) == 2) {
            col_count++;
        }
    }
    fclose(meta_file);

    if (strcmp(operation, "add") == 0) {
        char col_name[50], col_type[50];
        // Tạo bản sao của column_info để tránh thay đổi chuỗi gốc
        char column_info_copy[256];
        strncpy(column_info_copy, column_info, sizeof(column_info_copy) - 1);
        column_info_copy[sizeof(column_info_copy) - 1] = '\0';
        trim(column_info_copy); // Loại bỏ khoảng trắng thừa
        printf("DEBUG: alter_table - column_info: '%s'\n", column_info_copy);

        // Phân tích column_info thành col_name và col_type
        if (sscanf(column_info_copy, "%49s %49s", col_name, col_type) != 2) {
            json_object_object_add(root, "error", json_object_new_string("Dinh dang cot khong hop le"));
            goto end;
        }
        printf("DEBUG: alter_table - Parsed new column: name='%s', type='%s'\n", col_name, col_type);

        // Kiểm tra kiểu dữ liệu hợp lệ
        if (strcmp(col_type, "int") != 0 && strcmp(col_type, "text") != 0 && strcmp(col_type, "date") != 0) {
            json_object_object_add(root, "error", json_object_new_string("Kieu du lieu khong hop le"));
            goto end;
        }

        // Kiểm tra xem cột đã tồn tại chưa
        for (int i = 0; i < col_count; i++) {
            if (strcmp(col_names[i], col_name) == 0) {
                json_object_object_add(root, "error", json_object_new_string("Cot da ton tai"));
                goto end;
            }
        }

        // Thêm cột mới vào file meta
        FILE *new_meta_file = fopen(temp_meta_path, "w");
        if (!new_meta_file) {
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta tam"));
            goto end;
        }
        for (int i = 0; i < col_count; i++) {
            fprintf(new_meta_file, "%s %s\n", col_names[i], col_types[i]);
        }
        fprintf(new_meta_file, "%s %s\n", col_name, col_type);
        fclose(new_meta_file);

        // Cập nhật file dữ liệu: thêm giá trị mặc định (NULL) cho cột mới
        FILE *data_file = fopen(data_path, "r");
        if (!data_file) {
            remove(temp_meta_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu"));
            goto end;
        }
        FILE *temp_data_file = fopen(temp_data_path, "w");
        if (!temp_data_file) {
            fclose(data_file);
            remove(temp_meta_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu tam"));
            goto end;
        }

        while (fgets(line, sizeof(line), data_file)) {
            line[strcspn(line, "\n")] = 0;
            char *token = strtok(line, "\t");
            int i = 0;
            while (token && i < col_count) {
                fprintf(temp_data_file, "%s\t", token);
                token = strtok(NULL, "\t");
                i++;
            }
            if (i != col_count) {
                fclose(data_file);
                fclose(temp_data_file);
                remove(temp_meta_path);
                remove(temp_data_path);
                json_object_object_add(root, "error", json_object_new_string("Du lieu trong file khong hop le"));
                goto end;
            }
            fprintf(temp_data_file, "NULL\n");
        }
        fclose(data_file);
        fclose(temp_data_file);

        if (rename(temp_meta_path, meta_path) != 0 || rename(temp_data_path, data_path) != 0) {
            remove(temp_meta_path);
            remove(temp_data_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the cap nhat file"));
            goto end;
        }

        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Cot da duoc them thanh cong"));
    } else if (strcmp(operation, "drop") == 0) {
        int col_idx = -1;
        for (int i = 0; i < col_count; i++) {
            if (strcmp(col_names[i], column_info) == 0) {
                col_idx = i;
                break;
            }
        }
        if (col_idx == -1) {
            json_object_object_add(root, "error", json_object_new_string("Cot khong ton tai"));
            goto end;
        }

        FILE *new_meta_file = fopen(temp_meta_path, "w");
        if (!new_meta_file) {
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta tam"));
            goto end;
        }
        for (int i = 0; i < col_count; i++) {
            if (i != col_idx) {
                fprintf(new_meta_file, "%s %s\n", col_names[i], col_types[i]);
            }
        }
        fclose(new_meta_file);

        FILE *data_file = fopen(data_path, "r");
        if (!data_file) {
            remove(temp_meta_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu"));
            goto end;
        }
        FILE *temp_data_file = fopen(temp_data_path, "w");
        if (!temp_data_file) {
            fclose(data_file);
            remove(temp_meta_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file du lieu tam"));
            goto end;
        }

        char line[1024];
        while (fgets(line, sizeof(line), data_file)) {
            line[strcspn(line, "\n")] = 0;
            char *token = strtok(line, "\t");
            int i = 0;
            while (token && i < col_count) {
                if (i != col_idx) {
                    fprintf(temp_data_file, "%s", token);
                    if (i < col_count - 1 && i + 1 != col_idx) {
                        fprintf(temp_data_file, "\t");
                    }
                }
                token = strtok(NULL, "\t");
                i++;
            }
            if (i != col_count) {
                fclose(data_file);
                fclose(temp_data_file);
                remove(temp_meta_path);
                remove(temp_data_path);
                json_object_object_add(root, "error", json_object_new_string("Du lieu trong file khong hop le"));
                goto end;
            }
            fprintf(temp_data_file, "\n");
        }
        fclose(data_file);
        fclose(temp_data_file);

        if (rename(temp_meta_path, meta_path) != 0 || rename(temp_data_path, data_path) != 0) {
            remove(temp_meta_path);
            remove(temp_data_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the cap nhat file"));
            goto end;
        }

        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Cot da duoc xoa thanh cong"));
    } else if (strcmp(operation, "rename") == 0) {
        int col_idx = -1;
        for (int i = 0; i < col_count; i++) {
            if (strcmp(col_names[i], column_info) == 0) {
                col_idx = i;
                break;
            }
        }
        if (col_idx == -1) {
            json_object_object_add(root, "error", json_object_new_string("Cot khong ton tai"));
            goto end;
        }
        for (int i = 0; i < col_count; i++) {
            if (strcmp(col_names[i], new_name) == 0) {
                json_object_object_add(root, "error", json_object_new_string("Ten cot moi da ton tai"));
                goto end;
            }
        }

        FILE *new_meta_file = fopen(temp_meta_path, "w");
        if (!new_meta_file) {
            json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta tam"));
            goto end;
        }
        for (int i = 0; i < col_count; i++) {
            if (i == col_idx) {
                fprintf(new_meta_file, "%s %s\n", new_name, col_types[i]);
            } else {
                fprintf(new_meta_file, "%s %s\n", col_names[i], col_types[i]);
            }
        }
        fclose(new_meta_file);

        if (rename(temp_meta_path, meta_path) != 0) {
            remove(temp_meta_path);
            json_object_object_add(root, "error", json_object_new_string("Khong the cap nhat file meta"));
            goto end;
        }

        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Cot da duoc doi ten thanh cong"));
    } else {
        json_object_object_add(root, "error", json_object_new_string("Thao tac ALTER khong hop le"));
    }

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    if (!result) {
        json_object_put(root);
        return strdup("{\"error\": \"Khong the cap phat bo nho cho ket qua\"}");
    }
    json_object_put(root);
    return result;
}
// DESCRIBE
char* describe_table(const char *db_name, const char *table_name) {
    json_object *root = json_object_new_object();

    if (strlen(db_name) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Chua chon database"));
        goto end;
    }

    char meta_path[256];
    snprintf(meta_path, sizeof(meta_path), "%s%s/%s.meta", DATA_DIR, db_name, table_name);

    if (access(meta_path, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong ton tai"));
        goto end;
    }

    FILE *meta_file = fopen(meta_path, "r");
    if (!meta_file) {
        json_object_object_add(root, "error", json_object_new_string("Khong the mo file meta"));
        goto end;
    }

    json_object *columns = json_object_new_array();
    char col_name[50], col_type[50];
    while (fscanf(meta_file, "%49s %49s", col_name, col_type) == 2) {
        json_object *col_info = json_object_new_object();
        json_object_object_add(col_info, "name", json_object_new_string(col_name));
        json_object_object_add(col_info, "type", json_object_new_string(col_type));
        json_object_array_add(columns, col_info);
    }
    fclose(meta_file);

    if (json_object_array_length(columns) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Bang khong co cot nao"));
        json_object_put(columns);
        goto end;
    }

    json_object_object_add(root, "columns", columns);

end:
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    if (!result) {
        json_object_put(root);
        return strdup("{\"error\": \"Khong the cap phat bo nho cho ket qua\"}");
    }
    json_object_put(root);
    return result;
}
