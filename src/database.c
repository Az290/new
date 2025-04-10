#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>
#include <sys/stat.h>
#include <json-c/json.h>
#include "../include/database.h"

#define DB_ROOT "../data/"

// Hàm tạo database
char* create_database(const char *dbname) {
    char dbdir[256];
    snprintf(dbdir, sizeof(dbdir), "%s%s", DB_ROOT, dbname);
    
    json_object *root = json_object_new_object();
    
    if (mkdir(dbdir, 0755) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Loi khi tao csdl. Co the thu muc da ton tai."));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }

    char list_file[256];
    snprintf(list_file, sizeof(list_file), "%s/tables.list", dbdir);
    FILE *file = fopen(list_file, "w");
    if (file) {
        fclose(file);
        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Co so du lieu duoc tao thanh cong"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    } else {
        json_object_object_add(root, "error", json_object_new_string("Ko the tao file tables.list"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }
}

// Hàm hiển thị tất cả database
char* show_database() {
    DIR *d = opendir(DB_ROOT);
    json_object *root = json_object_new_object();
    
    if (!d) {
        json_object_object_add(root, "error", json_object_new_string("Loi khi mo thu muc './data'"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }

    json_object *db_array = json_object_new_array();
    struct dirent *dir;
    int count = 0;
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_type == DT_DIR && strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0) {
            json_object_array_add(db_array, json_object_new_string(dir->d_name));
            count++;
        }
    }
    closedir(d);

    if (count == 0) {
        json_object_object_add(root, "message", json_object_new_string("Ko co co so du lieu nao"));
    } else {
        json_object_object_add(root, "databases", db_array);
    }
    
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

// Hàm xóa database
char* drop_database(const char *dbname) {
    char dbdir[256];
    snprintf(dbdir, sizeof(dbdir), "%s%s", DB_ROOT, dbname);
    
    json_object *root = json_object_new_object();
    
    if (access(dbdir, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Co so du lieu ko ton tai"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }

    DIR *d = opendir(dbdir);
    if (d) {
        struct dirent *dir;
        while ((dir = readdir(d)) != NULL) {
            if (strstr(dir->d_name, ".tbl") || strstr(dir->d_name, ".meta") || strcmp(dir->d_name, "tables.list") == 0) {
                char filepath[256];
                snprintf(filepath, sizeof(filepath), "%s/%s", dbdir, dir->d_name);
                remove(filepath);
            }
        }
        closedir(d);
    } else {
        json_object_object_add(root, "error", json_object_new_string("Ko the mo thu muc"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }

    if (rmdir(dbdir) == 0) {
        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Co so du lieu da bi xoa"));
    } else {
        json_object_object_add(root, "error", json_object_new_string("Ko the xoa thu muc"));
    }
    
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

// Hàm đổi tên database
char* rename_database(const char *old_dbname, const char *new_dbname) {
    char old_dbdir[256], new_dbdir[256];
    snprintf(old_dbdir, sizeof(old_dbdir), "%s%s", DB_ROOT, old_dbname);
    snprintf(new_dbdir, sizeof(new_dbdir), "%s%s", DB_ROOT, new_dbname);
    
    json_object *root = json_object_new_object();
    
    if (access(old_dbdir, F_OK) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Co so du lieu cu ko ton tai"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }
    if (access(new_dbdir, F_OK) == 0) {
        json_object_object_add(root, "error", json_object_new_string("Co so du lieu moi da ton tai"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }
    if (rename(old_dbdir, new_dbdir) != 0) {
        json_object_object_add(root, "error", json_object_new_string("Loi ko the doi ten co so du lieu"));
        const char *json_str = json_object_to_json_string(root);
        char *result = strdup(json_str);
        json_object_put(root);
        return result;
    }
    
    json_object_object_add(root, "status", json_object_new_string("success"));
    json_object_object_add(root, "message", json_object_new_string("Co so du lieu da duoc doi ten thanh cong"));
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}

// Hàm kiểm tra sự tồn tại của database
char* check_database_exists(const char *dbname) {
    char dbdir[256];
    snprintf(dbdir, sizeof(dbdir), "%s%s", DB_ROOT, dbname);
    struct stat st = {0};
    
    json_object *root = json_object_new_object();
    
    if (stat(dbdir, &st) == -1) {
        json_object_object_add(root, "error", json_object_new_string("Co so du lieu ko ton tai"));
    } else {
        json_object_object_add(root, "status", json_object_new_string("success"));
        json_object_object_add(root, "message", json_object_new_string("Co so du lieu ton tai"));
    }
    
    const char *json_str = json_object_to_json_string(root);
    char *result = strdup(json_str);
    json_object_put(root);
    return result;
}
