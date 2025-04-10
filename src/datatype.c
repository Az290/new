#include "../include/datatype.h"
#include <stdio.h>
#include <string.h>
#include <ctype.h>

// Loại bỏ khoảng trắng thừa ở đầu và cuối chuỗi
static void trim(char *str) {
    char *start = str;
    while (*start == ' ') start++;
    char *end = str + strlen(str) - 1;
    while (end >= start && *end == ' ') *end-- = '\0';
    memmove(str, start, strlen(start) + 1);
}

// Kiểm tra ngày hợp lệ
static int is_valid_date(const char *value) {
    if (strlen(value) != 10 || value[4] != '-' || value[7] != '-') return 0;

    int year, month, day;
    if (sscanf(value, "%4d-%2d-%2d", &year, &month, &day) != 3) return 0;

    if (year < 1900 || year > 9999) return 0;
    if (month < 1 || month > 12) return 0;
    if (day < 1 || day > 31) return 0;

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        if (day > 29) return 0; // Năm nhuận
    } else if (day > days_in_month[month - 1]) return 0;

    return 1;
}

int validate_data(DataType type, const char *value) {
    char temp[256];
    strncpy(temp, value, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    trim(temp); // Loại bỏ khoảng trắng thừa

    size_t len = strlen(temp);
    switch (type) {
        case TYPE_INT: {
            if (len == 1 && temp[0] == '-') {
                printf("Loi: Gia tri '%s' khong phai so nguyen!\n", value);
                return 0;
            }
            for (int i = (temp[0] == '-' ? 1 : 0); i < len; i++) {
                if (!isdigit(temp[i])) {
                    printf("Loi: Gia tri '%s' khong phai so nguyen!\n", value);
                    return 0;
                }
            }
            break;
        }
        case TYPE_TEXT:
            if (len < 2 || temp[0] != '"' || temp[len - 1] != '"') {
                printf("Loi: Gia tri '%s' cua kieu TEXT phai duoc bao quanh boi dau \" \"\n", value);
                return 0;
            }
            break;
        case TYPE_DATE:
            if (!is_valid_date(temp)) {
                printf("Loi: Gia tri '%s' khong phai ngay hop le (YYYY-MM-DD)!\n", value);
                return 0;
            }
            break;
        default:
            printf("Loi: Kieu du lieu khong hop le!\n");
            return 0;
    }
    return 1;
}

