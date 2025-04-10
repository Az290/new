#include <gtk/gtk.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <json-c/json.h>

#define SERVER_PORT 8080
#define SERVER_IP "127.0.0.1"

typedef struct {
    GtkWidget *window;
    GtkWidget *query_entry;
    GtkWidget *result_text_view;
    GtkWidget *db_label;
    GtkTextBuffer *result_buffer;
    int client_socket;
    char current_db[256];
} AppData;

// Hàm để hiển thị kết quả từ server
void display_result(AppData *app_data, const char *result) {
    json_object *json_obj = json_tokener_parse(result);
    if (!json_obj) {
        gtk_text_buffer_set_text(app_data->result_buffer, "Lỗi phân tích JSON", -1);
        return;
    }

    json_object *error_obj;
    if (json_object_object_get_ex(json_obj, "error", &error_obj)) {
        const char *error_msg = json_object_get_string(error_obj);
        gtk_text_buffer_set_text(app_data->result_buffer, error_msg, -1);
        json_object_put(json_obj);
        return;
    }

    json_object *message_obj;
    if (json_object_object_get_ex(json_obj, "message", &message_obj)) {
        const char *message = json_object_get_string(message_obj);
        if (strstr(message, "Da chon database") != NULL) {
            const char *db_name = strstr(message, "Da chon database") + 17;
            snprintf(app_data->current_db, sizeof(app_data->current_db), "%s", db_name);
            char label_text[300];
            snprintf(label_text, sizeof(label_text), "Database hiện tại: %s", app_data->current_db);
            gtk_label_set_text(GTK_LABEL(app_data->db_label), label_text);
        }
    }

    const char *formatted_result = json_object_to_json_string_ext(json_obj, JSON_C_TO_STRING_PRETTY);
    gtk_text_buffer_set_text(app_data->result_buffer, formatted_result, -1);
    
    json_object_put(json_obj);
}

// Hàm xử lý khi nút thực thi được nhấn
void on_execute_clicked(__attribute__((unused)) GtkWidget *button, AppData *app_data) {
    const char *query = gtk_entry_get_text(GTK_ENTRY(app_data->query_entry));
    
    if (strlen(query) == 0) {
        gtk_text_buffer_set_text(app_data->result_buffer, "Hãy nhập truy vấn", -1);
        return;
    }

    if (strcmp(query, "exit") == 0 || strcmp(query, "quit") == 0) {
        gtk_main_quit();
        return;
    }

    if (send(app_data->client_socket, query, strlen(query), 0) < 0) {
        gtk_text_buffer_set_text(app_data->result_buffer, "Lỗi khi gửi truy vấn", -1);
        return;
    }

    char buffer[4096] = {0};
    int bytes_read = read(app_data->client_socket, buffer, sizeof(buffer) - 1);
    if (bytes_read < 0) {
        gtk_text_buffer_set_text(app_data->result_buffer, "Lỗi khi nhận dữ liệu", -1);
        return;
    } else if (bytes_read == 0) {
        gtk_text_buffer_set_text(app_data->result_buffer, "Server đã đóng kết nối", -1);
        return;
    }

    display_result(app_data, buffer);
    
    gtk_entry_set_text(GTK_ENTRY(app_data->query_entry), "");
}

// Hàm xử lý khi một mục menu được chọn
void on_menu_item_activate(GtkMenuItem *menu_item, AppData *app_data) {
    const char *text = g_object_get_data(G_OBJECT(menu_item), "text");
    gtk_entry_set_text(GTK_ENTRY(app_data->query_entry), text);
}

// Hàm tạo menu với các lệnh thông dụng
void create_menu(GtkWidget *vbox, AppData *app_data) {
    GtkWidget *menubar = gtk_menu_bar_new();
    
    // Menu Database
    GtkWidget *db_menu = gtk_menu_new();
    GtkWidget *db_item = gtk_menu_item_new_with_label("Database");
    gtk_menu_item_set_submenu(GTK_MENU_ITEM(db_item), db_menu);
    
    GtkWidget *show_db = gtk_menu_item_new_with_label("Hiển thị tất cả DB");
    g_object_set_data(G_OBJECT(show_db), "text", "showdb");
    
    GtkWidget *create_db = gtk_menu_item_new_with_label("Tạo DB mới");
    g_object_set_data(G_OBJECT(create_db), "text", "credb ");
    
    gtk_menu_shell_append(GTK_MENU_SHELL(db_menu), show_db);
    gtk_menu_shell_append(GTK_MENU_SHELL(db_menu), create_db);
    
    // Menu Table
    GtkWidget *table_menu = gtk_menu_new();
    GtkWidget *table_item = gtk_menu_item_new_with_label("Bảng");
    gtk_menu_item_set_submenu(GTK_MENU_ITEM(table_item), table_menu);
    
    GtkWidget *show_table = gtk_menu_item_new_with_label("Hiển thị tất cả bảng");
    g_object_set_data(G_OBJECT(show_table), "text", "showtbl");
    
    GtkWidget *create_table = gtk_menu_item_new_with_label("Tạo bảng mới");
    g_object_set_data(G_OBJECT(create_table), "text", "cretbl ");
    
    gtk_menu_shell_append(GTK_MENU_SHELL(table_menu), show_table);
    gtk_menu_shell_append(GTK_MENU_SHELL(table_menu), create_table);
    
    gtk_menu_shell_append(GTK_MENU_SHELL(menubar), db_item);
    gtk_menu_shell_append(GTK_MENU_SHELL(menubar), table_item);
    
    gtk_box_pack_start(GTK_BOX(vbox), menubar, FALSE, FALSE, 0);
    
    // Kết nối signal chỉ với on_menu_item_activate
    g_signal_connect(show_db, "activate", G_CALLBACK(on_menu_item_activate), app_data);
    g_signal_connect(create_db, "activate", G_CALLBACK(on_menu_item_activate), app_data);
    g_signal_connect(show_table, "activate", G_CALLBACK(on_menu_item_activate), app_data);
    g_signal_connect(create_table, "activate", G_CALLBACK(on_menu_item_activate), app_data);
}

// Hàm tạo giao diện người dùng
void create_ui(AppData *app_data) {
    app_data->window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
    gtk_window_set_title(GTK_WINDOW(app_data->window), "Yocto-SQL Client");
    gtk_window_set_default_size(GTK_WINDOW(app_data->window), 800, 600);
    g_signal_connect(app_data->window, "destroy", G_CALLBACK(gtk_main_quit), NULL);
    
    GtkWidget *vbox = gtk_box_new(GTK_ORIENTATION_VERTICAL, 5);
    gtk_container_add(GTK_CONTAINER(app_data->window), vbox);
    
    create_menu(vbox, app_data);
    
    app_data->db_label = gtk_label_new("Database hiện tại: None");
    gtk_box_pack_start(GTK_BOX(vbox), app_data->db_label, FALSE, FALSE, 5);
    
    GtkWidget *query_frame = gtk_frame_new("Nhập truy vấn SQL:");
    gtk_box_pack_start(GTK_BOX(vbox), query_frame, FALSE, FALSE, 5);
    
    GtkWidget *query_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 5);
    gtk_container_add(GTK_CONTAINER(query_frame), query_box);
    
    app_data->query_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(app_data->query_entry), "Nhập truy vấn SQL hoặc 'exit' để thoát");
    gtk_box_pack_start(GTK_BOX(query_box), app_data->query_entry, TRUE, TRUE, 5);
    
    GtkWidget *execute_button = gtk_button_new_with_label("Thực thi");
    gtk_box_pack_start(GTK_BOX(query_box), execute_button, FALSE, FALSE, 5);
    g_signal_connect(execute_button, "clicked", G_CALLBACK(on_execute_clicked), app_data);
    
    GtkWidget *result_frame = gtk_frame_new("Kết quả:");
    gtk_box_pack_start(GTK_BOX(vbox), result_frame, TRUE, TRUE, 5);
    
    GtkWidget *scrolled_window = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(scrolled_window),
                                  GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);
    gtk_container_add(GTK_CONTAINER(result_frame), scrolled_window);
    
    app_data->result_text_view = gtk_text_view_new();
    app_data->result_buffer = gtk_text_view_get_buffer(GTK_TEXT_VIEW(app_data->result_text_view));
    gtk_text_view_set_editable(GTK_TEXT_VIEW(app_data->result_text_view), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(app_data->result_text_view), GTK_WRAP_WORD);
    gtk_container_add(GTK_CONTAINER(scrolled_window), app_data->result_text_view);
    
    gtk_widget_show_all(app_data->window);
    
    gtk_widget_grab_focus(app_data->query_entry);
}

// Hàm kết nối đến server
int connect_to_server() {
    int client_socket;
    struct sockaddr_in server_addr;
    
    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        perror("Socket creation failed");
        return -1;
    }
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr(SERVER_IP);
    
    if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(client_socket);
        return -1;
    }
    
    return client_socket;
}

int main(int argc, char *argv[]) {
    gtk_init(&argc, &argv);
    
    AppData app_data;
    memset(&app_data, 0, sizeof(AppData));
    
    app_data.client_socket = connect_to_server();
    if (app_data.client_socket < 0) {
        GtkWidget *dialog = gtk_message_dialog_new(NULL,
                                                  GTK_DIALOG_MODAL,
                                                  GTK_MESSAGE_ERROR,
                                                  GTK_BUTTONS_OK,
                                                  "Không thể kết nối đến server. Vui lòng đảm bảo server đang chạy.");
        gtk_dialog_run(GTK_DIALOG(dialog));
        gtk_widget_destroy(dialog);
        return 1;
    }
    
    create_ui(&app_data);
    
    gtk_main();
    
    close(app_data.client_socket);
    
    return 0;
}
