#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <unistd.h>
#include "../include/parser.h" // Để gọi execute_command

#define PORT 8080 // Cổng server lắng nghe


// Hàm xử lý yêu cầu từ client
void *handle_client(void *client_socket_ptr) {
    int client_socket = *(int *)client_socket_ptr;
    free(client_socket_ptr);
    // Khoi tao phien lam viec cho client
    ClientSession session;
    session.socket = client_socket;
    strcpy(session.current_db, "");

    char buffer[1024];
    
    while (1) {
        memset(buffer, 0, sizeof(buffer)); // Xóa buffer trước khi nhận dữ liệu mới
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            // Client đã đóng kết nối hoặc có lỗi
            break;
        }
        
        printf("Received query from client %d: %s\n", client_socket, buffer);
        
        // Thực thi lệnh và nhận kết quả JSON
        char *result = execute_command(buffer, &session);
        send(client_socket, result, strlen(result), 0);
        free(result); // Giải phóng bộ nhớ của chuỗi JSON
    }

    // Đóng kết nối khi client ngắt kết nối
    close(client_socket);
    return NULL;
}

int main() {
    int server_fd, *client_socket_ptr;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // Tạo socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Thiết lập địa chỉ server
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Chấp nhận từ mọi IP
    address.sin_port = htons(PORT);

    // Gắn socket vào cổng
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Lắng nghe kết nối
    if (listen(server_fd, 10) < 0) { // Tối đa 10 kết nối chờ
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Server is listening on port %d...\n", PORT);

    // Chấp nhận kết nối từ client
    while (1) {
        client_socket_ptr = malloc(sizeof(int));
        *client_socket_ptr = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
        if (*client_socket_ptr < 0) {
            perror("Accept failed");
            free(client_socket_ptr);
            continue;
        }

        // Tạo thread để xử lý client
        pthread_t thread;
        pthread_create(&thread, NULL, handle_client, client_socket_ptr);
        pthread_detach(thread); // Để thread tự giải phóng khi hoàn thành
    }

    close(server_fd);
    return 0;
}
