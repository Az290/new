#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#define SERVER_PORT 8080
#define SERVER_IP "127.0.0.1"

int main() {
    int client_socket;
    struct sockaddr_in server_addr;
    char buffer[1024] = {0};
    char query[256];

    // Tạo socket
    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Thiết lập địa chỉ server
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    server_addr.sin_addr.s_addr = inet_addr(SERVER_IP);

    // Kết nối đến server
    if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(client_socket);
        exit(EXIT_FAILURE);
    }

    printf("Kết nối đến server thành công. Nhập truy vấn SQL (hoặc 'exit' để thoát, hoặc 'quit'):\n");

    // Vòng lặp để nhận và gửi nhiều truy vấn
    while (1) {
        printf("Nhập truy vấn SQL: ");
        fgets(query, sizeof(query), stdin);
        query[strcspn(query, "\n")] = 0;  // Xóa ký tự xuống dòng

        // Kiểm tra lệnh thoát
        if (strcmp(query, "exit") == 0 || strcmp(query, "quit") == 0) {
            printf("Thoát client.\n");
            break;
        }

        // Gửi truy vấn đến server
        if (send(client_socket, query, strlen(query), 0) < 0) {
            perror("Gửi truy vấn thất bại");
            break;
        }

        // Nhận và in phản hồi từ server
        memset(buffer, 0, sizeof(buffer));  // Xóa buffer trước khi nhận
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read < 0) {
            perror("Nhận dữ liệu thất bại");
            break;
        } else if (bytes_read == 0) {
            printf("Server đã đóng kết nối.\n");
            break;
        }
        printf("Kết quả từ server: %s\n", buffer);
    }

    // Đóng kết nối khi thoát
    close(client_socket);
    return 0;
}
