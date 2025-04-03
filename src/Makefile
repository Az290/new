CC = gcc
CFLAGS = -Wall -Wextra
LIBS = -ljson-c -lpthread

SERVER_SOURCES = server.c parser.c database.c table.c datatype.c
SERVER_OBJECTS = $(SERVER_SOURCES:.c=.o)

CLIENT_SOURCES = client.c
CLIENT_OBJECTS = $(CLIENT_SOURCES:.c=.o)

all: sql_server sql_client

sql_server: $(SERVER_OBJECTS)
	$(CC) $(CFLAGS) -o sql_server $(SERVER_OBJECTS) $(LIBS)

sql_client: $(CLIENT_OBJECTS)
	$(CC) $(CFLAGS) -o sql_client $(CLIENT_OBJECTS)

.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(SERVER_OBJECTS) $(CLIENT_OBJECTS) sql_server sql_client

