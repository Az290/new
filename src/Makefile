CC = gcc
CFLAGS = -Wall -Wextra `pkg-config --cflags gtk+-3.0`
LIBS = -ljson-c -lpthread `pkg-config --libs gtk+-3.0`

SERVER_SOURCES = server.c parser.c database.c table.c datatype.c
SERVER_OBJECTS = $(SERVER_SOURCES:.c=.o)

CLIENT_SOURCES = client.c
CLIENT_OBJECTS = $(CLIENT_SOURCES:.c=.o)

GTK_CLIENT_SOURCES = gtk_client.c
GTK_CLIENT_OBJECTS = $(GTK_CLIENT_SOURCES:.c=.o)

all: sql_server sql_client sql_gtk_client

sql_server: $(SERVER_OBJECTS)
	$(CC) $(CFLAGS) -o sql_server $(SERVER_OBJECTS) $(LIBS)

sql_client: $(CLIENT_OBJECTS)
	$(CC) $(CFLAGS) -o sql_client $(CLIENT_OBJECTS) $(LIBS)

sql_gtk_client: $(GTK_CLIENT_OBJECTS)
	$(CC) $(CFLAGS) -o sql_gtk_client $(GTK_CLIENT_OBJECTS) $(LIBS)

.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(SERVER_OBJECTS) $(CLIENT_OBJECTS) $(GTK_CLIENT_OBJECTS) sql_server sql_client sql_gtk_client

.PHONY: all clean


