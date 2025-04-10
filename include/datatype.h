#ifndef DATATYPE_H
#define DATATYPE_H

typedef enum {
	    TYPE_INT,
        TYPE_TEXT,
	    TYPE_DATE
} DataType;

int validate_data(DataType type, const char *value);

#endif

