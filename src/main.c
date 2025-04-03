#include <stdio.h>
#include <string.h>
#include "../include/parser.h"
#include "../include/database.h"
#define MAX_INPUT 256

void print_prompt(){
if (strlen(current_db) > 0) {
  printf("Yocto-SQL/%s> ", current_db);
} 
else {
  printf("Yocto-SQL> ");
}
}
int main() {
char input[MAX_INPUT];
printf("Yocto-SQL> ");
while (fgets(input, MAX_INPUT, stdin)) {  
  input[strcspn(input, "\n")] = 0;
  if (strcmp(input, "exit") == 0) {
    printf("Bye!\n");
    break;
  }
  execute_command(input);
  print_prompt();
}
return 0;
}

