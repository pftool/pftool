//
// Defines for string functions
//

#ifndef      __STR_H
#define      __STR_H

#include <ctype.h>

#ifndef TRUE
#  define TRUE 1
#endif
#ifndef FALSE
#  define FALSE 0
#endif

// Function Declarations
size_t str2Size(char *ss);
int strIsBlank(const char *s);
char *str2md5(const char *str);

#endif  // __STR_H
