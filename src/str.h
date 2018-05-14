//
// Defines for string functions
//

#ifndef      __STR_H
#define      __STR_H

#include <ctype.h>
#include "sig.h"
#ifndef TRUE
#  define TRUE 1
#endif
#ifndef FALSE
#  define FALSE 0
#endif
// Function Declarations
char* str2sig(const char *str);
size_t str2Size(char *ss);
int strIsBlank(const char *s);
int strHasPrefix(const char *prefixes[],const char *s);
char *strStripPrefix(const char *prefixes[],const char *s);

#endif  // __STR_H
