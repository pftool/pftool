#ifndef      __PFTOOL_DEBUG_H
#define      __PFTOOL_DEBUG_H


//define debugs
//#define DEBUG_GEN

//define debug print statements
#ifdef DEBUG_GEN
#define PRINT_DEBUG_GEN(format, args...) printf("DEBUG_GEN: "format, ##args);
#else
#define PRINT_DEBUG_GEN(format, args...)
#endif

#endif
