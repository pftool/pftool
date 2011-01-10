#ifndef      __PFTOOL_DEBUG_H
#define      __PFTOOL_DEBUG_H


//define debugs
//#define DEBUG_LEVEL001
//#define VERBOSE_DEBUG
//#define FUSE_DEBUG
//#define FUSE_CHUNK_DEBUG
//#define POLL_DEBUG
//#define TAPE_RECALLED_DEBUG
//#define TAPE_TABLE_DEBUG
//#define QUEUE_DEBUG
//#define COPY_DEBUG
//#define DMAPI_DEBUG
//#define WATCHDOG_DEBUG
//#define PATH_DEBUG

//define debug print statements
//maybe later #define LOBA_GS_DEBUG_MESSAGE(pfArgs...) printf(pfArgs);
#ifdef DEBUG_LEVEL001
#define PRINT_DEBUG_LEVEL001(format, args...) printf("DEBUG_LEVEL001: "format, ##args);
#else
#define PRINT_DEBUG_LEVEL001(format, args...)
#endif

#ifdef VERBOSE_DEBUG 
#define PRINT_VERBOSE_DEBUG(format, args...) printf("VERBOSE_DEBUG: "format, ##args);
#else
#define PRINT_VERBOSE_DEBUG(format, args...)
#endif


#ifdef FUSE_DEBUG 
#define PRINT_FUSE_DEBUG(format, args...) printf("FUSE_DEBUG: "format, ##args);
#else
#define PRINT_FUSE_DEBUG(format, args...)
#endif

#ifdef FUSE_CHUNK_DEBUG 
#define PRINT_FUSE_CHUNK_DEBUG(format, args...) printf("FUSE_CHUNK_DEBUG: "format, ##args);
#else
#define PRINT_FUSE_CHUNK_DEBUG(format, args...)
#endif

#ifdef POLL_DEBUG
#define PRINT_POLL_DEBUG(format, args...) printf("POLL_DEBUG: "format, ##args);
#else
#define PRINT_POLL_DEBUG(format, args...)
#endif

#ifdef TAPE_RECALLED_DEBUG
#define PRINT_TAPE_RECALLED_DEBUG(format, args...) printf("TAPE_RECALLED_DEBUG: "format, ##args);
#else
#define PRINT_TAPE_RECALLED_DEBUG(format, args...)
#endif

#ifdef TAPE_TABLE_DEBUG
#define PRINT_TAPE_TABLE_DEBUG(format, args...) printf("TAPE_TABLE_DEBUG_DEBUG: "format, ##args);
#else
#define PRINT_TAPE_TABLE_DEBUG(format, args...)
#endif

#ifdef QUEUE_DEBUG
#define PRINT_QUEUE_DEBUG(format, args...) printf("QUEUE_DEBUG: "format, ##args);
#else
#define PRINT_QUEUE_DEBUG(format, args...)
#endif

#ifdef COPY_DEBUG 
#define PRINT_COPY_DEBUG(format, args...) printf("COPY_DEBUG: "format, ##args);
#else
#define PRINT_COPY_DEBUG(format, args...)
#endif

#ifdef DMAPI_DEBUG 
#define PRINT_DMAPI_DEBUG(format, args...) printf("DMAPI_DEBUG: "format, ##args);
#else
#define PRINT_DMAPI_DEBUG(format, args...)
#endif

#ifdef WATCHDOG_DEBUG 
#define PRINT_WATCHDOG_DEBUG(format, args...) printf("WATCHDOG_DEBUG: "format, ##args);
#else
#define PRINT_WATCHDOG_DEBUG(format, args...)
#endif

#ifdef PATH_DEBUG 
#define PRINT_PATH_DEBUG(format, args...) printf("PATH_DEBUG: "format, ##args);
#else
#define PRINT_PATH_DEBUG(format, args...)
#endif

#endif
