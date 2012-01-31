#ifndef      __PFTOOL_DEBUG_H
#define      __PFTOOL_DEBUG_H


//define debugs
//#define POLL_DEBUG
//#define MPI_DEBUG
//#define PROC_DEBUG
//#define DMAPI_DEBUG

//define debug print statements

#ifdef POLL_DEBUG
#define PRINT_POLL_DEBUG(format, args...) printf("POLL_DEBUG: "format, ##args);
#else
#define PRINT_POLL_DEBUG(format, args...)
#endif

#ifdef PROC_DEBUG
#define PRINT_PROC_DEBUG(format, args...) printf("PROC_DEBUG: "format, ##args);
#else
#define PRINT_PROC_DEBUG(format, args...)
#endif

#ifdef MPI_DEBUG
#define PRINT_MPI_DEBUG(format, args...) printf("MPI_DEBUG: "format, ##args);
#else
#define PRINT_MPI_DEBUG(format, args...)
#endif

#ifdef DMAPI_DEBUG
#define PRINT_DMAPI_DEBUG(format, args...) printf("DMAPI_DEBUG: "format, ##args);
#else
#define PRINT_DMAPI_DEBUG(format, args...)
#endif


#endif
