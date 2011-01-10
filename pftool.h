#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <dirent.h>
#include <getopt.h>
#include "pfutils.h"

/* Function Prototypes */
void manager(char *, int, int, int);
void worker(int);

#define ANYFS     0
#define PANASASFS 1
#define GPFSFS    2
#define NULLFS    3
#define FUSEFS    4 


#define FUSE_SUPER_MAGIC 0x65735546 
#define GPFS_FILE        0x47504653 
#define FUSE_FILE        0x65735546 
#define PANFS_FILE       0xaad7aaea
#define EXT2_FILE        0xEF53
#define EXT3_FILE        0xEF53 

#define NULL_DEVICE      "/dev/null"  


/*four our MPI communications*/
#define MANAGER_PROC  0
#define OUTPUT_PROC   1
#define NAMEREAD_PROC 2
#define NAMESTAT_PROC 3
#define MANAGER_TAG   0
#define OUTPUT_TAG    1
#define NAMEREAD_TAG  2
#define NAMESTAT_TAG  3 


#define WAIT_TIME    1
// 0507-2009  TEST 
//#define QSIZE 1000
#define QSIZE           75000

#define QSIZE_INCREASED 1000

#define QSIZE_SOFTQUOTA 25000

// The original PACKSIZE was set to 100 
#define PACKSIZE  200 

#define WORKSIZE (QSIZE * PACKSIZE * 10)

#define SANITY_TIMER  300 


#define FATAL       1
#define NONFATAL    0

#define INTERNALERR 9999
#define INFOONLY    2

// pflog ACTION CODE 
#define ACT_ERROR    "ERROR"
#define ACT_INFO     "INFO"
#define ACT_CRITICAL "CRITICAL"
#define ACT_WARNING  "WARNING"
#define ACT_EMERG    "EMERGENCY"
#define ACT_ALERT    "ALERT"
#define ACT_NOTICE   "NOTICE"
#define ACT_DEBUG    "DEBUG"


#define SMGC_MASTER_RANK 0


/*these are the different commands that can be sent around
  basically its just a tag of what we are talking about
  in our MPI communications*/
enum cmd_opcode {
  DIRCMD = 1,
  NAMECMD,
  REQCMD,
  EXITCMD,
  OUTCMD,
  STATCMD,
  ABORTCMD,
  COPYCMD,
  OUTFIN,
  OUTFLUSH,
  WAITCMD,
  COPYCMD2,
  COMPCMD,
  CHUNKFILECMD,
  WATCHDOGCMD

};


#endif
