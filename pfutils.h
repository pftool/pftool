#ifndef      __MPI_UTILS_H
#define      __MPI_UTILS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <gpfs.h>
#include "mpi.h"

#define PATHSIZE_PLUS FILENAME_MAX+30

#define MAX_STAT  600 
#define MAXFILES 50000


#define FUSE_SUPER_MAGIC 0x65735546
#define GPFS_FILE        0x47504653
#define FUSE_FILE        0x65735546
#define PANFS_FILE       0xaad7aaea
#define EXT2_FILE        0xEF53
#define EXT3_FILE        0xEF53
#define EXT4_FILE        0xEF53
#define PNFS_FILE        0X00000000
#define ANY_FILE         0X00000000 

typedef unsigned long long int uint_64;

//Function Declarations
void usage();

                        

#endif



