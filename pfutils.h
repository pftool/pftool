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

//panasas
#include "pan_fs_client_sdk.h"


#define PATHSIZE_PLUS FILENAME_MAX+30
#define ERRORSIZE PATHSIZE_PLUS
#define MESSAGESIZE PATHSIZE_PLUS

#define MAX_STAT  600 
#define MAXFILES 50000

#define QSIZE           75000                                                                                                                                                                                                                                                                                      
#define QSIZE_INCREASED 1000
#define QSIZE_SOFTQUOTA 25000
#define PACKSIZE  200 
#define WORKSIZE (QSIZE * PACKSIZE * 10)


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

//mpi_commands
enum cmd_opcode {                                                                                                                                                                                                                                                                                                  
  EXITCMD = 1,
  OUTCMD,
  NAMECMD,
  STATCMD,
  COMPARECMD,
  COPYCMD,
};

//for our MPI communications 
#define MANAGER_PROC  0
#define OUTPUT_PROC   1

//errsend
#define FATAL 1
#define NONFATAL 0


//Function Declarations
void usage();
char *printmode (mode_t aflag, char *buf);

//operations on ranks
void errsend(int rank, int fatal, char *error_text);
void write_output(int rank, char *message);
void stat_path(int rank, int target_rank, char *path);
void exit_rank(int target_rank);
void send_command(int target_rank, int type_cmd);

//Queues
// A queue to store all of our input nodes
struct path_queue{
  char path[PATHSIZE_PLUS];
  struct path_queue *next;
};

typedef struct path_queue path_node;

//function definitions for queues
void enqueue_path(path_node **head, char *path, int *count);
void dequeue_path(path_node **head, int *count);
void print_queue_path(path_node *head);
                        



#endif



