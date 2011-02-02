#ifndef      __MPI_UTILS_H
#define      __MPI_UTILS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <libgen.h>
#include <unistd.h>
#include <gpfs.h>
#include "mpi.h"

//panasas
#include "pan_fs_client_sdk.h"

//gpfs
#include <gpfs.h>                                                                                                                                                                                                                                                                    
#include "gpfs_fcntl.h"
#include <dmapi.h>



#define PATHSIZE_PLUS (FILENAME_MAX+30)
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
  BUFFEROUTCMD,
  NAMECMD,
  STATCMD,
  COMPARECMD,
  COPYCMD,
  REGULARCMD,
  INPUTCMD,
  DIRCMD,
  TAPECMD,
  WORKDONECMD,
  NONFATALINCCMD
};

enum wrk_type{
  COPYWORK = 1,
  LSWORK = 2,
  COMPAREWORK = 3
};

//for our MPI communications 
#define MANAGER_PROC  0
#define OUTPUT_PROC   1

//errsend
#define FATAL 1
#define NONFATAL 0


//Structs and typedefs
//options{
struct options{
  int recurse;
  int work_type;
  char jid[128];
};


// A queue to store all of our input nodes
/*struct path_link{
  char path[PATHSIZE_PLUS];
  struct stat st;
};*/

struct path_queue{
  char path[PATHSIZE_PLUS];
  struct path_queue *next;
};

typedef struct path_queue path_node;

//Function Declarations
void usage();
char *printmode (mode_t aflag, char *buf);
char *get_base_path(const char *path, int wildcard);
char *get_dest_path(const char *beginning_path, const char *dest_path, int recurse);
char *get_output_path(const char *base_path, const char *src_path, const char *dest_path, int recurse);

//local functions
void send_command(int target_rank, int type_cmd);
void send_path_list(int target_rank, int command, int num_send, path_node **list_head, path_node **list_tail, int *list_count);

//worker utility functions
void errsend(int fatal, char *error_text);
int get_free_rank(int *proc_status, int start_range, int end_range);
int processing_complete(int *proc_status, int nproc);

//function definitions for manager
void send_manager_regs(int num_send, path_node **reg_list_head, path_node **reg_list_tail, int *reg_list_count);
void send_manager_dirs(int num_send, path_node **dir_list_head, path_node **dir_list_tail, int *dir_list_count);
void send_manager_tape(int num_send, path_node **tape_list_head, path_node **tape_list_tail, int *tape_list_count);
void send_manager_new_input(int num_send, path_node **new_input_list_head, path_node **new_input_list_tail, int *new_input_list_count);
void send_manager_nonfatal_inc();
void send_manager_work_done();

//function definitions for workers
void write_output(char *message);
void write_buffer_output(char *buffer, int buffer_size, int buffer_count);
void send_worker_stat_path(int target_rank, int num_send, path_node **input_queue_head, path_node **input_queue_tail, int *input_queue_count);
void send_worker_readdir(int target_rank, int num_send, path_node **dir_work_queue_head, path_node **dir_work_queue_tail, int *dir_work_queue_count);
void send_worker_copy_path(int target_rank, int num_send, path_node **work_queue_head, path_node **work_queue_tail, int *work_queue_count);
void send_worker_exit(int target_rank);

//function definitions for queues
void enqueue_path(path_node **head, path_node **tail, char *path, int *count);
void dequeue_path(path_node **head, path_node **tail, int *count);
void print_queue_path(path_node *head);
void delete_queue_path(path_node **head, int *count);
#endif



