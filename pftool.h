#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <dirent.h>
#include <getopt.h>
#include "pfutils.h"

/* Function Prototypes */
//manager rank operations
void manager(int rank, struct options o, int nproc, path_node *input_queue_head, path_node *input_queue_tail, int input_queue_count, const char *dest_path);
void manager_workdone(int rank, int sending_rank, int *proc_status);
int manager_add_paths(int rank, int sending_rank, path_node **queue_head, path_node **queue_tail, int *queue_count);
void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, int *num_copied_bytes);

//worker rank operations
void worker(int rank, struct options o);
void worker_output(int rank, int sending_rank);
void worker_buffer_output(int rank, int sending_rank);
void worker_stat(int rank, int sending_rank);
void worker_readdir(int rank, int sending_rank);
void worker_copylist(int rank, int sending_rank, const char *base_path, const char *dest_path, struct options o);

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

#define WAIT_TIME    1
#define SANITY_TIMER  300 

#endif
