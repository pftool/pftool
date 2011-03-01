#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <getopt.h>
#include "pfutils.h"

/* Function Prototypes */
//manager rank operations
void manager(int rank, struct options o, int nproc, path_list *input_queue_head, path_list *input_queue_tail, int input_queue_count, const char *dest_path);
void manager_workdone(int rank, int sending_rank, int *proc_status);
int manager_add_paths(int rank, int sending_rank, path_list **queue_head, path_list **queue_tail, int *queue_count);
int manager_add_buffs(int rank, int sending_rank, work_buf_list **workbuflist, int *workbufsize);
void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, int *num_copied_bytes);

//worker rank operations
void worker(int rank, struct options o);
void worker_check_chunk(int rank, int sending_rank, HASHTBL **chunk_hash);
void worker_output(int rank, int sending_rank);
void worker_buffer_output(int rank, int sending_rank);
void worker_stat(int rank, int sending_rank, const char *base_path, path_item dest_node, int recurse, int work_type);
void worker_readdir(int rank, int sending_rank, const char *base_path, path_item dest_node, int recurse, int mkdir);
void worker_readdir_stat(int rank, int sending_rank, const char *base_path, path_item dest_node, int recurse, int makedir);
void worker_copylist(int rank, int sending_rank, const char *base_path, path_item dest_node, int recurse);





#define FUSE_SUPER_MAGIC 0x65735546 
#define GPFS_FILE        0x47504653 
#define FUSE_FILE        0x65735546 
#define PANFS_FILE       0xaad7aaea
#define EXT2_FILE        0xEF53
#define EXT3_FILE        0xEF53 

#define NULL_DEVICE      "/dev/null"  

#define WAIT_TIME    1
#define SANITY_TIMER  300 

enum wrk_type{
  COPYWORK = 0,
  LSWORK,
  COMPAREWORK
};

#endif
