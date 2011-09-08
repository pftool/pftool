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
void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, double *num_copied_bytes);
void manager_add_examined_stats(int rank, int sending_rank, int *num_examined);

//worker rank operations
void worker(int rank, struct options o);
void worker_check_chunk(int rank, int sending_rank, HASHTBL **chunk_hash);
void worker_output(int rank, int sending_rank);
void worker_buffer_output(int rank, int sending_rank);
void worker_update_chunk(int rank, int sending_rank, HASHTBL **chunk_hash, int *hash_count, const char *base_path, path_item dest_node, struct options o);
void worker_stat(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o);
void worker_readdir(int rank, int sending_rank, const char *base_path, path_item dest_node, int mkdir, struct options o);
void worker_readdir_stat(int rank, int sending_rank, const char *base_path, path_item dest_node, int recurse, int makedir);
void worker_copylist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o);
void worker_comparelist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o);


#define NULL_DEVICE      "/dev/null"  

#define WAIT_TIME    1
#define SANITY_TIMER  300 


#endif
