/*
*This material was prepared by the Los Alamos National Security, LLC (LANS) under
*Contract DE-AC52-06NA25396 with the U.S. Department of Energy (DOE). All rights
*in the material are reserved by DOE on behalf of the Government and LANS
*pursuant to the contract. You are authorized to use the material for Government
*purposes but it is not to be released or distributed to the public. NEITHER THE
*UNITED STATES NOR THE UNITED STATES DEPARTMENT OF ENERGY, NOR THE LOS ALAMOS
*NATIONAL SECURITY, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY WARRANTY, EXPRESS
*OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR RESPONSIBILITY FOR THE ACCURACY,
*COMPLETENESS, OR USEFULNESS OF ANY INFORMATION, APPARATUS, PRODUCT, OR PROCESS
*DISCLOSED, OR REPRESENTS THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
*/

#ifndef      __PSTAT_H
#define      __PSTAT_H

#include <signal.h>
#include <getopt.h>
#include "hashtbl.h"
#include "pfutils.h"

/* Function Prototypes */
//manager rank operations
int manager(int rank, struct options& o, int nproc, path_list *input_queue_head, path_list *input_queue_tail, int input_queue_count, const char *dest_path);
void manager_workdone(int rank, int sending_rank, int *proc_status);
int  manager_add_paths(int rank, int sending_rank, path_list **queue_head, path_list **queue_tail, int *queue_count);
void manager_add_buffs(int rank, int sending_rank, work_buf_list **workbuflist, int *workbufsize);
void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, size_t *num_copied_bytes);
void manager_add_examined_stats(int rank, int sending_rank, int *num_examined_files, size_t *num_examined_bytes, int *num_examined_dirs);
#ifdef TAPE
void manager_add_tape_stats(int rank, int sending_rank, int *num_examined_tapes, size_t *num_examined_tape_bytes);
#endif

//worker rank operations
void worker(int rank, struct options& o);
void worker_check_chunk(int rank, int sending_rank, HASHTBL **chunk_hash);
void worker_flush_output(char *output_buffer, int *output_count);
void worker_output(int rank, int sending_rank, int log, char *output_buffer, int *output_count, struct options& o);
void worker_buffer_output(int rank, int sending_rank, char *output_buffer, int *output_count, struct options& o);
void worker_update_chunk(int rank, int sending_rank, HASHTBL **chunk_hash, int *hash_count, const char *base_path, path_item* dest_node, struct options& o);
void worker_readdir(int rank, int sending_rank, const char *base_path, path_item* dest_node, int start, int makedir, struct options& o);
int stat_item(path_item *work_node, struct options& o);
//void process_stat_buffer(path_item *path_buffer, int *stat_count, const char *base_path, path_item* dest_node, struct options& o);
void process_stat_buffer(path_item *path_buffer, int *stat_count, const char *base_path, path_item *dest_node, struct options& o, int rank);
void worker_taperecall(int rank, int sending_rank, path_item* dest_node, struct options& o);
//void worker_taperecall(int rank, int sending_rank, path_item dest_node, struct options o);
void worker_copylist(int rank, int sending_rank, const char *base_path, path_item* dest_node, struct options& o);
//void worker_copylist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o);
void worker_comparelist(int rank, int sending_rank, const char *base_path, path_item* dest_node, struct options& o);
//void worker_comparelist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o);


#define NULL_DEVICE      "/dev/null"

#define WAIT_TIME    1
#define SANITY_TIMER  300


#endif
