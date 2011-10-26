#ifndef      __MPI_UTILS_H
#define      __MPI_UTILS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <gpfs.h>
#include "mpi.h"
#include "hashtbl.h"
#include <errno.h>

//panasas
#include "pan_fs_client_sdk.h"

//gpfs
#include <gpfs.h>                                                                                                                                                                                                                                                                    
#include "gpfs_fcntl.h"
#include <dmapi.h>



#define PATHSIZE_PLUS (FILENAME_MAX+30)
#define ERRORSIZE PATHSIZE_PLUS
#define MESSAGESIZE PATHSIZE_PLUS
#define MESSAGEBUFFER 2000

#define DIRBUFFER 50
#define STATBUFFER 2000
#define CHUNKBUFFER 300
#define COPYBUFFER 30
#define TAPEBUFFER 30

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
#define EXT4_FILE        0xEF53
#define PNFS_FILE        0X00000000
#define ANY_FILE         0X00000000 

#define O_CONCURRENT_WRITE          020000000000

#define DevMinor(x) ((x)&0xFFFF)
#define DevMajor(x) ((unsigned)(x)>>16)
typedef unsigned long long int uint_64;
//mpi_commands
enum cmd_opcode {                                                                                                                                                                                                                                                                                                  
  EXITCMD = 1,
  UPDCHUNKCMD,
  OUTCMD,
  BUFFEROUTCMD,
  QUEUESIZECMD,
  NAMECMD,
  STATCMD,
  COMPARECMD,
  COPYCMD,
  REGULARCMD,
  INPUTCMD,
  DIRCMD,
  TAPECMD,
  WORKDONECMD,
  NONFATALINCCMD,
  CHUNKBUSYCMD,
  COPYSTATSCMD,
  EXAMINEDSTATSCMD
};


//for our MPI communications 
#define MANAGER_PROC  0
#define OUTPUT_PROC   1
#define ACCUM_PROC   2

//errsend
#define FATAL 1
#define NONFATAL 0

enum wrk_type{
  COPYWORK = 0,
  LSWORK,
  COMPAREWORK
};

enum filetype {
  REGULARFILE = 0,
  FUSEFILE,
  LINKFILE,
  PREMIGRATEFILE,
  MIGRATEFILE
};

//Structs and typedefs
//options{
struct options{
  int verbose;
  int recurse;
  int different;
  int work_type;
  int meta_data_only;
  char file_list[PATHSIZE_PLUS];
  int use_file_list;
  char jid[128];
};


// A queue to store all of our input nodes
struct path_link{
  char path[PATHSIZE_PLUS];
  struct stat st;
  off_t offset;
  off_t length;
  enum filetype ftype;
};
typedef struct path_link path_item;

struct path_queue{
  //char path[PATHSIZE_PLUS];
  path_item data;
  struct path_queue *next;
};
typedef struct path_queue path_list;

struct work_buffer_list{
  char *buf;
  int size;
  struct work_buffer_list *next;
};
typedef struct work_buffer_list work_buf_list;

//Function Declarations
void usage();
char *printmode (mode_t aflag, char *buf);
int read_inodes(const char *fnameP, gpfs_ino_t startinode, gpfs_ino_t endinode, int *dmarray);
int dmapi_lookup (char *mypath, int *dmarray, char *dmouthexbuf);
char *get_base_path(const char *path, int wildcard);
void get_dest_path(const char *beginning_path, const char *dest_path, path_item *dest_node, int makedir, int num_paths, struct options o);
char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, struct options o);
int copy_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st);
int compare_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st, int meta_data_only);
int update_stats(const char *src_file, const char *dest_file, struct stat src_st);


//local functions
int request_response(int type_cmd);
int request_input_queuesize();
void send_command(int target_rank, int type_cmd);
void send_path_list(int target_rank, int command, int num_send, path_list **list_head, path_list **list_tail, int *list_count);
void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count);
void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, int *workbufsize);

//worker utility functions
void errsend(int fatal, char *error_text);
int is_fuse_chunk(const char *path);
void set_fuse_chunk_data(path_item *work_node);
void get_stat_fs_info(path_item *work_node, int *sourcefs, char *sourcefsc);
int get_free_rank(int *proc_status, int start_range, int end_range);
int processing_complete(int *proc_status, int nproc);

//function definitions for manager
void send_manager_regs_buffer(path_item *buffer, int *buffer_count);
void send_manager_dirs_buffer(path_item *buffer, int *buffer_count);
void send_manager_tape_buffer(path_item *buffer, int *buffer_count);
void send_manager_new_buffer(path_item *buffer, int *buffer_count);
void send_manager_nonfatal_inc();
void send_manager_chunk_busy();
void send_manager_copy_stats(int num_copied_files, double num_copied_bytes);
void send_manager_examined_stats(int num_examined);
void send_manager_work_done();

//function definitions for workers
void update_chunk(path_item *buffer, int *buffer_count);
void write_output(char *message);
void write_buffer_output(char *buffer, int buffer_size, int buffer_count);
void send_worker_queue_count(int target_rank, int queue_count);
void send_worker_stat_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
void send_worker_compare_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
void send_worker_exit(int target_rank);

//function definitions for queues
void enqueue_path(path_list **head, path_list **tail, char *path, int *count);
void print_queue_path(path_list *head);
void delete_queue_path(path_list **head, int *count);
void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count);
void dequeue_node(path_list **head, path_list **tail, int *count);
void pack_list(path_list *head, int count, work_buf_list **workbuflist, int *workbufsize);


//function definitions for workbuf_list;
void enqueue_buf_list(work_buf_list **workbuflist, int *workbufsize, char *buffer, int buffer_size);
void dequeue_buf_list(work_buf_list **workbuflist, int *workbufsize);
void delete_buf_list(work_buf_list **workbuflist, int *workbufsize);

#endif



