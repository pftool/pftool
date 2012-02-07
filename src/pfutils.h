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

#ifndef      __PF_UTILS_H
#define      __PF_UTILS_H

//lets make sure things are 64 bit
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include "hashtbl.h"
#include <errno.h>
#include <utime.h>


//mpi
#include "mpi.h"

//gpfs
#ifdef TAPE
#ifdef HAVE_GPFS_H
#include <gpfs.h>
#ifdef HAVE_GPFS_FCNTL_H
#include "gpfs_fcntl.h"
#endif
#endif


#ifdef HAVE_DMAPI_H
#include <dmapi.h>
#endif

#endif

//fuse
#ifdef FUSE_CHUNKER
#include <sys/xattr.h>
#endif

#ifdef PLFS
#include "plfs.h"
#endif

#include "debug.h"


#define PATHSIZE_PLUS (FILENAME_MAX+30)
#define ERRORSIZE PATHSIZE_PLUS
#define MESSAGESIZE PATHSIZE_PLUS
#define MESSAGEBUFFER 400

#define DIRBUFFER 5
#define STATBUFFER 50
#define COPYBUFFER 15
#define CHUNKBUFFER COPYBUFFER
#define TAPEBUFFER 5

#define ANYFS     0
#define PANASASFS 1
#define GPFSFS    2
#define NULLFS    3
#ifdef FUSE_CHUNKER
#define FUSEFS    4

#define FUSE_SUPER_MAGIC 0x65735546
#define FUSE_FILE        0x65735546
#endif
#define GPFS_FILE        0x47504653
#define PANFS_FILE       0xAAd7AAEA
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
    LOGCMD,
    QUEUESIZECMD,
    STATCMD,
    COMPARECMD,
    COPYCMD,
    PROCESSCMD,
    INPUTCMD,
    DIRCMD,
#ifdef TAPE
    TAPECMD,
    TAPESTATCMD,
#endif
    WORKDONECMD,
    NONFATALINCCMD,
    CHUNKBUSYCMD,
    COPYSTATSCMD,
    EXAMINEDSTATSCMD
};


//for our MPI communications
#define MANAGER_PROC  0
#define OUTPUT_PROC   1
#define ACCUM_PROC    2
#define START_PROC    3

//errsend
#define FATAL 1
#define NONFATAL 0

enum wrk_type {
    COPYWORK = 0,
    LSWORK,
    COMPAREWORK
};

enum filetype {
    REGULARFILE = 0,
    FUSEFILE,
    LINKFILE,
    PREMIGRATEFILE,
    MIGRATEFILE,
    PLFSFILE,
    NONE
};

//Structs and typedefs
//options{
struct options {
    int verbose;
    int recurse;
    int logging;
    int different;
    int parallel_dest;
    int work_type;
    int meta_data_only;
    size_t blocksize;
    size_t chunk_at;
    size_t chunksize;
    char file_list[PATHSIZE_PLUS];
    int use_file_list;
    char jid[128];
#ifdef FUSE_CHUNKER
    char archive_path[PATHSIZE_PLUS];
    char fuse_path[PATHSIZE_PLUS];
    int use_fuse;
    int fuse_chunkdirs;
    size_t fuse_chunk_at;
    size_t fuse_chunksize;
#endif
#ifdef PLFS
    size_t plfs_chunksize;
#endif

    //fs info
    int sourcefs;
    int destfs;
};


// A queue to store all of our input nodes
struct path_link {
    char path[PATHSIZE_PLUS];
    struct stat st;
    off_t offset;
    size_t length;
    enum filetype ftype;
#ifdef FUSE_CHUNKER
    int fuse_dest;
#endif
#ifdef PLFS
    int plfs_dest;
#endif
};
typedef struct path_link path_item;

struct path_queue {
    //char path[PATHSIZE_PLUS];
    path_item data;
    struct path_queue *next;
};
typedef struct path_queue path_list;

struct work_buffer_list {
    char *buf;
    int size;
    struct work_buffer_list *next;
};
typedef struct work_buffer_list work_buf_list;

//Function Declarations
void usage();
char *printmode (mode_t aflag, char *buf);
char *get_base_path(const char *path, int wildcard);
void get_dest_path(const char *beginning_path, const char *dest_path, path_item *dest_node, int makedir, int num_paths, struct options o);
char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, struct options o);
int one_byte_read(const char *path);
int copy_file(const char *src_file, const char *dest_file, off_t offset, size_t length, size_t blocksize, struct stat src_st, int rank);
int compare_file(const char *src_file, const char *dest_file, off_t offset, size_t length, size_t blocksize, struct stat src_st, int meta_data_only);
int update_stats(const char *src_file, const char *dest_file, struct stat src_st);

//dmapi/gpfs specfic
#ifdef TAPE
int read_inodes(const char *fnameP, gpfs_ino_t startinode, gpfs_ino_t endinode, int *dmarray);
int dmapi_lookup (char *mypath, int *dmarray, char *dmouthexbuf);
#endif


//local functions
int request_response(int type_cmd);
int request_input_queuesize();
void send_command(int target_rank, int type_cmd);
void send_path_list(int target_rank, int command, int num_send, path_list **list_head, path_list **list_tail, int *list_count);
void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count);
void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, int *workbufsize);

//worker utility functions
void errsend(int fatal, char *error_text);
#ifdef FUSE_CHUNKER
int is_fuse_chunk(const char *path);
void set_fuse_chunk_data(path_item *work_node);
int get_fuse_chunk_attr(const char *path, int offset, int length, struct utimbuf *ut, uid_t *userid, gid_t *groupid);
int set_fuse_chunk_attr(const char *path, int offset, int length, struct utimbuf ut, uid_t userid, gid_t groupid);
#endif
//void get_stat_fs_info(path_item *work_node, int *sourcefs, char *sourcefsc);
void get_stat_fs_info(const char *path, int *fs);
int get_free_rank(int *proc_status, int start_range, int end_range);
int processing_complete(int *proc_status, int nproc);

//function definitions for manager
void send_manager_regs_buffer(path_item *buffer, int *buffer_count);
void send_manager_dirs_buffer(path_item *buffer, int *buffer_count);
#ifdef TAPE
void send_manager_tape_buffer(path_item *buffer, int *buffer_count);
#endif
void send_manager_new_buffer(path_item *buffer, int *buffer_count);
void send_manager_nonfatal_inc();
void send_manager_chunk_busy();
void send_manager_copy_stats(int num_copied_files, double num_copied_bytes);
void send_manager_examined_stats(int num_examined_files, double num_examined_bytes, int num_examined_dirs);
void send_manager_tape_stats(int num_examined_tapes, double num_examined_tape_bytes);
void send_manager_work_done();

//function definitions for workers
void update_chunk(path_item *buffer, int *buffer_count);
void write_output(char *message, int log);
void write_buffer_output(char *buffer, int buffer_size, int buffer_count);
void send_worker_queue_count(int target_rank, int queue_count);
void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
#ifdef TAPE
void send_worker_tape_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
#endif
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

//fake mpi
int MPY_Pack(void *inbuf, int incount, MPI_Datatype datatype, void *outbuf, int outcount, int *position, MPI_Comm comm);
int MPY_Unpack(void *inbuf, int insize, int *position, void *outbuf, int outcount, MPI_Datatype datatype, MPI_Comm comm);
int MPY_Abort(MPI_Comm comm, int errorcode);
#endif



