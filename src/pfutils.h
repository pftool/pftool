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
#include <stdarg.h>             // va_list, vsnprintf()
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#ifdef HAVE_SYS_VFS_H
#  include <sys/vfs.h>
#endif

#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <errno.h>
#include <utime.h>
#include "str.h"

//mpi
#include "mpi.h"

//synthetic data generation
#ifdef GEN_SYNDATA
#  include "syndata.h"
#else
   typedef void*  SyndataBufPtr;   /* eliminates some need for #ifdefs */
#  define syndataExists(PTR)  (PTR)  /* eliminates some need for #ifdefs */
#endif


#include "debug.h"

#define DATE_STRING_MAX 64  /* includes room for leading '+', when used in filenames */
#define PATHSIZE_PLUS   (FILENAME_MAX + DATE_STRING_MAX + 30)
#define ERRORSIZE       PATHSIZE_PLUS
#define MESSAGESIZE     PATHSIZE_PLUS
#define MESSAGEBUFFER   400

#define MAX_TEMP_UNLINK_ITER  3
#define TEMP_UNLINK_WAIT_TIME 1

// if you are trying to increase max pack size, STATBUFFER must be >= to
// COPYBUFFER because it only collects one stat buffer worth of things before
// shipping off.
#define DIRBUFFER      5
#define STATBUFFER     4096
#define COPYBUFFER     4096
#define CHUNKBUFFER    COPYBUFFER

// The amount of data to accumulate before shipping off to a copy process
#define SHIPOFF        536870912

// The number of stat processes to default to, -1 is infinate
#define MAXREADDIRRANKS (-1)

// <sys/vfs.h> provides statfs(), which operates on a struct statfs,
// similarly to what stat() does with struct fs.  statfs.f_type identifies
// various known file-system types.  Ours also includes these:
#define FUSE_SUPER_MAGIC  0x65735546
#define FUSE_FILE         0x65735546
#define GPFS_FILE         0x47504653
#define PANFS_FILE        0xAAd7AAEA
#define EXT2_FILE         0xEF53
#define EXT3_FILE         0xEF53
#define EXT4_FILE         0xEF53
#define PNFS_FILE         0X00000000
#define ANY_FILE          0X00000000

// get_stat_fs_info() translates the statfs.f_types (above) into these, for
// convenience.  These are then stored in options.sourcefs/destfs, where
// they are completely ignored, except for one test in #ifdef TAPE.
// Actually, they are also examined one other place: in the initializations
// of worker(), if the command-line didn't specify '-P' (parallel
// destination), and options.destfs is not ANYFS, then the destination is
// considered to be parallel anyhow.  Because this list may grow, we're
// changing that test to look at whether options.destfs > PARALLEL_DESTFS.
// So, if you are extending this list, and your FS is not a parallel
// destination (i.e. capable of N:1), then put it before PARALLEL_DESTFS.
// Similarly, with REST_FS.  (If you ever find a REST-ful fs that is not
// parallel, we'll have to replace PARALLEL_FS and REST_FS with functions
// to check any given FS value for membership.)
//
// We're converting to an enum, so it will be obvious where these things
// are (not) used.
//
// TBD: Just add a virtual Path method, to return the SrcDstFSType, and get
// rid of get_stat_fs_info().
enum SrcDstFSType {
   ANYFS      = 0,

   PANASASFS  = 1,   // everything after here supports N:1 (see PARALLEL_DESTFS)
   GPFSFS     = 2,
   NULLFS     = 3,
   SYNDATAFS  = 4,
   FUSEFS     = 5,

   S3FS       = 6,   // everything after here is REST-ful (see REST_FS)
   PLFSFS     = 7,
   MARFSFS    = 8
};
#define PARALLEL_DESTFS  PANASASFS /* beginning of SrcDstFSTypes supporting N:1 writes */
#define REST_FS          S3FS      /* beginning of SrcDstFSTypes that are RESTful */

#define O_CONCURRENT_WRITE          020000000000



#define DevMinor(x) ((x)&0xFFFF)
#define DevMajor(x) ((unsigned)(x)>>16)
typedef unsigned long long int uint_64;

//mpi_commands
enum cmd_opcode {
    EXITCMD,
    UPDCHUNKCMD,
    BUFFEROUTCMD,
    OUTCMD,
    LOGCMD,
    LOGONLYCMD,
    QUEUESIZECMD,
    STATCMD,
    COMPARECMD,
    COPYCMD,
    PROCESSCMD,
    INPUTCMD,
    DIRCMD,
    WORKDONECMD,
    NONFATALINCCMD,
    CHUNKBUSYCMD,
    COPYSTATSCMD,
    EXAMINEDSTATSCMD,
    STATS
};
typedef enum cmd_opcode OpCode;

//for our MPI communications
#define MANAGER_PROC  0
#define OUTPUT_PROC   1
#define ACCUM_PROC    2
#define START_PROC    3

//errsend
#define FATAL 1
#define NONFATAL 0

enum WorkType {
    COPYWORK = 0,
    LSWORK,
    COMPAREWORK
};

enum FileType {
   NONE = 0,                    // deleted?  irrelevant?  see process_stat_buffer()
   TBD,                         // not yet initialized by factory

   REGULARFILE,
   FUSEFILE,
   PLFSFILE,
   S3FILE,
   MARFSFILE,

   SYNDATA,                     // synthetic data (no file - in memory read)
   NULLFILE,
   NULLDIR                      // no-cost writes
};

// this is currently only used in three places:
// (a) command-line option '-t' initializes options.dest_fstype
// (b) worker_copylist() copies from options.dest_fstype to out_node.fstype
// (c) copy_file() checks to see whether (dest_file.fstype == PAN_FS)
enum FSType {
   UNKNOWN_FS =0,
   PAN_FS,
};

//Structs and typedefs
//options{
struct options {
    int     verbose;            				// each '-v' increases verbosity
    int     debug;           					// each '-g' increases diagnostic-level
    int     recurse;
    int     logging;
    FSType  dest_fstype;					// specifies the FS type of the destination
    int     different;
    int     parallel_dest;
    int     work_type;
    int     meta_data_only;
    size_t  blocksize;
    size_t  chunk_at;
    size_t  chunksize;
    int     preserve;           				// attempt to preserve ownership during copies.

    char exclude[PATHSIZE_PLUS]; 				// pattern/list to exclude

    char    file_list[PATHSIZE_PLUS];
    int     use_file_list;
    char    jid[128];

    int     max_readdir_ranks;

#if GEN_SYNDATA
    char    syn_pattern[128];					// a file holding a pattern to be used when generating synthetic data
    char    syn_suffix[SYN_SUFFIX_MAX];				// holds the suffix for syntheticlly generated files - should be the syn_size as specified by the user
    size_t  syn_size;						// the size of each syntheticlly generated file
#endif

    // see ANYFS, etc, #define'd above.
    SrcDstFSType  sourcefs;
    SrcDstFSType  destfs;
};

struct worker_proc_status {
    char inuse;
    char readdir;
};

// the basic object used by pftool internals
typedef struct path_item {
    int start; // tells us if this path item was created by the inital list provided by the user
    FileType      ftype;
    FileType      dest_ftype;
    FSType        fstype;       // the file system type of the source file
    struct stat   st;

   // the transfer chunk size of the file. For non-chunked file, this is the
   // tranfer length or file length
    off_t         chksz;
    int           chkidx;              // the chunk index or number of the chunk being processed
    int           packable;
    int           temp_flag;

   // keep this last, for efficient init
    char          path[PATHSIZE_PLUS];
    char          timestamp[DATE_STRING_MAX];
} path_item;

// A queue to store all of our input nodes
typedef struct path_list {
    path_item data;
    struct path_list *next;
} path_list;

typedef struct work_buf_list {
    char *buf;
    int size;
    struct work_buf_list *next;
} work_buf_list;

typedef struct pod_stat
{
	size_t buff_size;
	char* buffer;
} pod_data;

typedef struct repo_timing_stats
{
	int tot_stats;
	int total_blk;
	int has_data;
	int total_pods;
	std::map<int, pod_data*> pod_to_stat;
} repo_stats;


//Function Declarations
void  usage();
char *printmode (mode_t aflag, char *buf);
void  trim_trailing(int ch, char* str);
const char *cmd2str(OpCode cmdidx);

//char *get_base_path(const char *path, int wildcard);
void  get_base_path(char* base_path, const path_item* path, int wildcard);
//void get_dest_path(path_item beginning_node, const char *dest_path, path_item *dest_node, int makedir, int num_paths, struct options o);
void  get_dest_path(path_item *dest_node, const char *dest_path, const path_item* beginning_node,
                    int makedir, int num_paths, struct options& o);
//char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, struct options o);
void  get_output_path(char* output_path, const char *base_path, const path_item* src_node, const path_item* dest_node, struct options& o);
void  get_output_path(path_item* out_node, const char *base_path, const path_item* src_node, const path_item* dest_node, struct options& o, int rename_flag);

//int one_byte_read(const char *path);
int   one_byte_read(const char *path);
ssize_t write_field(int fd, void *start, size_t len);
int mkpath(char *thePath, mode_t perms);
int   compare_file(path_item* src_file, path_item* dest_file, size_t blocksize, int meta_data_only, struct options& o);

//local functions
int  request_response(int type_cmd);
int  request_input_queuesize();
void send_command(int target_rank, int type_cmd);
void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count);
void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize);

//worker utility functions
void errsend(int fatal, const char *error_text);
void errsend_fmt(int fatal, const char *format, ...);

//void get_stat_fs_info(path_item *work_node, int *sourcefs, char *sourcefsc);
int  stat_item(path_item* work_node, struct options& o);
void get_stat_fs_info(const char *path, SrcDstFSType *fs);
int  get_free_rank(struct worker_proc_status *proc_status, int start_range, int end_range);
int  processing_complete(struct worker_proc_status *proc_status, int free_worker_count, int nproc);

//function definitions for manager
void send_manager_regs_buffer(path_item *buffer, int *buffer_count);
void send_manager_dirs_buffer(path_item *buffer, int *buffer_count);
void send_manager_new_buffer(path_item *buffer, int *buffer_count);
void send_manager_nonfatal_inc();
void send_manager_chunk_busy();
void send_manager_copy_stats(int num_copied_files, size_t num_copied_bytes);
void send_manager_examined_stats(int num_examined_files, size_t num_examined_bytes, int num_examined_dirs, size_t num_finished_bytes);
void send_manager_work_done(int ignored);
void send_manager_timing_stats(int tot_stats, int pod_id, int total_blk, size_t timing_stats_buff_size, char* repo, char* timing_stats);

//function definitions for workers
void update_chunk(path_item *buffer, int *buffer_count);
void write_output(const char *message, int log);
void write_output_fmt(int log, const char *fmt, ...);
void write_buffer_output(char *buffer, int buffer_size, int buffer_count);

void send_worker_queue_count(int target_rank, int queue_count);
void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize);
void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize);
void send_worker_compare_path(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize);
void send_worker_exit(int target_rank);

//function definitions for queues
void enqueue_path(path_list **head, path_list **tail, char *path, int *count);
void print_queue_path(path_list *head);
void delete_queue_path(path_list **head, int *count);
void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count);
void dequeue_node(path_list **head, path_list **tail, int *count);
void pack_list(path_list *head, int count, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize);

//function definitions for workbuf_list;
void enqueue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize, char *buffer, int buffer_size);
void dequeue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize);
void delete_buf_list (work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize);

// functions with signatures that involve C++ Path sub-classes, etc
// (Path subclasses are also used internally by other util-functions.)
#include "Path.h"
int samefile(PathPtr p_src, PathPtr p_dst, const struct options& o);
int copy_file(PathPtr p_src, PathPtr p_dest, size_t blocksize, int rank, struct options& o);
int update_stats(PathPtr p_src, PathPtr p_dst, struct options& o);
int  check_temporary(PathPtr p_src, path_item* out_node);
int epoch_to_string(char* str, size_t size, const time_t* time);

//void send_manager_timing_stats(int tot_stats, int pod_id, int total_blk, size_t timing_stats_buff_size, char* repo, char* timing_stats);

#endif

