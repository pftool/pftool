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
//#include "hashtbl.h"
#include <errno.h>
#include <utime.h>

#include "str.h"

//mpi
#include "mpi.h"

//gpfs
#ifdef TAPE
#  ifdef HAVE_GPFS_H
#    include <gpfs.h>
#    ifdef HAVE_GPFS_FCNTL_H
#      include "gpfs_fcntl.h"
#    endif
#  endif

#  ifdef HAVE_DMAPI_H
#    include <dmapi.h>
#  endif

#endif

//fuse
#ifdef FUSE_CHUNKER
#  include <attr/xattr.h>
#endif

#ifdef PLFS
#  include "plfs.h"
#endif

//synthetic data generation
#ifdef GEN_SYNDATA
#  include "syndata.h"
#else
   typedef void*  SyndataBufPtr;   /* eliminates some need for #ifdefs */
#  define syndataExists(PTR)  (PTR)  /* eliminates some need for #ifdefs */
#endif


#include "debug.h"


#define PATHSIZE_PLUS  (FILENAME_MAX+30)
#define ERRORSIZE      PATHSIZE_PLUS
#define MESSAGESIZE    PATHSIZE_PLUS
#define MESSAGEBUFFER  400

#define DIRBUFFER      5
#define STATBUFFER     128
#define COPYBUFFER     128
#define CHUNKBUFFER    COPYBUFFER
#define TAPEBUFFER     5

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
   FUSEFS     = 4,

   S3FS       = 5,   // everything after here is REST-ful (see REST_FS)
   PLFSFS     = 6,
   MARFSFS    = 7
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

enum WorkType {
    COPYWORK,
    LSWORK,
    COMPAREWORK
};

enum FileType {
   NONE = 0,                    // deleted?  irrelevant?  see process_stat_buffer()
   TBD,                         // not yet initialized by factory

   REGULARFILE,
   FUSEFILE,
   PREMIGRATEFILE,              // for TAPE
   MIGRATEFILE,                 // for TAPE
   PLFSFILE,
   S3FILE,
   SYNDATA,                     // synthetic data (no file)
   MARFSFILE,

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
    int     verbose;            // each '-v' increases verbosity
    int     debug;           // each '-g' increases diagnostic-level
    int     recurse;
    int     logging;
    FSType  dest_fstype;			// specifies the FS type of the destination
    int     different;
    int     parallel_dest;
    int     work_type;
    int     meta_data_only;
    size_t  blocksize;
    size_t  chunk_at;
    size_t  chunksize;
    int     preserve;           // attempt to preserve ownership during copies.

    char exclude[PATHSIZE_PLUS]; // pattern/list to exclude

    char    file_list[PATHSIZE_PLUS];
    int     use_file_list;
    char    jid[128];

    int     max_readdir_ranks;

#if GEN_SYNDATA
    char    syn_pattern[128];
    size_t  syn_size;
#endif

#ifdef FUSE_CHUNKER
    char    archive_path[PATHSIZE_PLUS];
    char    fuse_path[PATHSIZE_PLUS];
    int     use_fuse;
    int     fuse_chunkdirs;
    size_t  fuse_chunk_at;
    size_t  fuse_chunksize;
#endif

#ifdef PLFS
    size_t  plfs_chunksize;
#endif

    // see ANYFS, etc, #define'd above.
    SrcDstFSType  sourcefs;
    SrcDstFSType  destfs;
};

struct worker_proc_status {
    char inuse;
    char readdir;
};

// A queue to store all of our input nodes
//struct path_link {
//    char path[PATHSIZE_PLUS];				// full path of file (or directory) to process
//    struct stat st;					// stat info of file/directory
//    int chkidx;						// the chunk index or number of the chunk being processed
//    off_t chksz;					// the tranfer chunk size of the file. For non-chunked file, this is the tranfer length or file length
//    enum filetype ftype;				// the "type" of the source file. Type is influenced by where/what the source is stored
//    enum filetype desttype;				// the "type" of the destination file
//    char fstype[128];					// the file system type of the source file
//};
//typedef struct path_link path_item;
//
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
   //    off_t         offset;
   //    size_t        length;              // (remaining) data in the file
    char          path[PATHSIZE_PLUS]; // keep this last, for efficient init
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


//Function Declarations
void  usage();
char *printmode (mode_t aflag, char *buf);
void  trim_trailing(int ch, char* str);

//char *get_base_path(const char *path, int wildcard);
void  get_base_path(char* base_path, const path_item* path, int wildcard);
//void get_dest_path(path_item beginning_node, const char *dest_path, path_item *dest_node, int makedir, int num_paths, struct options o);
void  get_dest_path(path_item *dest_node, const char *dest_path, const path_item* beginning_node,
                    int makedir, int num_paths, struct options& o);
//char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, struct options o);
void  get_output_path(char* output_path, const char *base_path, const path_item* src_node, const path_item* dest_node, struct options& o);
void  get_output_path(path_item* out_node, const char *base_path, const path_item* src_node, const path_item* dest_node, struct options& o);

//int one_byte_read(const char *path);
int   one_byte_read(const char *path);
ssize_t write_field(int fd, void *start, size_t len);
int mkpath(char *thePath, mode_t perms);
//#ifdef GEN_SYNDATA
//int copy_file(path_item src_file, path_item dest_file, size_t blocksize, syndata_buffer *synbuf, int rank);
//#else
//int copy_file(path_item src_file, path_item dest_file, size_t blocksize, int rank);
//#endif
int   copy_file(path_item* src_file, path_item* dest_file, size_t blocksize, int rank, SyndataBufPtr synbuf, struct options& o);

int   compare_file(path_item* src_file, path_item* dest_file, size_t blocksize, int meta_data_only, struct options& o);
int   update_stats(path_item* src_file, path_item* dest_file, struct options& o);

//dmapi/gpfs specfic
#ifdef TAPE
int   read_inodes(const char *fnameP, gpfs_ino_t startinode, gpfs_ino_t endinode, int *dmarray);
int   dmapi_lookup (char *mypath, int *dmarray, char *dmouthexbuf);
#endif


//local functions
int  request_response(int type_cmd);
int  request_input_queuesize();
void send_command(int target_rank, int type_cmd);
void send_path_list(int target_rank, int command, int num_send, path_list **list_head, path_list **list_tail, int *list_count);
void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count);
void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, int *workbufsize);

//worker utility functions
void errsend(int fatal, const char *error_text);
void errsend_fmt(int fatal, const char *format, ...);

#ifdef FUSE_CHUNKER
int  is_fuse_chunk(const char *path, struct options& o);
void set_fuse_chunk_data(path_item *work_node);
int  get_fuse_chunk_attr(const char *path, off_t offset, size_t length, struct utimbuf *ut, uid_t *userid, gid_t *groupid);
int  set_fuse_chunk_attr(const char *path, off_t offset, size_t length, struct utimbuf ut, uid_t userid, gid_t groupid);
#endif

//void get_stat_fs_info(path_item *work_node, int *sourcefs, char *sourcefsc);
int  stat_item(path_item* work_node, struct options& o);
void get_stat_fs_info(const char *path, SrcDstFSType *fs);
int  get_free_rank(struct worker_proc_status *proc_status, int start_range, int end_range);
int  processing_complete(struct worker_proc_status *proc_status, int nproc);

//function definitions for manager
void send_manager_regs_buffer(path_item *buffer, int *buffer_count);
void send_manager_dirs_buffer(path_item *buffer, int *buffer_count);

#ifdef TAPE
void send_manager_tape_buffer(path_item *buffer, int *buffer_count);
#endif

void send_manager_new_buffer(path_item *buffer, int *buffer_count);
void send_manager_nonfatal_inc();
void send_manager_chunk_busy();
void send_manager_copy_stats(int num_copied_files, size_t num_copied_bytes);
void send_manager_examined_stats(int num_examined_files, size_t num_examined_bytes, int num_examined_dirs, size_t num_finished_bytes);
void send_manager_tape_stats(int num_examined_tapes, size_t num_examined_tape_bytes);
void send_manager_work_done(int ignored);

//function definitions for workers
void update_chunk(path_item *buffer, int *buffer_count);
void write_output(const char *message, int log);
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

// functions with signatures that involve C++ Path sub-classes, etc
// (Path subclasses are also used internally by other util-functions.)
#include "Path.h"
int samefile(PathPtr p_src, PathPtr p_dst, const struct options& o);


#endif

