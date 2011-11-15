/*************************************************************************************
* Name: pftool
*
* Description:
*
* Author:  Gary Grider / Alfred Torrez / Ben McClelland / HB Chen / Aaron Torres
*
*************************************************************************************/
//Standard includes
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>

//Regular Filesystem
#include <sys/vfs.h>

// include that is associated with pftool itself
#include "pftool.h"
#include "debug.h"
#include "recall_api.h"

#define STGPOOLSERVER_PORT 1664

#ifdef THREADS_ONLY      
#define MPI_Abort MPY_Abort
#define MPI_Pack MPY_Pack
#define MPI_Unpack MPY_Unpack
#endif

//External Declarations
extern void usage();
extern char *printmode (mode_t aflag, char *buf);
extern char *get_base_path(const char *path, int wildcard);
extern void get_dest_path(const char *beginning_path, const char *dest_path, path_item *dest_node, int makedir, int num_paths, struct options o);
extern char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, struct options o);
extern int copy_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st);
extern int compare_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st, int meta_data_only);

//dmapi/gpfs
#ifndef DISABLE_TAPE
extern int read_inodes(const char *fnameP, gpfs_ino_t startinode, gpfs_ino_t endinode, int *dmarray);
extern int dmapi_lookup (char *mypath, int *dmarray, char *dmouthexbuf);
#endif

//manager
extern void send_manager_nonfatal_inc();
extern void send_manager_chunk_busy();
extern void send_manager_regs_buffer(path_item *buffer, int *buffer_count);
extern void send_manager_dirs_buffer(path_item *buffer, int *buffer_count);

#ifndef DISABLE_TAPE
extern void send_manager_tape_buffer(path_item *buffer, int *buffer_count);
#endif

extern void send_manager_new_buffer(path_item *buffer, int *buffer_count);
extern void send_manager_work_done();

//worker
extern void write_output(char *message);
extern void write_buffer_output(char *buffer, int buffer_size, int buffer_count);
extern void send_worker_queue_count(int target_rank, int queue_count);
extern void send_worker_stat_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
extern void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
extern void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
extern void send_worker_compare_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize);
extern void send_worker_exit();

//functions that use workers
extern void errsend(int fatal, char *error_text);
extern int is_fuse_chunk(const char *path);
extern void set_fuse_chunk_data(path_item *work_node);
extern int get_free_rank(int *proc_status, int start_range, int end_range);
extern int processing_complete(int *proc_status, int nproc);

//queues 
extern void enqueue_path(path_list **head, path_list **tail, char *path, int *count);
extern void print_queue_path(path_list *head);
extern void delete_queue_path(path_list **head, int *count);
extern void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count);
extern void dequeue_node(path_list **head, path_list **tail, int *count);
extern void pack_list(path_list *head, int count, work_buf_list **workbuflist, int *workbufsize);

//buffers
extern void enqueue_buf_list(work_buf_list **workbuflist, int *workbufsize, char *buffer, int buffer_size);
extern void dequeue_buf_list(work_buf_list **workbuflist, int *workbufsize);
extern void delete_buf_list(work_buf_list **workbuflist, int *workbufsize);

//fake mpi
extern int MPY_Pack(void *inbuf, int incount, MPI_Datatype datatype, void *outbuf, int outcount, int *position, MPI_Comm comm);
extern int MPY_Unpack(void *inbuf, int insize, int *position, void *outbuf, int outcount, MPI_Datatype datatype, MPI_Comm comm);
extern int MPY_Abort(MPI_Comm comm, int errorcode);

int main(int argc, char *argv[]){
  //general variables
  int i;

  //mpi
  int rank = 0;
  int nproc = 0;
  int mpi_ret_code = 0;

  //getopt
  int c;
  struct options o;

  //queues
  path_list *input_queue_head = NULL, *input_queue_tail = NULL;
  int input_queue_count = 0;
  
  //paths
  char src_path[PATHSIZE_PLUS], dest_path[PATHSIZE_PLUS];
  struct stat dest_stat;
  int statrc;

  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
    fprintf(stderr, "Error in MPI_Init\n");
    return -1;
  }

  // Get the number of procs
  if (MPI_Comm_size(MPI_COMM_WORLD, &nproc) != MPI_SUCCESS) {
    fprintf(stderr, "Error in MPI_Comm_size\n");
    return -1;
  }

  // Get your rank
  if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
    fprintf(stderr, "Error in MPI_Comm_rank\n");
    return -1;
  }

  //Process using getopt
    //initialize options
  if (rank == MANAGER_PROC){
    o.verbose = 0;
    o.use_file_list = 0;
    o.recurse = 0;
    o.meta_data_only = 1;
    strncpy(o.jid, "TestJob", 128);
    o.work_type = LSWORK;

    // start MPI - if this fails we cant send the error to the output proc so we just die now 
    while ((c = getopt(argc, argv, "p:c:j:w:i:vrMnh")) != -1) 
      switch(c){
        case 'p':
          //Get the source/beginning path
          strncpy(src_path, optarg, PATHSIZE_PLUS);        
          break;
        case 'c':
          //Get the destination path
          strncpy(dest_path, optarg, PATHSIZE_PLUS);        
          break;
        case 'j':
          strncpy(o.jid, optarg, 128);
          break;
        case 'w':
          o.work_type = atoi(optarg);
          break;
        case 'i':
          strncpy(o.file_list, optarg, PATHSIZE_PLUS);
          o.use_file_list = 1;
          break;
        case 'n':
          //different
          o.different = 1;
        case 'r':
          //Recurse
          o.recurse = 1;
          break;
        case 'M':
          o.meta_data_only = 0;
          break;
        case 'v':
          o.verbose = 1;
          break;
        case 'h':
          //Help -- incoming!
          usage();
          return 0;
        case '?': 
          return -1;
        default:
          break;
      }
  }

  MPI_Barrier(MPI_COMM_WORLD);


  //broadcast all the options
  mpi_ret_code = MPI_Bcast(&o.verbose, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
  mpi_ret_code = MPI_Bcast(&o.use_file_list, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
  mpi_ret_code = MPI_Bcast(&o.recurse, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
  mpi_ret_code = MPI_Bcast(&o.meta_data_only, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
  mpi_ret_code = MPI_Bcast(o.jid, 128, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
  mpi_ret_code = MPI_Bcast(&o.work_type, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);

  //Modifies the path based on recursion/wildcards
  //wildcard
  if (rank == MANAGER_PROC){
    if (optind < argc && (o.work_type == COPYWORK || o.work_type == COMPAREWORK)){
      statrc = lstat(dest_path, &dest_stat);
      if (statrc < 0 || !S_ISDIR(dest_stat.st_mode)){
        printf("Multiple inputs and target '%s' is not a directory\n", dest_path);
        MPI_Abort(MPI_COMM_WORLD, -1);
      }
    }
  }
 
      

  //process remaining optind for * and multiple src files
  // stick them on the input_queue
  if (rank == MANAGER_PROC && optind < argc){
    enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
    for (i = optind; i < argc; ++i){
      enqueue_path(&input_queue_head, &input_queue_tail, argv[i], &input_queue_count);
    }
  }
  else if (rank == MANAGER_PROC && o.use_file_list == 0){
    enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
  }
  else if (rank == MANAGER_PROC){
    enqueue_path(&input_queue_head, &input_queue_tail, o.file_list, &input_queue_count);
  }
  
  if (rank == MANAGER_PROC){
    manager(rank, o, nproc, input_queue_head, input_queue_tail, input_queue_count, dest_path);
  }
  else{
    worker(rank, o);
  }

  //Program Finished
  //printf("%d -- done.\n", rank);
  MPI_Finalize(); 
  return 0;
}


void manager(int rank, struct options o, int nproc, path_list *input_queue_head, path_list *input_queue_tail, int input_queue_count, const char *dest_path){
  MPI_Status status;
#ifndef THREADS_ONLY
  int message_ready = 0, probecount = 0;
  int prc;
#endif
  int type_cmd;
  int work_rank, sending_rank;

  int i;
  int *proc_status;
  struct timeval in, out;
  int non_fatal = 0, examined_file_count = 0, dir_count = 0;
  double examined_byte_count = 0;
  
#ifndef DISABLE_TAPE
  int tape_count = 0;
#endif

  int makedir = 0;

  char message[MESSAGESIZE], errmsg[MESSAGESIZE];
  char beginning_path[PATHSIZE_PLUS], base_path[PATHSIZE_PLUS], temp_path[PATHSIZE_PLUS];
  struct stat st;

  path_item dest_node;

  path_list *iter = NULL;
  int  num_copied_files = 0;
  double num_copied_bytes = 0;

  work_buf_list *stat_buf_list = NULL, *process_buf_list = NULL, *dir_buf_list = NULL;
  int stat_buf_list_size = 0, process_buf_list_size = 0, dir_buf_list_size = 0;

#ifndef DISABLE_TAPE
  work_buf_list *tape_buf_list = NULL;
  int tape_buf_list_size = 0;
#endif
    
  int mpi_ret_code, rc;




  //path stuff
  int wildcard = 0;
  if (input_queue_count > 1){
    wildcard = 1;
  }
  
  //make directories if it's a copy job
  if (o.work_type == COPYWORK){
    makedir = 1;
  }
  

  if (!o.use_file_list){
    //setup paths
    strncpy(beginning_path, input_queue_head->data.path, PATHSIZE_PLUS);
    strncpy(base_path, get_base_path(beginning_path, wildcard), PATHSIZE_PLUS);
    if (o.work_type != LSWORK){
      get_dest_path(beginning_path, dest_path, &dest_node, makedir, input_queue_count, o);

      //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the dest_path: %s\n", rank, dest_path);
      mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
      if (mpi_ret_code < 0){
        errsend(FATAL, "Failed to Bcast dest_path");
      }
    }
    //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the base_path: %s\n", rank, base_path);
    mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    if (mpi_ret_code < 0){
      errsend(FATAL, "Failed to Bcast base_path");
    }
  }


  iter = input_queue_head;
  if (strncmp(base_path, ".", PATHSIZE_PLUS) != 0 && o.recurse == 1 && o.work_type != LSWORK){
    while (iter != NULL){
      if (strncmp(get_base_path(iter->data.path, wildcard), base_path, PATHSIZE_PLUS) != 0){
        errsend(FATAL, "All sources for a recursive operation must be contained within the same directory.");
      }
      iter = iter->next;
    }
  }

  //quick check that source is not nested
  strncpy(temp_path, dirname(strndup(dest_path, PATHSIZE_PLUS)), PATHSIZE_PLUS);
  rc = stat(temp_path, &st);
  if (rc < 0){
    snprintf(errmsg, MESSAGESIZE, "%s: No such file or directory", dest_path);
    errsend(FATAL, errmsg);
  }
    
  //pack our list into a buffer:
  
  if (o.use_file_list){
    pack_list(input_queue_head, input_queue_count, &dir_buf_list, &dir_buf_list_size);
  }
  else{
    pack_list(input_queue_head, input_queue_count, &stat_buf_list, &stat_buf_list_size);
  }
  delete_queue_path(&input_queue_head, &input_queue_count);
  

  //proc stuff
  proc_status = malloc(nproc * sizeof(int));
  //initialize proc_status
  for (i = 0; i < nproc; i++){
    proc_status[i] = 0;
  }
  

  sprintf(message, "INFO  HEADER   ========================  %s  ============================\n", o.jid);
  write_output(message);
  sprintf(message, "INFO  HEADER   Starting Path: %s\n", beginning_path);
  write_output(message);

  //starttime
  gettimeofday(&in, NULL);

  while (1){
    //poll for message
#ifndef THREADS_ONLY
    while ( message_ready == 0){ 
      prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
      if (prc != MPI_SUCCESS) {
        errsend(FATAL, "MPI_Iprobe failed\n");
        message_ready = -1; 
      }   
      else{
        probecount++;
      }

      if  (probecount % 3000 == 0){
        PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
        PRINT_POLL_DEBUG("process_buf_list_size = %d\n", process_buf_list_size);
        PRINT_POLL_DEBUG("stat_buf_list_size = %d\n", stat_buf_list_size);
        PRINT_POLL_DEBUG("dir_buf_list_size = %d\n", dir_buf_list_size);
      }   
      //we didn't get any new messages from workers
      if (message_ready == 0){
#endif
        
        for (i = 0; i < nproc; i++){
          PRINT_PROC_DEBUG("Rank %d, Status %d\n", i, proc_status[i]);
        }
        PRINT_PROC_DEBUG("=============\n");

        //work_rank = get_free_rank(proc_status, 3, nproc - 1);
        work_rank = get_free_rank(proc_status, 3, nproc - 1);
        if ((o.recurse && work_rank != -1 && dir_buf_list_size != 0) || (o.use_file_list && dir_buf_list_size != 0 && stat_buf_list_size < nproc*3)){
          proc_status[work_rank] = 1;
          send_worker_readdir(work_rank, &dir_buf_list, &dir_buf_list_size);
        }
        else if (!o.recurse){
          delete_buf_list(&dir_buf_list, &dir_buf_list_size);
        }

        if (o.work_type == COPYWORK){
          for (i = 0; i < 3; i ++){
            work_rank = get_free_rank(proc_status, 3, nproc - 1);
            if (work_rank > -1 && process_buf_list_size > 0){
              proc_status[work_rank] = 1;
              send_worker_copy_path(work_rank, &process_buf_list, &process_buf_list_size);
            }
          }
        }
        else if (o.work_type == COMPAREWORK){
          for (i = 0; i < 3; i ++){
            work_rank = get_free_rank(proc_status, 3, nproc - 1);
            if (work_rank > -1 && process_buf_list_size > 0){
              proc_status[work_rank] = 1;
              send_worker_compare_path(work_rank, &process_buf_list, &process_buf_list_size);
            }
          }
        }
        else{
          //delete the queue here
          delete_buf_list(&process_buf_list, &process_buf_list_size);

#ifndef DISABLE_TAPE
          delete_buf_list(&tape_buf_list, &tape_buf_list_size);
#endif

        }

        work_rank = get_free_rank(proc_status, 3, nproc - 1);
        if (work_rank > -1 && stat_buf_list_size != 0){
          proc_status[work_rank] = 1;
          send_worker_stat_path(work_rank, &stat_buf_list, &stat_buf_list_size);
        }

#ifndef THREADS_ONLY
      }
      //are we finished?
      if (process_buf_list_size == 0 && stat_buf_list_size == 0 && dir_buf_list_size == 0 && processing_complete(proc_status, nproc) == 0){
        break;
      }
      usleep(1);
    }   
#endif

    if (process_buf_list_size == 0 && stat_buf_list_size == 0 && dir_buf_list_size == 0 && processing_complete(proc_status, nproc) == 0){
      break;
    }

    //grab message type
    PRINT_MPI_DEBUG("rank %d: manager() Receiving the message type\n", rank);
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive type_cmd\n");
    }   
    sending_rank = status.MPI_SOURCE;


    //do operations based on the message
    switch(type_cmd){ 
      case WORKDONECMD:
        //worker finished their tasks
        manager_workdone(rank, sending_rank, proc_status);       
        break;
      case NONFATALINCCMD:
        //non fatal errsend encountered
        non_fatal++;
        break;
      case CHUNKBUSYCMD:
        proc_status[ACCUM_PROC] = 1;
        break;
      case COPYSTATSCMD:
        manager_add_copy_stats(rank, sending_rank, &num_copied_files, &num_copied_bytes);
        break;
      case EXAMINEDSTATSCMD:
        manager_add_examined_stats(rank, sending_rank, &examined_file_count, &examined_byte_count);
        break;
      case PROCESSCMD:
          manager_add_buffs(rank, sending_rank, &process_buf_list, &process_buf_list_size);
        break;
      case DIRCMD:
        dir_count += manager_add_buffs(rank, sending_rank, &dir_buf_list, &dir_buf_list_size);
        if (o.recurse == 0){
          delete_buf_list(&dir_buf_list, &dir_buf_list_size);
        }
        break;
#ifndef DISABLE_TAPE
      case TAPECMD:
        tape_count += manager_add_buffs(rank, sending_rank, &tape_buf_list, &tape_buf_list_size);
        delete_buf_list(&tape_buf_list, &tape_buf_list_size);
        break;
#endif
      case INPUTCMD:
        manager_add_buffs(rank, sending_rank, &stat_buf_list, &stat_buf_list_size);
        break;
      case QUEUESIZECMD:
        send_worker_queue_count(sending_rank, stat_buf_list_size);
        break;
      default:
        break;
    }
#ifndef THREADS_ONLY
    message_ready = 0;
#endif
    
    
  }
  gettimeofday(&out, NULL);
  int elapsed_time = out.tv_sec - in.tv_sec;
  //Manager is done, cleaning have the other ranks exit
  sprintf(message, "INFO  FOOTER   ========================   NONFATAL ERRORS = %d   ================================\n", non_fatal);
  write_output(message);
  sprintf(message, "INFO  FOOTER   =================================================================================\n");
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Files/Links Examined: %d\n", examined_file_count);
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Bytes: %0.0f\n", examined_byte_count);
  write_output(message);

#ifndef DISABLE_TAPE
  sprintf(message, "INFO  FOOTER   Total Files on Tape: %d\n", tape_count);
  write_output(message);
#endif

  sprintf(message, "INFO  FOOTER   Total Dirs Examined: %d\n", dir_count);
  write_output(message);
  
  if (o.work_type == COPYWORK){
    sprintf(message, "INFO  FOOTER   Total Files Copied: %d\n", num_copied_files);
    write_output(message);
    sprintf(message, "INFO  FOOTER   Total Bytes Copied: %0.0f\n", num_copied_bytes);
    write_output(message);
    if ((num_copied_bytes/(1024*1024)) > 0 ){
      sprintf(message, "INFO  FOOTER   Total Megabytes Copied: %0.0f\n", (num_copied_bytes/(1024*1024)));
      write_output(message);
    }
    
    if((num_copied_bytes/(1024*1024)) > 0 ){
      sprintf(message, "INFO  FOOTER   Data Rate: %0.0f MB/second\n", (num_copied_bytes/(1024*1024))/(elapsed_time+1));
      write_output(message);
    }
  }
  else if (o.work_type == COMPAREWORK){
    sprintf(message, "INFO  FOOTER   Total Files Compared: %d\n", num_copied_files);
    write_output(message);
    if (o.meta_data_only == 0){
      sprintf(message, "INFO  FOOTER   Total Bytes Compared: %0.0f\n", num_copied_bytes);
      write_output(message);
    }

  }
  if (elapsed_time == 1){
    sprintf(message, "INFO  FOOTER   Elapsed Time: %d second\n", elapsed_time);
  }
  else{
    sprintf(message, "INFO  FOOTER   Elapsed Time: %d seconds\n", elapsed_time);
  }
  write_output(message);


  for(i = 1; i < nproc; i++){
    send_worker_exit(i);
  }

  //free any allocated stuff
  free(proc_status);
}

int manager_add_paths(int rank, int sending_rank, path_list **queue_head, path_list **queue_tail, int *queue_count){
  MPI_Status status;
  int path_count;

  path_list *work_node = malloc(sizeof(path_list));
  char path[PATHSIZE_PLUS];
  char *workbuf;
  int worksize, position;
  
  int i;  

  //gather the # of files
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive path_count\n");
  }
  worksize =  path_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }
  
  position = 0;
  for (i = 0; i < path_count; i++){
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Unpacking the work_node from rank %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node->data, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node->data.path, PATHSIZE_PLUS); 
    enqueue_node(queue_head, queue_tail, work_node, queue_count);
  }
  free(work_node);
  free(workbuf);

  return path_count;

}

int manager_add_buffs(int rank, int sending_rank, work_buf_list **workbuflist, int *workbufsize){
  MPI_Status status;
  int path_count;
  char *workbuf;
  int worksize;

  //gather the # of files
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive path_count\n");
  }
  worksize =  path_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }

  if (path_count > 0){
    enqueue_buf_list(workbuflist, workbufsize, workbuf, path_count);
  }

  return path_count;
}

void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, double *num_copied_bytes){
  MPI_Status status;
  int num_files;
  double num_bytes;
  //gather the # of copied files
  PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_files from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_files, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }
  
  //gather the # of copied byes
  PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_bytes from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_bytes, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }

  *num_copied_files += num_files;
  *num_copied_bytes += num_bytes;

}

void manager_add_examined_stats(int rank, int sending_rank, int *num_examined_files, double *num_examined_bytes){
  MPI_Status status;
  int num_files = 0;
  double num_bytes = 0;
  
  //gather the # of examined files
  PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_files from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_files, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }

  PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_bytes from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_bytes, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }

  *num_examined_files += num_files;
  *num_examined_bytes += num_bytes;

}

void manager_workdone(int rank, int sending_rank, int *proc_status){
  proc_status[sending_rank] = 0;
}

void worker(int rank, struct options o){
  MPI_Status status;
  int sending_rank;
  int all_done = 0;
  int makedir = 0;

#ifndef THREADS_ONLY
  int message_ready = 0, probecount = 0;
  int prc;
#endif

  char *output_buffer;
  

  int type_cmd;
  int mpi_ret_code;
  char base_path[PATHSIZE_PLUS];
  path_item dest_node;

  //variables stored by the 'accumulator' proc
  HASHTBL *chunk_hash;
  int base_count = 100, hash_count = 0;

  int output_count = 0;

  if (rank == OUTPUT_PROC){
    output_buffer = (char *) malloc(MESSAGESIZE*MESSAGEBUFFER*sizeof(char));
    memset(output_buffer,'\0', sizeof(output_buffer));
  }

  if (rank == ACCUM_PROC){
    if(!(chunk_hash=hashtbl_create(base_count, NULL))) {
      errsend(FATAL, "hashtbl_create() failed\n");
    }
  }

  if (o.work_type == COPYWORK){
    makedir = 1;
  }


  if (!o.use_file_list){
    //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the dest_path\n", rank);
    if (o.work_type != LSWORK){
      mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
      if (mpi_ret_code < 0){
        errsend(FATAL, "Failed to Receive Bcast dest_path");
      }

    }
    //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the base_path\n", rank);
    mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    if (mpi_ret_code < 0){
      errsend(FATAL, "Failed to Receive Bcast base_path");
    }


    get_stat_fs_info(base_path, &o.sourcefs);
    if (o.work_type != LSWORK){
      get_stat_fs_info(dest_node.path, &o.destfs);
    }
  }

  //change this to get request first, process, then get work    
  while ( all_done == 0){
#ifndef THREADS_ONLY
    //poll for message
    while ( message_ready == 0){
      prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
      if (prc != MPI_SUCCESS) {
        errsend(FATAL, "MPI_Iprobe failed\n");
      }
      else{
        probecount++;
      }

      if  (probecount % 3000 == 0){
        PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
      }
      usleep(1);
    }
#endif

    //grab message type
    PRINT_MPI_DEBUG("rank %d: worker() receiving the type_cmd\n", rank);
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive type_cmd\n");
    }
    sending_rank = status.MPI_SOURCE;

    //do operations based on the message
    switch(type_cmd){
      case OUTCMD:
        worker_output(rank, sending_rank, output_buffer, &output_count, o);
        break;
      case BUFFEROUTCMD:
        worker_buffer_output(rank, sending_rank, output_buffer, &output_count, o);
        break;
      case UPDCHUNKCMD:
        worker_update_chunk(rank, sending_rank, &chunk_hash, &hash_count, base_path, dest_node, o);
        break;
      case STATCMD:
        worker_stat(rank, sending_rank, base_path, dest_node, o);
        break;
      case DIRCMD:
        worker_readdir(rank, sending_rank, base_path, dest_node, makedir, o);
        break;
      case COPYCMD:
        worker_copylist(rank, sending_rank, base_path, dest_node, o);
        break;
      case COMPARECMD:
        worker_comparelist(rank, sending_rank, base_path, dest_node, o);
        break;
      case EXITCMD:
        all_done = 1;
        break;
      default:
        break;
    }
#ifndef THREADS_ONLY
    message_ready = 0;
#endif
  }
  if (rank == ACCUM_PROC){
    hashtbl_destroy(chunk_hash);
  }
  if (rank == OUTPUT_PROC){
    worker_flush_output(output_buffer, &output_count);
    free(output_buffer);
  }
}

void worker_update_chunk(int rank, int sending_rank, HASHTBL **chunk_hash, int *hash_count, const char *base_path, path_item dest_node, struct options o){
  MPI_Status status;
  int path_count;

  path_item work_node;
  char *workbuf;
  int worksize, position;
  int hash_value, chunk_size, new_size;
  int num_copied_files = 0;
  double num_copied_bytes = 0;

  int i;  

  //gather the # of files
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive path_count\n");
  }
  worksize =  path_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }
  
  position = 0;
  for (i = 0; i < path_count; i++){
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Unpacking the work_node from rank %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
    hash_value = hashtbl_get(*chunk_hash, work_node.path);
    chunk_size = work_node.length;
    num_copied_bytes += chunk_size;

    if (hash_value == -1){
      //resize the hashtable if needed
      if (*hash_count == (*chunk_hash)->size){
        hashtbl_resize(*chunk_hash, *hash_count+100);
      }
      *hash_count += 1;
      new_size = chunk_size;
    }
    else{
      new_size = hash_value + chunk_size;
    }
    //we've completed a file
    if (new_size == work_node.st.st_size){
      hashtbl_remove(*chunk_hash, work_node.path);
      update_stats(work_node.path, get_output_path(base_path, work_node, dest_node, o), work_node.st);
      num_copied_files++;
    }
    else{
      hashtbl_insert(*chunk_hash, work_node.path, new_size);
    }
  }
  send_manager_copy_stats(num_copied_files, num_copied_bytes);
  free(workbuf);
  send_manager_work_done(rank);
}

void worker_output(int rank, int sending_rank, char *output_buffer, int *output_count, struct options o){
  //have a worker receive and print a single message
  MPI_Status status;
  
  char msg[MESSAGESIZE];

  //gather the message to print
  PRINT_MPI_DEBUG("rank %d: worker_output() Receiving the message from %d\n", rank, sending_rank);
  if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive msg\n");
  }

#ifdef THREADS_ONLY
  printf("%s", msg);
#else
  strncat(output_buffer, msg, MESSAGESIZE);
  (*output_count)++;
  if (*output_count >= MESSAGEBUFFER || !o.verbose){
    worker_flush_output(output_buffer, output_count);
  }
#endif
}

void worker_buffer_output(int rank, int sending_rank, char *output_buffer, int *output_count, struct options o){
  //have a worker receive and print a single message
  MPI_Status status;
  
  int message_count;
  char msg[MESSAGESIZE];
  char outmsg[MESSAGESIZE];

  char *buffer;
  int buffersize;
  int position;

  int i;

  //gather the message_count
  PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Receiving the message_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&message_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive message_count\n");
  }
    
  buffersize = MESSAGESIZE*message_count;
  buffer = (char *) malloc(buffersize * sizeof(char));
  
  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Receiving the buffer from %d\n", rank, sending_rank);
  if (MPI_Recv(buffer, buffersize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive buffer\n");
  }


  position = 0;
  for (i = 0; i < message_count; i++){
    PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Unpacking the message from %d\n", rank, sending_rank);
    MPI_Unpack(buffer, buffersize, &position, msg, MESSAGESIZE, MPI_CHAR, MPI_COMM_WORLD);
    snprintf(outmsg, MESSAGESIZE+10, "RANK %2d: %s", sending_rank, msg);
#ifdef THREADS_ONLY
    printf("%s", outmsg);
#else
    strncat(output_buffer, outmsg, MESSAGESIZE);
    (*output_count)++;
    if (*output_count >= MESSAGEBUFFER){
      worker_flush_output(output_buffer, output_count);
    }
#endif
    //printf("Rank %2d: %s", sending_rank, msg);
  }
  free(buffer);
}


void worker_flush_output(char *output_buffer, int *output_count){
  if (*output_count > 0){
    printf("%s", output_buffer);
    (*output_count) = 0;
    memset(output_buffer,'\0', sizeof(output_buffer));
  }
}

void worker_readdir(int rank, int sending_rank, const char *base_path, path_item dest_node, int makedir, struct options o){
  //When a worker is told to readdir, it comes here
  MPI_Status status;
  char *workbuf;
  int worksize;
  int position, out_position;
  int read_count;
  char path[PATHSIZE_PLUS], full_path[PATHSIZE_PLUS];
  char errmsg[MESSAGESIZE];
  char mkdir_path[PATHSIZE_PLUS];

  path_item work_node;
  path_item workbuffer[STATBUFFER];
  int buffer_count = 0;

  DIR *dip;
  struct dirent *dit;

  //filelist
  FILE *fp;

  int i;

  PRINT_MPI_DEBUG("rank %d: worker_readdir() Receiving the read_count %d\n", rank, sending_rank);
  if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive read_count\n");
  }
  worksize = read_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));

  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: worker_readdir() Receiving the workbuf %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive workbuf\n");
  }
  
  position = 0;
  out_position = 0;
  for (i = 0; i < read_count; i++){
    PRINT_MPI_DEBUG("rank %d: worker_readdir() Unpacking the work_node %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
   
    if (o.use_file_list == 0){
      if ((dip = opendir(work_node.path)) == NULL){
        snprintf(errmsg, MESSAGESIZE, "Failed to open dir %s\n", work_node.path);
        errsend(NONFATAL, errmsg);
        continue;
      }
      if (makedir == 1){
        strncpy(mkdir_path, get_output_path(base_path, work_node, dest_node, o), PATHSIZE_PLUS);
        mkdir(mkdir_path, S_IRWXU);
      }
      strncpy(path, work_node.path, PATHSIZE_PLUS);
      while ((dit = readdir(dip)) != NULL){
        if (strncmp(dit->d_name, ".", PATHSIZE_PLUS) != 0 && strncmp(dit->d_name, "..", PATHSIZE_PLUS) != 0){

          strncpy(full_path, path, PATHSIZE_PLUS);
          if (full_path[strnlen(full_path, PATHSIZE_PLUS) - 1 ] != '/'){
            strncat(full_path, "/", 1);
          }
          strncat(full_path, dit->d_name, PATHSIZE_PLUS);
          strncpy(work_node.path, full_path, PATHSIZE_PLUS);
          stat_item(&work_node, o);
          workbuffer[buffer_count] = work_node;
          buffer_count++;
        }
        
        if (buffer_count != 0 && buffer_count % STATBUFFER == 0){
          process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o);
        }
      }
      if (closedir(dip) == -1){
        snprintf(errmsg, MESSAGESIZE, "Failed to closedir: %s", work_node.path);
        errsend(1, errmsg);
      }
    }
    //we were provided a file list
    else{
      fp = fopen(work_node.path, "r");
      while (fgets(work_node.path, PATHSIZE_PLUS, fp) != NULL){
        if (work_node.path[strlen(work_node.path) - 1] == '\n'){
          work_node.path[strlen(work_node.path) - 1] = '\0';
        }
        workbuffer[buffer_count] = work_node;
        buffer_count++;

        if (buffer_count != 0 && buffer_count % STATBUFFER == 0){
          process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o);
        }

        while (request_input_queuesize() >= 10){
          sleep(1);
        }
      }

      fclose(fp);
    }
  }

  while(buffer_count != 0){
    process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o);
  }

  free(workbuf);
  send_manager_work_done(rank);
}

void stat_item(path_item *work_node, struct options o){
  //takes a work node, stats it and figures out some of its characteristics
  struct stat st;
  char errortext[MESSAGESIZE];
  //dmapi
#ifndef DISABLE_TAPE
  uid_t uid;
  int dmarray[3];
  char hexbuf[128];
#endif
  int numchars;
  char linkname[PATHSIZE_PLUS];
  char *link_result = NULL;

  if (lstat(work_node->path, &st) == -1) {
    snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", work_node->path);
    if (o.work_type == LSWORK){
      errsend(NONFATAL, errortext);
      return;
    }
    else{
      errsend(FATAL, errortext);
    }
  }

  work_node->st = st;
  work_node->ftype = REGULARFILE;

  //dmapi to find managed files
#ifndef DISABLE_TAPE
  if (!S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode) && o.sourcefs == GPFSFS){
    uid = getuid();
#ifndef THREADS_ONLY
    if (uid == 0 && st.st_size > 0 && st.st_blocks == 0){
#else
    if (0){
#endif
      dmarray[0] = 0;
      dmarray[1] = 0;
      dmarray[2] = 0;

      if (read_inodes (work_node->path, work_node->st.st_ino, work_node->st.st_ino+1, dmarray) != 0) {
        snprintf(errortext, MESSAGESIZE, "read_inodes failed: %s", work_node->path);
        errsend(FATAL, errortext);
      }

      else if (dmarray[0] > 0){
        dmapi_lookup(work_node->path, dmarray, hexbuf);
        if (dmarray[1] == 1) {                                                                                                                                                                                                                                                                             
          work_node->ftype = PREMIGRATEFILE;
        }
        else if (dmarray[2] == 1) {
          work_node->ftype = MIGRATEFILE;
        }
      }
    }
    else if (st.st_size > 0 && st.st_blocks == 0){
      work_node->ftype = MIGRATEFILE;
    }
  }
#endif

  //special cases for links
  if (S_ISLNK(work_node->st.st_mode)){
    memset(linkname,'\0', PATHSIZE_PLUS);
    numchars = readlink(work_node->path, linkname, PATHSIZE_PLUS);
    if (numchars < 0){
      snprintf(errortext, MESSAGESIZE, "Failed to read link %s", link_result);
      errsend(FATAL, errortext);
    }
    if (is_fuse_chunk(canonicalize_file_name(work_node->path))){
      if (lstat(linkname, &st) == -1) {
        snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", linkname);
        errsend(FATAL, errortext);
      }
      work_node->st = st;
      work_node->ftype = FUSEFILE;
    }
    else{
      //handle symlinks here
      work_node->ftype = LINKFILE;
    }
  }
}

void process_stat_buffer(path_item *path_buffer, int *stat_count, const char *base_path, path_item dest_node, struct options o){
  //When a worker is told to stat, it comes here

  int position, out_position;

  char *writebuf;
  int writesize;
  int write_count = 0, write_count_max = 100;
  int num_examined_files = 0;
  double num_examined_bytes = 0;
  
  int numchars;
  char linkname[PATHSIZE_PLUS];
  char errortext[MESSAGESIZE], statrecord[MESSAGESIZE];

  path_item work_node, out_node;
  int process = 1;
  
  
  //stat 
  struct stat st, out_st;
  struct tm sttm;
  char modebuf[15], timebuf[30];
  //for truncing an existing out file
  int trunc = 0;
  int rc;
  int i;

  //chunks
  //1 GB
  off_t chunk_size = 0;
  off_t chunk_size_save = 107374182400; //10GB

  off_t num_bytes_seen = 0;
  off_t ship_off = 2147483648; //2GB
  //int chunk_size = 1024;
  off_t chunk_curr_offset = 0;

  //classification
  path_item dirbuffer[DIRBUFFER], regbuffer[COPYBUFFER];
  int dir_buffer_count = 0, reg_buffer_count = 0;
  
#ifndef DISABLE_TAPE
  path_item tapebuffer[TAPEBUFFER];
  int tape_buffer_count = 0;
#endif


  //write_count = stat_count;
  writesize = MESSAGESIZE * write_count_max;
  writebuf = (char *) malloc(writesize * sizeof(char));


  position = 0;
  out_position = 0;
  for (i = 0; i < *stat_count; i++){
    work_node = path_buffer[i];
    st = work_node.st;

    if (st.st_ino == dest_node.st.st_ino){
      write_count--;
      continue;
    }
    else if (S_ISDIR(st.st_mode)){
      dirbuffer[dir_buffer_count] = work_node;
      dir_buffer_count++;
      if (dir_buffer_count % DIRBUFFER == 0){
        send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
      }
    }
#ifndef DISABLE_TAPE
    else if (work_node.ftype == MIGRATEFILE){
      tapebuffer[tape_buffer_count] = work_node;
      tape_buffer_count++;
      if (tape_buffer_count % TAPEBUFFER == 0){
        send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
      }
    }
#endif
    else{
      //do this for all regular files AND fuse+symylinks
      chunk_curr_offset = 0;
      if (o.work_type == COPYWORK){
        process = 1;
        strncpy(out_node.path, get_output_path(base_path, work_node, dest_node, o), PATHSIZE_PLUS);


        //if the out path exists
        if (lstat(out_node.path, &out_st) == 0){
          //Check if it's a valid match
          if (o.different == 1){
            //check size, mtime, mode, and owners 
            if (work_node.st.st_size == out_st.st_size &&
                (work_node.st.st_mtime == out_st.st_mtime  ||
                S_ISLNK(work_node.st.st_mode))&&
                work_node.st.st_mode == out_st.st_mode &&
                work_node.st.st_uid == out_st.st_uid &&
                work_node.st.st_gid == out_st.st_gid){
              process = 0;
            }
          }

          if (process == 1){
  
            out_node.st = out_st;
            if (S_ISLNK(out_st.st_mode)){
              memset(linkname,'\0', PATHSIZE_PLUS);
              numchars = readlink(out_node.path, linkname, PATHSIZE_PLUS);
              if (numchars < 0){
                snprintf(errortext, MESSAGESIZE, "Failed to read link %s", out_node.path);
                errsend(FATAL, errortext);
              }
              if (is_fuse_chunk(canonicalize_file_name(work_node.path)) == 1){
                //it's a fuse file trunc
                trunc = 1;
              }
              else{
                trunc = 0;
                //it's a regular symlink, unlink
                rc = unlink(out_node.path);
                if (rc < 0){
                  snprintf(errortext, MESSAGESIZE, "Failed to unlink %s", out_node.path);
                  errsend(FATAL, errortext);
                }
              }
            }
            else{
              //it's a regular file trunc
              trunc = 1;
            }
            //trunc
            if (trunc == 1){
              //trunc out file if it exists and is not a symlink
              rc = truncate(out_node.path, 0);
              if (rc != 0){
                sprintf(errortext, "Failed to truncate file %s", out_node.path);
                errsend(NONFATAL, errortext);
              }
            }
          }
        }
      }



      if (process == 1){
        //parallel filesystem can do n-to-1
        if (o.destfs == GPFSFS || o.destfs == PANASASFS){
          chunk_size = chunk_size_save;
          if (work_node.ftype == FUSEFILE){
            set_fuse_chunk_data(&work_node);
            chunk_size = work_node.length;
          }

          if (work_node.st.st_size == 0){
            work_node.offset = 0;
            work_node.length = 0;
            regbuffer[reg_buffer_count] = work_node;
            reg_buffer_count++;
          }

          while (chunk_curr_offset < work_node.st.st_size){
            work_node.offset = chunk_curr_offset;
            if ((chunk_curr_offset + chunk_size) >  work_node.st.st_size){
              work_node.length = work_node.st.st_size - chunk_curr_offset;
              chunk_curr_offset = work_node.st.st_size; 
            }
            else{
              work_node.length = chunk_size;
              chunk_curr_offset += chunk_size;
            }

            if (work_node.ftype == LINKFILE || (o.work_type == COMPAREWORK && o.meta_data_only)){
              //for links or metadata compare work just send the whole file
              work_node.offset = 0;
              work_node.length = work_node.st.st_size;
              chunk_curr_offset = work_node.st.st_size;
            }
            if (o.work_type != LSWORK){
              regbuffer[reg_buffer_count] = work_node;
              reg_buffer_count++;
              num_bytes_seen += work_node.length;
              if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off){
                send_manager_regs_buffer(regbuffer, &reg_buffer_count);
                num_bytes_seen = 0;
              }      
            }
          }
        }
        //regular filesystem
        else{
          work_node.offset = 0;
          chunk_size = work_node.st.st_size;
          work_node.length = chunk_size;
          
          if (o.work_type != LSWORK){
            regbuffer[reg_buffer_count] = work_node;
            reg_buffer_count++;
            num_bytes_seen += work_node.length;
            if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off){
              send_manager_regs_buffer(regbuffer, &reg_buffer_count);
            } 
          }
        }
      }
    }
    if (! S_ISDIR(st.st_mode)){
      num_examined_files++;
      num_examined_bytes += st.st_size;
    }

    printmode(st.st_mode, modebuf);
    memcpy(&sttm, localtime(&st.st_mtime), sizeof(sttm));
    strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);

    //if (st.st_size > 0 && st.st_blocks == 0){                                                                                                                                                                                                                                                          
    if (o.verbose){
      if (work_node.ftype == MIGRATEFILE){
        sprintf(statrecord, "INFO  DATASTAT M %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      else if (work_node.ftype == PREMIGRATEFILE){
        sprintf(statrecord, "INFO  DATASTAT P %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      else{
        sprintf(statrecord, "INFO  DATASTAT - %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
      write_count++;
      if (write_count % write_count_max == 0){
        write_buffer_output(writebuf, writesize, write_count);
        out_position = 0;
        write_count = 0;
      }
    }
  } 

  //incase we tried to copy a file into itself
  if (o.verbose){
    writesize = MESSAGESIZE * write_count;
    writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
    write_buffer_output(writebuf, writesize, write_count);
  }
  
  while(dir_buffer_count != 0){
    send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
  }
  while (reg_buffer_count != 0){
    send_manager_regs_buffer(regbuffer, &reg_buffer_count);
  } 
#ifndef DISABLE_TAPE
  while (tape_buffer_count != 0){
    send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
  } 
#endif


  send_manager_examined_stats(num_examined_files, num_examined_bytes);
  //free malloc buffers
  free(writebuf);
  *stat_count = 0;
}


void worker_stat(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o){
  //When a worker is told to stat, it comes here
  MPI_Status status;

  char *workbuf;
  int worksize;
  int position, out_position;
  int stat_count;

  char *writebuf;
  int writesize;
  int write_count = 0;
  int write_count_max = 100;
  
  char *link_result = NULL;
  char linkname[PATHSIZE_PLUS];
  char errortext[MESSAGESIZE], statrecord[MESSAGESIZE];
  int numchars;

  path_item work_node, out_node;
  int process = 1;
  
  //dmapi
#ifndef DISABLE_TAPE
  uid_t uid;
  int dmarray[3];
  char hexbuf[128];
#endif
  
  //stat 
  struct stat st, out_st;
  struct tm sttm;
  char modebuf[15], timebuf[30];
  //for truncing an existing out file
  int trunc = 0;
  int num_examined_files = 0;
  double num_examined_bytes = 0;
  int rc;
  int i;

  //chunks
  //1 GB
  off_t chunk_size = 0;
  off_t chunk_size_save = 107374182400; //10GB

  off_t num_bytes_seen = 0;
  off_t ship_off = 2147483648; //2GB
  //int chunk_size = 1024;
  off_t chunk_curr_offset = 0;

  //classification
  path_item dirbuffer[DIRBUFFER], regbuffer[COPYBUFFER];
  int dir_buffer_count = 0, reg_buffer_count = 0;
  
#ifndef DISABLE_TAPE
  path_item tapebuffer[TAPEBUFFER];
  int tape_buffer_count = 0;
#endif



  PRINT_MPI_DEBUG("rank %d: worker_stat() Receiving the stat_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&stat_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive stat_count\n");
  }
  worksize = sizeof(path_item) * stat_count;
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  //write_count = stat_count;
  writesize = MESSAGESIZE * write_count_max;
  writebuf = (char *) malloc(writesize * sizeof(char));

  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: worker_stat() Receiving the workbuf from %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive workbuf\n");
  }
  
  position = 0;
  out_position = 0;
  for (i = 0; i < stat_count; i++){
    PRINT_MPI_DEBUG("rank %d: worker_stat() Unpacking the work_node %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
   

    if (lstat(work_node.path, &st) == -1) {
      snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", work_node.path);
      if (o.work_type == LSWORK){
        errsend(NONFATAL, errortext);
        continue;
      }
      else{
        errsend(FATAL, errortext);
      }
    }

    work_node.st = st;
    work_node.ftype = REGULARFILE;

    //dmapi to find managed files
#ifndef DISABLE_TAPE
    if (!S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode) && o.sourcefs == GPFSFS){
      uid = getuid();
      if (uid == 0 && st.st_size > 0 && st.st_blocks == 0){
        dmarray[0] = 0;
        dmarray[1] = 0;
        dmarray[2] = 0;

        if (read_inodes (work_node.path, work_node.st.st_ino, work_node.st.st_ino+1, dmarray) != 0) {
          snprintf(errortext, MESSAGESIZE, "read_inodes failed: %s", work_node.path);
          errsend(FATAL, errortext);
        }
  
        else if (dmarray[0] > 0){
          dmapi_lookup(work_node.path, dmarray, hexbuf);
          if (dmarray[1] == 1) {                                                                                                                                                                                                                                                                             
            work_node.ftype = PREMIGRATEFILE;
          }
          else if (dmarray[2] == 1) {
            work_node.ftype = MIGRATEFILE;
          }
        }
      }
      else if (st.st_size > 0 && st.st_blocks == 0){
        work_node.ftype = MIGRATEFILE;
      }
    }
#endif


    
    if (st.st_ino == dest_node.st.st_ino){
      write_count--;
      continue;
    }
    else if (S_ISDIR(st.st_mode)){
      dirbuffer[dir_buffer_count] = work_node;
      dir_buffer_count++;
      if (dir_buffer_count % 15 == 0){
        send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
      }
    }
#ifndef DISABLE_TAPE
    else if (work_node.ftype == MIGRATEFILE){
      tapebuffer[tape_buffer_count] = work_node;
      tape_buffer_count++;
      if (tape_buffer_count % TAPEBUFFER == 0){
        send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
      }
    }
#endif
    else{
      if (S_ISLNK(work_node.st.st_mode)){
        memset(linkname,'\0', PATHSIZE_PLUS);
        numchars = readlink(work_node.path, linkname, PATHSIZE_PLUS);
        if (numchars < 0){
          snprintf(errortext, MESSAGESIZE, "Failed to read link %s", link_result);
          errsend(FATAL, errortext);
        }
        if (is_fuse_chunk(canonicalize_file_name(work_node.path))){
          if (lstat(linkname, &st) == -1) {
            snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", linkname);
            errsend(FATAL, errortext);
          }
          work_node.st = st;
          work_node.ftype = FUSEFILE;
        }
        else{
          //handle symlinks here
          work_node.ftype = LINKFILE;
        }
      }
      //do this for all regular files AND fuse+symylinks
      chunk_curr_offset = 0;
      if (o.work_type == COPYWORK){
        process = 1;
        strncpy(out_node.path, get_output_path(base_path, work_node, dest_node, o), PATHSIZE_PLUS);


        //if the out path exists
        if (lstat(out_node.path, &out_st) == 0){
          //Check if it's a valid match
          if (o.different == 1){
            //check size, mtime, mode, and owners 
            if (work_node.st.st_size == out_st.st_size &&
                (work_node.st.st_mtime == out_st.st_mtime  ||
                S_ISLNK(work_node.st.st_mode))&&
                work_node.st.st_mode == out_st.st_mode &&
                work_node.st.st_uid == out_st.st_uid &&
                work_node.st.st_gid == out_st.st_gid){
              process = 0;
            }
          }

          if (process == 1){
  
            out_node.st = out_st;
            if (S_ISLNK(out_st.st_mode)){
              memset(linkname,'\0', PATHSIZE_PLUS);
              numchars = readlink(out_node.path, linkname, PATHSIZE_PLUS);
              if (numchars < 0){
                snprintf(errortext, MESSAGESIZE, "Failed to read link %s", out_node.path);
                errsend(FATAL, errortext);
              }
              if (is_fuse_chunk(canonicalize_file_name(work_node.path)) == 1){
                //it's a fuse file trunc
                trunc = 1;
              }
              else{
                trunc = 0;
                //it's a regular symlink, unlink
                rc = unlink(out_node.path);
                if (rc < 0){
                  snprintf(errortext, MESSAGESIZE, "Failed to unlink %s", out_node.path);
                  errsend(FATAL, errortext);
                }
              }
            }
            else{
              //it's a regular file trunc
              trunc = 1;
            }
            //trunc
            if (trunc == 1){
              //trunc out file if it exists and is not a symlink
              rc = truncate(out_node.path, 0);
              if (rc != 0){
                sprintf(errortext, "Failed to truncate file %s", out_node.path);
                errsend(NONFATAL, errortext);
              }
            }
          }
        }
      }



      if (process == 1){
        //parallel filesystem can do n-to-1
        if (o.destfs == GPFSFS || o.destfs == PANASASFS){
          chunk_size = chunk_size_save;
          if (work_node.ftype == FUSEFILE){
            set_fuse_chunk_data(&work_node);
            chunk_size = work_node.length;
          }

          if (work_node.st.st_size == 0){
            work_node.offset = 0;
            work_node.length = 0;
            regbuffer[reg_buffer_count] = work_node;
            reg_buffer_count++;
          }

          while (chunk_curr_offset < work_node.st.st_size){
            work_node.offset = chunk_curr_offset;
            if ((chunk_curr_offset + chunk_size) >  work_node.st.st_size){
              work_node.length = work_node.st.st_size - chunk_curr_offset;
              chunk_curr_offset = work_node.st.st_size; 
            }
            else{
              work_node.length = chunk_size;
              chunk_curr_offset += chunk_size;
            }

            if (work_node.ftype == LINKFILE || (o.work_type == COMPAREWORK && o.meta_data_only)){
              //for links or metadata compare work just send the whole file
              work_node.offset = 0;
              work_node.length = work_node.st.st_size;
              chunk_curr_offset = work_node.st.st_size;
            }
            regbuffer[reg_buffer_count] = work_node;
            reg_buffer_count++;
            num_bytes_seen += work_node.length;
            if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off){
              send_manager_regs_buffer(regbuffer, &reg_buffer_count);
              num_bytes_seen = 0;
            }      
          }
        }
        //regular filesystem
        else{
          work_node.offset = 0;
          chunk_size = work_node.st.st_size;
          work_node.length = chunk_size;
          
          regbuffer[reg_buffer_count] = work_node;
          reg_buffer_count++;
          num_bytes_seen += work_node.length;
          if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off){
            send_manager_regs_buffer(regbuffer, &reg_buffer_count);
          } 
        }
      }
    }
    if (! S_ISDIR(st.st_mode)){
      num_examined_files++;
      num_examined_bytes += st.st_size;
    }

    printmode(st.st_mode, modebuf);
    memcpy(&sttm, localtime(&st.st_mtime), sizeof(sttm));
    strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);

    //if (st.st_size > 0 && st.st_blocks == 0){                                                                                                                                                                                                                                                          
    if (o.verbose){
      if (work_node.ftype == MIGRATEFILE){
        sprintf(statrecord, "INFO  DATASTAT M %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      else if (work_node.ftype == PREMIGRATEFILE){
        sprintf(statrecord, "INFO  DATASTAT P %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      else{
        sprintf(statrecord, "INFO  DATASTAT - %s %6lu %6d %6d %21zd %s %s\n", modebuf, st.st_blocks, st.st_uid, st.st_gid, st.st_size, timebuf, work_node.path);
      }
      MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
      write_count++;
      if (write_count % write_count_max == 0){
        write_buffer_output(writebuf, writesize, write_count);
        out_position = 0;
        write_count = 0;
      }
    }
  } 

  //incase we tried to copy a file into itself
  if (o.verbose){
    writesize = MESSAGESIZE * write_count;
    writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
    write_buffer_output(writebuf, writesize, write_count);
  }
  
  while(dir_buffer_count != 0){
    send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
  }
  while (reg_buffer_count != 0){
    send_manager_regs_buffer(regbuffer, &reg_buffer_count);
  } 
#ifndef DISABLE_TAPE
  while (tape_buffer_count != 0){
    send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
  } 
#endif
  send_manager_examined_stats(num_examined_files, num_examined_bytes);


  //free malloc buffers
  free(workbuf);
  free(writebuf);
  send_manager_work_done(rank);  
}


void worker_copylist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o){
  //When a worker is told to copy, it comes here
  MPI_Status status;

  char *workbuf, *writebuf;
  int worksize, writesize;
  int position, out_position;
  int read_count;
  
  path_item work_node;
  char path[PATHSIZE_PLUS], out_path[PATHSIZE_PLUS];
  char copymsg[MESSAGESIZE];
  off_t offset, length;
  int num_copied_files = 0;
  double num_copied_bytes = 0;

  path_item chunks_copied[CHUNKBUFFER];
  int buffer_count = 0;
  
  int i, rc;

  PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive read_count\n");
  }
  worksize = read_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));

  writesize = MESSAGESIZE * read_count;
  writebuf = (char *) malloc(writesize * sizeof(char));  

  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the workbuf from %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive workbuf\n");
  }
  
  position = 0;
  out_position = 0;
  for (i = 0; i < read_count; i++){
    PRINT_MPI_DEBUG("rank %d: worker_copylist() unpacking work_node from %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node.path, PATHSIZE_PLUS);
    strncpy(out_path, get_output_path(base_path, work_node, dest_node, o), PATHSIZE_PLUS);
    //sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", slavecopy.req, (long long) slavecopy.offset, (long long) slavecopy.length, copyoutpath)
    offset = work_node.offset;
    length = work_node.length;
    rc = copy_file(path, out_path, offset, length, work_node.st);
    if (rc >= 0){
      if (S_ISLNK(work_node.st.st_mode)){
        sprintf(copymsg, "INFO  DATACOPY Created symlink %s from %s\n", out_path, path);
      }
      else{
        sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", path, (long long)offset, (long long)length, out_path);
      }
      MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
      //file is not 'chunked'
      if (offset == 0 && length == work_node.st.st_size){ 
        num_copied_files +=1;
        if (!S_ISLNK(work_node.st.st_mode)){
          num_copied_bytes += length;
        }
      }
      else{
        chunks_copied[buffer_count] = work_node;
        buffer_count++;
      }
    }
  }
  
  write_buffer_output(writebuf, writesize, read_count); 

  //update the chunk information
  if (buffer_count > 0){
    send_manager_chunk_busy();
    update_chunk(chunks_copied, &buffer_count);
  }
  //for all non-chunked files
  if (num_copied_files > 0 || num_copied_bytes > 0){
    send_manager_copy_stats(num_copied_files, num_copied_bytes);
  }
  send_manager_work_done(rank);

  free(workbuf);
  free(writebuf);
}

void worker_comparelist(int rank, int sending_rank, const char *base_path, path_item dest_node, struct options o){
  //When a worker is told to copy, it comes here
  MPI_Status status;

  char *workbuf, *writebuf;
  int worksize, writesize;
  int position, out_position;
  int read_count;
  
  path_item work_node;
  char path[PATHSIZE_PLUS], out_path[PATHSIZE_PLUS];
  char copymsg[MESSAGESIZE];
  off_t offset, length;
  int num_compared_files = 0;
  double num_compared_bytes = 0;

  path_item chunks_copied[CHUNKBUFFER];
  int buffer_count = 0;
  
  int i, rc;

  PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive read_count\n");
  }
  worksize = read_count * sizeof(path_list);
  workbuf = (char *) malloc(worksize * sizeof(char));

  writesize = MESSAGESIZE * read_count;
  writebuf = (char *) malloc(writesize * sizeof(char));  

  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the workbuf from %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive workbuf\n");
  }
  
  position = 0;
  out_position = 0;
  for (i = 0; i < read_count; i++){
    PRINT_MPI_DEBUG("rank %d: worker_copylist() unpacking work_node from %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node.path, PATHSIZE_PLUS);
    strncpy(out_path, get_output_path(base_path, work_node, dest_node, o), PATHSIZE_PLUS);

    //sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", slavecopy.req, (long long) slavecopy.offset, (long long) slavecopy.length, copyoutpath)
    offset = work_node.offset;
    length = work_node.length;
    rc = compare_file(path, out_path, offset, length, work_node.st, o.meta_data_only);
    if (o.meta_data_only || work_node.ftype == LINKFILE){
      sprintf(copymsg, "INFO  DATACOMPARE compared %s to %s", path, out_path);
    }
    else{
      sprintf(copymsg, "INFO  DATACOMPARE compared %s offs %lld len %lld to %s", path, (long long)offset, (long long)length, out_path);
    }
    if (rc == 0){
      strncat(copymsg, "-- SUCCESS\n", MESSAGESIZE);
    }
    else if (rc == 2 ){
      strncat(copymsg, "-- MISSING DESTINATION\n", MESSAGESIZE);
    }
    else{
      strncat(copymsg, "-- MISMATCH\n", MESSAGESIZE);
    }
    MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
    //file is not 'chunked'
    if (offset == 0 && length == work_node.st.st_size){ 
      num_compared_files +=1;
      num_compared_bytes += length;
    }
    else{
      chunks_copied[buffer_count] = work_node;
      buffer_count++;
    }
    
  }
  
  write_buffer_output(writebuf, writesize, read_count); 

  //update the chunk information
  if (buffer_count > 0){
    send_manager_chunk_busy();
    update_chunk(chunks_copied, &buffer_count);
  }
  //for all non-chunked files
  if (num_compared_files > 0 || num_compared_bytes > 0){
    send_manager_copy_stats(num_compared_files, num_compared_bytes);
  }
  send_manager_work_done(rank);

  free(workbuf);
  free(writebuf);
}

