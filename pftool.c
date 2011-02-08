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

//External Declarations
extern void usage();
extern char *printmode (mode_t aflag, char *buf);
extern char *get_base_path(const char *path, int wildcard);
extern void get_dest_path(const char *beginning_path, const char *dest_path, path_node **dest_node, int recurse);
extern char *get_output_path(const char *base_path, path_node *src_node, path_node *dest_node, int recurse);
extern int copy_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st);

//manager
extern void send_manager_nonfatal_inc();
extern void send_manager_regs(int num_send, path_node **reg_list_head, path_node **reg_list_tail, int *reg_list_count);
extern void send_manager_dirs(int num_send, path_node **dir_list_head, path_node **dir_list_tail, int *dir_list_count);
extern void send_manager_tape(int num_send, path_node **tape_list_head, path_node **tape_list_tail, int *tape_list_count);
extern void send_manager_new_input(int num_send, path_node **new_input_list_head, path_node **new_input_list_tail, int *new_input_list_count);
extern void send_manager_work_done();

//worker
extern void write_output(char *message);
extern void write_buffer_output(char *buffer, int buffer_size, int buffer_count);
extern void send_worker_stat_path(int target_rank, int num_send, path_node **input_queue_head, path_node **input_queue_tail, int *input_queue_count);
extern void send_worker_readdir(int target_rank, int num_send, path_node **dir_work_queue_head, path_node **dir_work_queue_tail, int *dir_work_queue_count);
extern void send_worker_copy_path(int target_rank, int num_send, path_node **work_queue_head, path_node **work_queue_tail, int *work_queue_count);
extern void send_worker_exit();

//functions that use workers
extern void errsend(int fatal, char *error_text);
extern int get_free_rank(int *proc_status, int start_range, int end_range);
extern int processing_complete(int *proc_status, int nproc);

//queues 
extern void enqueue_path(path_node **head, path_node **tail, char *path, int *count);
extern void print_queue_path(path_node *head);
extern void delete_queue_path(path_node **head, int *count);
extern void enqueue_node(path_node **head, path_node **tail, path_node *new_node, int *count);
extern void dequeue_node(path_node **head, path_node **tail, int *count);

int main(int argc, char *argv[]){
  //general variables
  int i;

  //mpi
  int rank, nproc;

  //getopt
  int c;
  struct options o;

  //queues
  path_node *input_queue_head = NULL, *input_queue_tail = NULL;
  int input_queue_count = 0;
  
  //paths
  char src_path[PATHSIZE_PLUS], dest_path[PATHSIZE_PLUS];
  struct stat dest_stat;
  int statrc;

  //initialize options
  o.recurse = 0;
  //o.work_type = LSWORK;
  o.work_type = COPYWORK;
  strncpy(o.jid, "TestJob", 128);

  //Process using getopt
  while ((c = getopt(argc, argv, "p:c:j:rh")) != -1) 
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
      case 'r':
        //Recurse
        o.recurse = 1;
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

  // start MPI - if this fails we cant send the error to the output proc so we just die now 
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

  //Modifies the path based on recursion/wildcards
  if (rank == MANAGER_PROC){
    //wildcard
    if (optind < argc && o.work_type == COPYWORK){
      statrc = lstat(dest_path, &dest_stat);
      if (statrc < 0 || !S_ISDIR(dest_stat.st_mode)){
        printf("Multiple inputs and target '%s' is not a directory\n", dest_path);
        MPI_Abort(MPI_COMM_WORLD, -1);
        return -1;
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
  else if (rank == MANAGER_PROC){
    enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
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


void manager(int rank, struct options o, int nproc, path_node *input_queue_head, path_node *input_queue_tail, int input_queue_count, const char *dest_path){
  MPI_Status status;
  int all_done = 0, message_ready = 0, probecount = 0;
  int prc, type_cmd;
  int work_rank, sending_rank;

  int i;
  int *proc_status;
  struct timeval in, out;
  int temp_count = 0, non_fatal = 0, reg_count = 0, dir_count = 0, tape_count = 0;

  char message[MESSAGESIZE];
  char beginning_path[PATHSIZE_PLUS], base_path[PATHSIZE_PLUS];
  path_node *dest_node = malloc(sizeof(path_node));

  path_node *iter = NULL;
  path_node *work_queue_head = NULL, *work_queue_tail = NULL;
  path_node *dir_work_queue_head = NULL, *dir_work_queue_tail = NULL;
  path_node *tape_work_queue_head = NULL, *tape_work_queue_tail = NULL;
  int work_queue_count = 0, dir_work_queue_count = 0, tape_work_queue_count = 0, num_copied_files = 0, num_copied_bytes = 0;
    
  int mpi_ret_code;


  //path stuff
  int wildcard = 0;
  if (input_queue_count > 1){
    wildcard = 1;
  }
  
  //setup paths
  strncpy(beginning_path, input_queue_head->data.path, PATHSIZE_PLUS);
  strncpy(base_path, get_base_path(beginning_path, wildcard), PATHSIZE_PLUS);
  get_dest_path(beginning_path, dest_path, &dest_node, o.recurse);
  
  //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the dest_path: %s\n", rank, dest_path);
  mpi_ret_code = MPI_Bcast(dest_node, sizeof(path_node), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
  if (mpi_ret_code < 0){
    errsend(FATAL, "Failed to Bcast dest_path");
  }
  //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the base_path: %s\n", rank, base_path);
  mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
  if (mpi_ret_code < 0){
    errsend(FATAL, "Failed to Bcast base_path");
  }
  //printf("==> %s -- %s\n",beginning_path, base_path);

  iter = input_queue_head;
  if (strncmp(base_path, ".", PATHSIZE_PLUS) != 0 && o.recurse == 1){
    while (iter != NULL){
      if (strstr(iter->data.path, base_path) == NULL){
        errsend(FATAL, "All source paths must be derived from the same base path for a recursive copy.");
      }
      iter = iter->next;
    }
  }
  

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

  while (all_done == 0){
    //poll for message
    while ( message_ready == 0){ 
      prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
      if (prc != MPI_SUCCESS) {
        errsend(FATAL, "MPI_Iprobe failed\n");
        message_ready = -1; 
      }   
      else{
        probecount++;
      }   

      if  (probecount % 30000 == 0){ 
        PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
      }   
      usleep(10);
      //we didn't get any new messages from workers
      if (message_ready == 0){
        
        for (i = 0; i < nproc; i++){
          PRINT_PROC_DEBUG("Rank %d, Status %d\n", i, proc_status[i]);
        }
        PRINT_PROC_DEBUG("=============\n");

        work_rank = get_free_rank(proc_status, 2, 2);
        if (work_rank > -1 && dir_work_queue_count !=0 && o.recurse){
          proc_status[work_rank] = 1;
          send_worker_readdir(work_rank, 100, &dir_work_queue_head, &dir_work_queue_tail, &dir_work_queue_count);
        }
        else if (!o.recurse){
          delete_queue_path(&dir_work_queue_head, &dir_work_queue_count);
        }
       

        for(i = 0; i < (input_queue_count / 200) + 1; i++){
          //work_rank = get_free_rank(proc_status, 3, 13);
          work_rank = get_free_rank(proc_status, 3, 13);

          //first run through the remaining stat_queue
          if (work_rank > -1 && input_queue_count != 0){
            proc_status[work_rank] = 1;
            send_worker_stat_path(work_rank, 200, &input_queue_head, &input_queue_tail, &input_queue_count);
          }
        }

        if (o.work_type == COPYWORK){
          work_rank = get_free_rank(proc_status, 14, 15);
          if (work_rank > -1 && work_queue_count > 0){
            proc_status[work_rank] = 1;
            send_worker_copy_path(work_rank, 200, &work_queue_head, &work_queue_tail, &work_queue_count);
          }
        }
        else{
          delete_queue_path(&work_queue_head, &work_queue_count);
        }

      }
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
      case COPYSTATSCMD:
        manager_add_copy_stats(rank, sending_rank, &num_copied_files, &num_copied_bytes);
        break;
      case REGULARCMD:
        reg_count += manager_add_paths(rank, sending_rank, &work_queue_head, &work_queue_tail, &work_queue_count);
        iter = work_queue_head;
        break;
      case DIRCMD:
        dir_count += manager_add_paths(rank, sending_rank, &dir_work_queue_head, &dir_work_queue_tail, &dir_work_queue_count);
        break;
      case TAPECMD:
        temp_count = manager_add_paths(rank, sending_rank, &tape_work_queue_head, &tape_work_queue_tail, &tape_work_queue_count);
        reg_count += temp_count;
        tape_count += temp_count;
        delete_queue_path(&tape_work_queue_head, &tape_work_queue_count);
        break;
      case INPUTCMD:
        manager_add_paths(rank, sending_rank, &input_queue_head, &input_queue_tail, &input_queue_count);
        break;
      default:
        break;
    }
    message_ready = 0;
    
    //are we finished?
    if (input_queue_count == 0 && dir_work_queue_count == 0 && processing_complete(proc_status, nproc) == 0){
      all_done = 1;
    }
    
  }
  gettimeofday(&out, NULL);
  int elapsed_time = out.tv_sec - in.tv_sec;
  //Manager is done, cleaning have the other ranks exit
  sprintf(message, "INFO  FOOTER   ========================   NONFATAL ERRORS = %d   ================================\n", non_fatal);
  write_output(message);
  sprintf(message, "INFO  FOOTER   =================================================================================\n");
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Files/Links Examined: %d\n", reg_count);
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Files on Tape: %d\n", tape_count);
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Dirs Examined: %d\n", dir_count);
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Files Copied: %d\n", num_copied_files);
  write_output(message);
  sprintf(message, "INFO  FOOTER   Total Bytes Copied: %d\n", num_copied_bytes);
  write_output(message);
  if (elapsed_time == 1){
    sprintf(message, "INFO  FOOTER   Elapsed Time: %d second\n", elapsed_time);
  }
  else{
    sprintf(message, "INFO  FOOTER   Elapsed Time: %d seconds\n", elapsed_time);
  }
  write_output(message);
  sprintf(message, "INFO  FOOTER   Data Rate: %d MB/second\n", (num_copied_bytes/(1024*1024))/(elapsed_time+1));
  write_output(message);
  for (i = 1; i < nproc; i++){
    send_worker_exit(i);
  }

  //free any allocated stuff
  free(proc_status);
  free(dest_node);
  
}

int manager_add_paths(int rank, int sending_rank, path_node **queue_head, path_node **queue_tail, int *queue_count){
  MPI_Status status;
  int path_count;

  path_node *work_node = malloc(sizeof(path_node));
  char path[PATHSIZE_PLUS];
  char *workbuf;
  int worksize, position;
  
  int i;  

  //gather the # of files
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive path_count\n");
  }
  worksize =  path_count * sizeof(path_node);
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  //gather the path to stat
  PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }
  
  position = 0;
  for (i = 0; i < path_count; i++){
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Unpacking the work_node from rank %d\n", rank, sending_rank);
    MPI_Unpack(workbuf, worksize, &position, work_node, sizeof(path_node), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node->data.path, PATHSIZE_PLUS); 
    enqueue_node(queue_head, queue_tail, work_node, queue_count);
  }
  free(work_node);
  free(workbuf);

  return path_count;

}

void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, int *num_copied_bytes){
  MPI_Status status;
  int num_files, num_bytes;
  //gather the # of copied files
  PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_files from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_files, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }
  
  //gather the # of copied byes
  PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_files from rank %d\n", rank, sending_rank);
  if (MPI_Recv(&num_bytes, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      errsend(FATAL, "Failed to receive worksize\n");
  }

  *num_copied_files += num_files;
  *num_copied_bytes += num_bytes;

}

void manager_workdone(int rank, int sending_rank, int *proc_status){
  proc_status[sending_rank] = 0;
}

void worker(int rank, struct options o){
  MPI_Status status;
  int sending_rank;
  int all_done = 0, message_ready = 0, probecount = 0;
  int prc;
  

  int type_cmd;
  int mpi_ret_code;
  char base_path[PATHSIZE_PLUS];
  path_node *dest_node = malloc(sizeof(path_node));



  //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the dest_path\n", rank);
  mpi_ret_code = MPI_Bcast(dest_node, sizeof(path_node), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
  if (mpi_ret_code < 0){
    errsend(FATAL, "Failed to Receive Bcast dest_path");
  }
  //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the base_path\n", rank);
  mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
  if (mpi_ret_code < 0){
    errsend(FATAL, "Failed to Receive Bcast base_path");
  }

  //change this to get request first, process, then get work    
  while ( all_done == 0){
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
      usleep(10);
    }

    //grab message type
    PRINT_MPI_DEBUG("rank %d: worker() receiving the type_cmd\n", rank);
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive type_cmd\n");
    }
    sending_rank = status.MPI_SOURCE;

    //do operations based on the message
    switch(type_cmd){
      case OUTCMD:
        worker_output(rank, sending_rank);
        break;
      case BUFFEROUTCMD:
        worker_buffer_output(rank, sending_rank);
        break;
      case EXITCMD:
        all_done = 1;
        break;
      case NAMECMD:
        worker_stat(rank, sending_rank, dest_node);
        break;
      case DIRCMD:
        worker_readdir(rank, sending_rank, base_path, dest_node, o.recurse);
        break;
      case COPYCMD:
        worker_copylist(rank, sending_rank, base_path, dest_node, o.recurse);
        break;
      default:
        break;
    }
    message_ready = 0;
    //process message
  }
  free(dest_node);
}

void worker_output(int rank, int sending_rank){
  //have a worker receive and print a single message
  MPI_Status status;
  
  char msg[MESSAGESIZE];

  //gather the message to print
  PRINT_MPI_DEBUG("rank %d: worker_output() Receiving the message from %d\n", rank, sending_rank);
  if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive msg\n");
  }
  printf("Rank %d: %s", sending_rank, msg);
  
}

void worker_buffer_output(int rank, int sending_rank){
  //have a worker receive and print a single message
  MPI_Status status;
  
  int message_count;
  char msg[MESSAGESIZE];

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
    printf("Rank %d: %s", sending_rank, msg);
  }
  free(buffer);
}

void worker_stat(int rank, int sending_rank, path_node *dest_node){
  //When a worker is told to stat, it comes here
  MPI_Status status;

  char *workbuf, *writebuf;
  int worksize, writesize;
  int position, out_position;
  int stat_count, write_count;
  path_node *work_node = malloc(sizeof(path_node));
  char path[PATHSIZE_PLUS];
  char errortext[MESSAGESIZE], statrecord[MESSAGESIZE];

  struct stat st;
  struct statfs stfs;
  struct tm sttm;
  int sourcefs;
  char sourcefsc[5], modebuf[15], timebuf[30];
  int i;

  //classification
  path_node *dir_list_head = NULL, *dir_list_tail = NULL;
  path_node *reg_list_head = NULL, *reg_list_tail = NULL; 
  path_node *tape_list_head = NULL, *tape_list_tail = NULL; 
  
  int dir_list_count = 0, reg_list_count = 0, tape_list_count = 0;

  PRINT_MPI_DEBUG("rank %d: worker_stat() Receiving the stat_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&stat_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive stat_count\n");
  }
  worksize = sizeof(path_node) * stat_count;
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  write_count = stat_count;
  writesize = MESSAGESIZE * write_count;
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
    MPI_Unpack(workbuf, worksize, &position, work_node, sizeof(path_node), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node->data.path, PATHSIZE_PLUS);
   

    if (lstat(path, &st) == -1) {
      snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", path);
      errsend(FATAL, errortext);
    }

    work_node->data.st = st;
    printmode(st.st_mode, modebuf);
    memcpy(&sttm, localtime(&st.st_mtime), sizeof(sttm));
    strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);

    if (st.st_ino == dest_node->data.st.st_ino){
      write_count--;
      continue;
    }
    else if (S_ISDIR(st.st_mode)){
      enqueue_node(&dir_list_head, &dir_list_tail, work_node, &dir_list_count);
    }
    else if (st.st_size > 0 && st.st_blocks == 0){                                                                                                                                                                                                                                                          
      enqueue_node(&tape_list_head, &tape_list_tail, work_node, &tape_list_count);
    }
    else{
      enqueue_node(&reg_list_head, &reg_list_tail, work_node, &reg_list_count);
    }

    if (!S_ISLNK(st.st_mode)){
      if (statfs(path, &stfs) < 0) { 
        snprintf(errortext, MESSAGESIZE, "Failed to statfs path %s", path);
        errsend(FATAL, errortext);
      } 

      if (stfs.f_type == GPFS_SUPER_MAGIC) {
        sourcefs = GPFSFS;
        sprintf(sourcefsc, "G");
      }
      else if (stfs.f_type == PAN_FS_CLIENT_MAGIC) {
        sourcefs = PANASASFS;
        sprintf(sourcefsc, "P");
      }
      else{
        sourcefs = ANYFS;
        sprintf(sourcefsc, "A");
      }
  }
  else{
    //symlink
    sourcefs = GPFSFS;
    sprintf(sourcefsc, "G");
  }

    if (st.st_size > 0 && st.st_blocks == 0){                                                                                                                                                                                                                                                          
      sprintf(statrecord, "INFO  DATASTAT %sM %s %6lu %6d %6d %21lld %s %s\n", sourcefsc, modebuf, st.st_blocks, st.st_uid, st.st_gid, (long long) st.st_size, timebuf, path);
    }
    else{
      sprintf(statrecord, "INFO  DATASTAT %s- %s %6lu %6d %6d %21lld %s %s\n", sourcefsc, modebuf, st.st_blocks, st.st_uid, st.st_gid, (long long) st.st_size, timebuf, path);
    }
    MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
    //write_output(rank, statrecord);
    
  } 

  //incase we tried to copy a file into itself
  if (write_count != stat_count){
    writesize = MESSAGESIZE * write_count;
    writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
  }
  write_buffer_output(writebuf, writesize, write_count);

  
  while(dir_list_count != 0){
    send_manager_dirs(200, &dir_list_head, &dir_list_tail, &dir_list_count);
  }
  while (reg_list_count != 0){
    send_manager_regs(200, &reg_list_head, &reg_list_tail, &reg_list_count);
  } 

  while (tape_list_count != 0){
    send_manager_tape(200, &tape_list_head, &tape_list_tail, &tape_list_count);
  } 

  //free malloc buffers
  free(work_node);
  free(workbuf);
  free(writebuf);
  send_manager_work_done(rank);  
}

void worker_readdir(int rank, int sending_rank, const char *base_path, path_node *dest_node, int recurse){
  //When a worker is told to readdir, it comes here
  MPI_Status status;

  char *workbuf;
  int worksize;
  int position, out_position;
  int read_count;
  path_node *work_node = malloc(sizeof(path_node));
  char path[PATHSIZE_PLUS], full_path[PATHSIZE_PLUS];
  char errmsg[MESSAGESIZE];
  char mkdir_path[PATHSIZE_PLUS];


  DIR *dip;
  struct dirent *dit;
  
  int i;

  path_node *new_input_list_head = NULL, *new_input_list_tail = NULL;
  int new_input_list_count = 0;

  PRINT_MPI_DEBUG("rank %d: worker_readdir() Receiving the read_count %d\n", rank, sending_rank);
  if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive read_count\n");
  }
  worksize = read_count * sizeof(path_node);
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
    MPI_Unpack(workbuf, worksize, &position, work_node, sizeof(path_node), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node->data.path, PATHSIZE_PLUS);
    if ((dip = opendir(path)) == NULL){
      snprintf(errmsg, MESSAGESIZE, "Failed to open dir %s\n", path);
      errsend(FATAL, errmsg);
    }
    strncpy(mkdir_path, get_output_path(base_path, work_node, dest_node, recurse), PATHSIZE_PLUS);
    mkdir(mkdir_path, S_IRWXU);
    while ((dit = readdir(dip)) != NULL){
      if (strncmp(dit->d_name, ".", PATHSIZE_PLUS) != 0 && strncmp(dit->d_name, "..", PATHSIZE_PLUS) != 0){
        strncpy(full_path, path, PATHSIZE_PLUS);
        if (full_path[strnlen(full_path, PATHSIZE_PLUS) - 1 ] != '/'){
          strncat(full_path, "/", 1);
        }
        strncat(full_path, dit->d_name, PATHSIZE_PLUS);
        strncpy(work_node->data.path, full_path, PATHSIZE_PLUS);
        enqueue_node(&new_input_list_head, &new_input_list_tail, work_node, &new_input_list_count);
      }
      if (new_input_list_count % 200 == 0){
        send_manager_new_input(200, &new_input_list_head, &new_input_list_tail, &new_input_list_count);
      }
    }
    if (closedir(dip) == -1){
      snprintf(errmsg, MESSAGESIZE, "Failed to closedir: %s", path);
      errsend(1, errmsg);
    }
  }


  while(new_input_list_count != 0){
    send_manager_new_input(100, &new_input_list_head, &new_input_list_tail, &new_input_list_count);
  }

  free(work_node);
  free(workbuf);
  send_manager_work_done(rank);

}

void worker_copylist(int rank, int sending_rank, const char *base_path, path_node *dest_node, int recurse){
  //When a worker is told to copy, it comes here
  MPI_Status status;

  char *workbuf, *writebuf;
  int worksize, writesize;
  int position, out_position;
  int read_count;
  
  path_node *work_node = malloc(sizeof(path_node));
  char path[PATHSIZE_PLUS], out_path[PATHSIZE_PLUS];
  char copymsg[MESSAGESIZE];//, errmsg[MESSAGESIZE];
  struct stat st;
  int num_copied_files = 0, num_copied_bytes = 0;
  
  int i, rc;

  PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
  if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    errsend(FATAL, "Failed to receive read_count\n");
  }
  worksize = read_count * sizeof(path_node);
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
    MPI_Unpack(workbuf, worksize, &position, work_node, sizeof(path_node), MPI_CHAR, MPI_COMM_WORLD);
    strncpy(path, work_node->data.path, PATHSIZE_PLUS);
    strncpy(out_path, get_output_path(base_path, work_node, dest_node, recurse), PATHSIZE_PLUS);
    st = work_node->data.st;
    //sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", slavecopy.req, (long long) slavecopy.offset, (long long) slavecopy.length, copyoutpath)
    rc = copy_file(path, out_path, 0, st.st_size, st);
    if (rc >= 0){
      sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", path, (long long)0, (long long)st.st_size, out_path);
      MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
      num_copied_files +=1;
      num_copied_bytes += st.st_size;
    }
  }
  
  write_buffer_output(writebuf, writesize, read_count); 
  send_manager_copy_stats(num_copied_files, num_copied_bytes);
  send_manager_work_done(rank);

  free(work_node);
  free(workbuf);
  free(writebuf);
}
