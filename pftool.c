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

//functions that use workers
extern void errsend(int rank, int fatal, char *error_text);
extern void write_output(int rank, char *message);
extern void write_buffer_output(int rank, char *buffer, int buffer_size, int buffer_count);
extern void stat_path(int rank, int target_rank, int num_stat, path_node **head, int *count);
extern void work_done(int rank);
extern int get_free_rank(int *proc_status, int start_range, int end_range);
extern int processing_complete(int *proc_status, int nproc);
extern void exit_rank(int target_rank);
extern void send_command(int target_rank, int type_cmd);

//queues 
extern void enqueue_path(path_node **head, char *path, int *count);
extern void dequeue_path(path_node **head, int *count);
extern void print_queue_path(path_node *head);

int main(int argc, char *argv[]){
  //general variables
  int i;

  //mpi
  int rank, nproc;

  //getopt
  int c;
  int recurse = 0;
  char jid[128];
  int work_type;

  //queues
  path_node *input_queue = NULL;
  int input_queue_count = 0;
  
  //paths
  char src_path[PATHSIZE_PLUS], dest_path[PATHSIZE_PLUS], temp_path[PATHSIZE_PLUS], temp_path2[PATHSIZE_PLUS];
  char *path_slice;
  struct stat src_stat, dest_stat;
  int statrc;

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
        strncpy(jid, optarg, 128);
      case 'r':
        //Recurse
        recurse = 1;
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
    if (optind < argc && work_type == COPYWORK){
      statrc = lstat(dest_path, &dest_stat);
      if (statrc < 0 || !S_ISDIR(dest_stat.st_mode)){
        printf("Multiple inputs and target '%s' is not a directory\n", dest_path);
        MPI_Abort(MPI_COMM_WORLD, -1);
        return -1;
      }
    }
    //recursion
    else if (recurse){
      lstat(src_path, &src_stat);
      statrc = lstat(dest_path, &dest_stat);
      //src is a dir and there was no wildcard
      if (statrc < 0 && S_ISDIR(src_stat.st_mode)){
        mkdir(dest_path, S_IRWXU);
      }
      //The dest is a dir
      else if (S_ISDIR(dest_stat.st_mode)) { 
        strcpy(temp_path, src_path);
        while (temp_path[strlen(temp_path) - 1] == '/') {
          temp_path[strlen(temp_path) - 1] = '\0';
        }    
        if (strstr(temp_path, "/")) {
          path_slice = strrchr(temp_path, '/') + 1; 
        }    
        else {
          path_slice = (char *) temp_path;
        }    
        if (dest_path[strlen(dest_path) - 1] != '/') {
          snprintf(temp_path2, PATHSIZE_PLUS, "%s/%s", dest_path, path_slice);
        }    
        else {
          snprintf(temp_path2, PATHSIZE_PLUS, "%s%s", dest_path, path_slice);
        }
        strcpy(dest_path, temp_path2);
      }    
      if (S_ISDIR(src_stat.st_mode)){
        mkdir(dest_path, S_IRWXU);
      }
    }
  }

  MPI_Bcast(dest_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);    
      
  statrc = lstat(src_path, &src_stat);
  //src exists
  if (statrc == 0) {
    if (S_ISDIR(src_stat.st_mode) && src_path[strlen(src_path) - 1] != '/') {
      strncat(src_path, "/", PATHSIZE_PLUS);
    }
  }

  statrc = lstat(dest_path, &dest_stat);
  //dest exists
  if (statrc == 0) {
    if (S_ISDIR(dest_stat.st_mode) && dest_path[strlen(dest_path) - 1] != '/') {
      strncat(dest_path, "/", PATHSIZE_PLUS);
    }
  }

  //process remaining optind for * and multiple src files
  // stick them on the input_queue
  if (rank == MANAGER_PROC && optind < argc){
    enqueue_path(&input_queue, src_path, &input_queue_count);
    for (i = optind; i < argc; ++i){
      enqueue_path(&input_queue, argv[i], &input_queue_count);
    }
  }
  
  if (rank == OUTPUT_PROC){
    worker(rank);
  } 
  

  if (rank == MANAGER_PROC){
    manager(rank, jid, nproc, input_queue, input_queue_count, work_type);
  }
  else if (rank != OUTPUT_PROC){
    worker(rank);
  }

  //Program Finished
  //printf("%d -- done.\n", rank);
  MPI_Finalize(); 
  return 0;
}


void manager(int rank, char *jid, int nproc, path_node *input_queue, int input_queue_count, int work_type){
  MPI_Status status;
  int all_done = 0, message_ready = 0, probecount = 0;
  int prc, type_cmd;
  int work_rank;

  int i;
  int *proc_status;
  struct timeval in, out;

  char message[MESSAGESIZE];
  char beginning_path[PATHSIZE_PLUS];


  //path stuff
  strncpy(beginning_path, input_queue->path, PATHSIZE_PLUS);

  //proc stuff
  proc_status = malloc(nproc * sizeof(int));
  //initialize proc_status
  for (i = 0; i < nproc; i++){
    proc_status[i] = 0;
  }
  

  sprintf(message, "INFO  HEADER   ========================  %s  ============================\n", jid);
  write_output(rank, message);
  sprintf(message, "INFO  HEADER   Starting Path: %s\n", beginning_path);
  write_output(rank, message);

  
  //starttime
  gettimeofday(&in, NULL);

  while (all_done == 0){
    //poll for message
    while ( message_ready == 0){ 
      prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
      if (prc != MPI_SUCCESS) {
        errsend(rank, FATAL, "MPI_Iprobe failed\n");
        message_ready = -1; 
      }   
      else{
        probecount++;
      }   

      if  (probecount % 3000 == 0){ 
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
  
        //first run through the remaining stat_queue
        if (work_rank != -1 && input_queue_count != 0){
          proc_status[work_rank] = 1;
          stat_path(rank, work_rank, 50, &input_queue, &input_queue_count);
        }
      }
    }   

    //grab message type
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      MPI_Abort(MPI_COMM_WORLD, -1);
    }   

    //do operations based on the message
    switch(type_cmd){ 
      case WORKDONECMD:
        manager_workdone(proc_status);       
        break;
      default:
        break;
    }
    message_ready = 0;
    
    //are we finished?
    if (input_queue_count == 0 && processing_complete(proc_status, nproc) == 0){
      all_done = 1;
    }
    
  }
  gettimeofday(&out, NULL);
  //Manager is done, cleaning have the other ranks exit
  for (i = 1; i < nproc; i++){
    exit_rank(i);
  }

  //free any allocated stuff
  free(proc_status);
  
}

void manager_workdone(int *proc_status){
  MPI_Status status;
  int rank = 0;  
  
  //gather the rank
  if (MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      MPI_Abort(MPI_COMM_WORLD, -1);
  }
  proc_status[rank] = 0;

}

void worker(int rank){
  MPI_Status status;
  int all_done = 0, message_ready = 0, probecount = 0;
  int prc;
  

  int type_cmd;
  char *workbuf= malloc(WORKSIZE * sizeof(char));


  //change this to get request first, process, then get work    
  while ( all_done == 0){
    //poll for message
    while ( message_ready == 0){
      prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
      if (prc != MPI_SUCCESS) {
        MPI_Abort(MPI_COMM_WORLD, -1);
        message_ready = -1;
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
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      MPI_Abort(MPI_COMM_WORLD, -1);
    }

    //do operations based on the message
    switch(type_cmd){
      case OUTCMD:
        worker_output();
        break;
      case BUFFEROUTCMD:
        worker_buffer_output();
        break;
      case EXITCMD:
        all_done = 1;
        break;
      case NAMECMD:
        worker_stat(rank);
        break;
      default:
        break;
    }
    message_ready = 0;
    //process message
  }
  free(workbuf);
}

void worker_output(){
  //have a worker receive and print a single message
  MPI_Status status;
  
  int rank;
  char msg[MESSAGESIZE];

  //gather the rank
  if (MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  //gather the message to print
  if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  printf("Rank %d: %s", rank, msg);
  
}

void worker_buffer_output(){
  //have a worker receive and print a single message
  MPI_Status status;
  
  int rank;
  int message_count;
  char msg[MESSAGESIZE];

  char *buffer;
  int buffersize;
  int position;

  int i;

  //gather the rank
  if (MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  //gather the message_count
  if (MPI_Recv(&message_count, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
    
  buffersize = MESSAGESIZE*message_count;
  buffer = (char *) malloc(buffersize * sizeof(char));
  
  //gather the path to stat
  if (MPI_Recv(buffer, buffersize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  position = 0;
  for (i = 0; i < message_count; i++){
    MPI_Unpack(buffer, buffersize, &position, msg, MESSAGESIZE, MPI_CHAR, MPI_COMM_WORLD);
    printf("Rank %d: %s", rank, msg);
  }
}

void worker_stat(int rank){
  //When a worker is told to stat, it comes here
  MPI_Status status;
  int req_rank;  

  char *workbuf, *writebuf;
  int worksize, writesize;
  int position, out_position;
  int stat_count;
  char path[PATHSIZE_PLUS];
  char errortext[MESSAGESIZE], statrecord[MESSAGESIZE];

  struct stat st;
  struct statfs stfs;
  struct tm sttm;
  int sourcefs;
  char sourcefsc[5], modebuf[15], timebuf[30];
  
  int i;

  //classification
  path_node *dir_list, *reg_list;
  int dir_list_count = 0, reg_list_count = 0;

  //gather the rank
  if (MPI_Recv(&req_rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  if (MPI_Recv(&stat_count, 1, MPI_INT, req_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  worksize = PATHSIZE_PLUS * stat_count;
  workbuf = (char *) malloc(worksize * sizeof(char));
  
  writesize = MESSAGESIZE * stat_count;
  writebuf = (char *) malloc(writesize * sizeof(char));

  //gather the path to stat
  if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  
  position = 0;
  out_position = 0;
  for (i = 0; i < stat_count; i++){
    MPI_Unpack(workbuf, worksize, &position, path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
    if (statfs(path, &stfs) < 0) { 
      snprintf(errortext, MESSAGESIZE, "Failed to statfs path %s", path);
      errsend(rank, FATAL, errortext);
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
   

    if (lstat(path, &st) == -1) {
      snprintf(errortext, MESSAGESIZE, "Failed to stat path %s", path);
      errsend(rank, FATAL, errortext);
    }

    printmode(st.st_mode, modebuf);
    memcpy(&sttm, localtime(&st.st_mtime), sizeof(sttm));
    strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);

    if (st.st_size > 0 && st.st_blocks == 0){                                                                                                                                                                                                                                                          
      sprintf(statrecord, "INFO  DATASTAT %sM %s %6lu %6d %6d %21lld %s %s\n", sourcefsc, modebuf, st.st_blocks, st.st_uid, st.st_gid, (long long) st.st_size, timebuf, path);
    }
    else{
      sprintf(statrecord, "INFO  DATASTAT %s- %s %6lu %6d %6d %21lld %s %s\n", sourcefsc, modebuf, st.st_blocks, st.st_uid, st.st_gid, (long long) st.st_size, timebuf, path);
    }
    MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
    //write_output(rank, statrecord);
  } 
  write_buffer_output(rank, writebuf, writesize, stat_count);
  free(workbuf);
  free(writebuf);
  work_done(rank);  
}

