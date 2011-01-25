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

//Parallel Filesystem
#include <gpfs.h>
#include "gpfs_fcntl.h"

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
extern void stat_path(int rank, int target_rank, char *path);
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
    if (optind < argc){
      statrc = lstat(dest_path, &dest_stat);
      if (statrc < 0 || !S_ISDIR(dest_stat.st_mode)){
        printf("pfcp: target '%s' is not a directory\n", dest_path);
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
    manager(rank, jid, nproc, input_queue);
  }
  else if (rank != OUTPUT_PROC){
    worker(rank);
  }

  //Program Finished
  //printf("%d -- done.\n", rank);
  free(input_queue);
  MPI_Finalize(); 
  return 0;
}


void manager(int rank, char *jid, int nproc, path_node *input_queue){
  int i;
  int *proc_status;

  char message[MESSAGESIZE];
  char beginning_path[PATHSIZE_PLUS];
  
  path_node *current_input = input_queue;

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

  //lets send a stat to rank 3
  while (current_input != NULL){
    stat_path(rank, 3, current_input->path);
    current_input = current_input->next;
  }
  usleep(1000);

  //Manager is done, cleaning have the other ranks exit
  for (i = 1; i < nproc; i++){
    exit_rank(i);
  }

  //free any allocated stuff
  free(proc_status);
  
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
        //report an error here
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
        //error message
    }

    //do operations based on the message
    switch(type_cmd){
      case OUTCMD:
        worker_output();
        break;
      case EXITCMD:
        all_done = 1;
        break;
      case NAMECMD:
        worker_stat();
        break;
      default:
        break;
    }
    //process message
  }
  free(workbuf);
}

void worker_output(){
  MPI_Status status;
  
  int rank;
  char msg[MESSAGESIZE];

  //gather the rank
  if (MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        //error message
  }
  //gather the message to print
  if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        //error message
  }
  printf("Rank %d: %s", rank, msg);
}

void worker_stat(){
  MPI_Status status;
  int rank;  
  char path[PATHSIZE_PLUS];
  char errortext[MESSAGESIZE], statrecord[MESSAGESIZE];

  struct stat st;
  struct statfs stfs;
  struct tm sttm;
  int sourcefs;
  char sourcefsc[5], modebuf[15], timebuf[30];

  //gather the rank
  if (MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        //error message
  }

  //gather the path to stat
  if (MPI_Recv(path, PATHSIZE_PLUS, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        //error message
  }

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

  write_output(rank, statrecord);
  

  


  
}
