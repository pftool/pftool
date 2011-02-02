/************************************************************************************
* Name:  pfutils part of pftool
*
* Description:
*  This file contains utility functions used by pftool.c
*
* Author:  Alfred Torrez / Ben McClelland / Gary Grider / HB Chen / Aaron Torres
*
**********************************************************************************************/

#include <fcntl.h>
#include <errno.h>
#include <utime.h>

/* special includes for gpfs and dmapi */
//#include <gpfs.h>
//#include <dmapi.h>

#include "pfutils.h"
#include "debug.h"

#include <syslog.h>

void usage () {
	// print usage statement 
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	printf (" \n");
	printf ("\npftool: parallel file tool utilities\n");
	printf ("1. Walk through directory tree structure and gather statistics on files and\n");
	printf ("   directories encountered.\n");
	printf ("2. Apply various data moving operationbased on the selected options \n");
	printf ("\n");
	printf ("mpirun -np totalprocesses pftool [options]\n");
	printf (" Options\n");
	printf (" [-p] path                                 : path to start parallel tree walk (required argument)\n");
	printf (" [-c] copypath                             : destination path for data movement\n");
	printf (" [-j] jobid                                : unique jobid for the pftool job\n");
	printf (" [-R] recursive                            : recursive operation down directory tree Active=1, InActive=0 (default 0)\n");
	printf (" [-h]                                      : Print Usage information\n");
	printf (" \n");
	printf (" Using man pftool for the details of pftool information \n");
	printf (" \n");
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	return;
}

char *printmode (mode_t aflag, char *buf){
  // print the mode in a regular 'pretty' format
  static int m0[] = { 1, S_IREAD >> 0, 'r', '-' };
  static int m1[] = { 1, S_IWRITE >> 0, 'w', '-' };
  static int m2[] = { 3, S_ISUID | S_IEXEC, 's', S_IEXEC, 'x', S_ISUID, 'S', '-' };
  static int m3[] = { 1, S_IREAD >> 3, 'r', '-' };
  static int m4[] = { 1, S_IWRITE >> 3, 'w', '-' };
  static int m5[] = { 3, S_ISGID | (S_IEXEC >> 3), 's', 
    S_IEXEC >> 3, 'x', S_ISGID, 'S', '-'
  };
  static int m6[] = { 1, S_IREAD >> 6, 'r', '-' };
  static int m7[] = { 1, S_IWRITE >> 6, 'w', '-' };
  static int m8[] = { 3, S_ISVTX | (S_IEXEC >> 6), 't', S_IEXEC >> 6, 'x', S_ISVTX, 'T', '-' };
  static int *m[] = { m0, m1, m2, m3, m4, m5, m6, m7, m8 };

  int i, j, n;
  int *p = (int *) 1;;

  buf[0] = S_ISREG (aflag) ? '-' : S_ISDIR (aflag) ? 'd' : S_ISLNK (aflag) ? 'l' : S_ISFIFO (aflag) ? 'p' : S_ISCHR (aflag) ? 'c' : S_ISBLK (aflag) ? 'b' : S_ISSOCK (aflag) ? 's' : '?'; 
  for (i = 0; i <= 8; i++) {
    for (n = m[i][0], j = 1; n > 0; n--, j += 2) { 
      p = m[i];
      if ((aflag & p[j]) == p[j]) {
        j++; 
        break;
      }    
    }    
    buf[i + 1] = p[j];
  }
  buf[10] = '\0';
  return buf; 
}

char *get_base_path(const char *path, int wildcard){
  //wild card is a boolean
  char base_path[PATHSIZE_PLUS], dir_name[PATHSIZE_PLUS];
  char errmsg[MESSAGESIZE];
  struct stat st;
  int rc;

  rc = lstat(path, &st);
  if (rc < 0){
    sprintf(errmsg, "Failed to stat %s\n", path);
    errsend(FATAL, errmsg);
  }

  strncpy(dir_name, dirname(strdup(path)), PATHSIZE_PLUS);
  
  if (strncmp(".", dir_name, PATHSIZE_PLUS == 0) && S_ISDIR(st.st_mode)){
    strncpy(base_path, path, PATHSIZE_PLUS);
  }
  else if (S_ISDIR(st.st_mode) && wildcard == 0){
    strncpy(base_path, path, PATHSIZE_PLUS);
  }
  else{
    strncpy(base_path, dir_name, PATHSIZE_PLUS);
  }
  while(base_path[strlen(base_path) - 1] == '/'){
    base_path[strlen(base_path) - 1] = '\0'; 
  }

  return strndup(base_path, PATHSIZE_PLUS);
}

char *get_dest_path(const char *beginning_path, const char *dest_path, int recurse){
  int rc;
  struct stat beg_st, dest_st;
  char temp_path[PATHSIZE_PLUS], final_dest_path[PATHSIZE_PLUS];
  char *path_slice;

  strncpy(final_dest_path, dest_path, PATHSIZE_PLUS);
  strncpy(temp_path, beginning_path, PATHSIZE_PLUS);

  while (temp_path[strlen(temp_path)-1] == '/'){
    temp_path[strlen(temp_path)-1] = '\0';
  }

  //recursion special cases
  if (recurse && strncmp(temp_path, "..", PATHSIZE_PLUS) != 0){
    rc = lstat(beginning_path, &beg_st);
    if (rc < 0){
      errsend(FATAL, "Unable to stat beginning_path in get_dest_path.\n");
    }

    rc = lstat(dest_path, &dest_st);
    if (rc >= 0 && S_ISDIR(dest_st.st_mode) && S_ISDIR(beg_st.st_mode)){
      if (strstr(temp_path, "/") == NULL){
        path_slice = (char *)temp_path;
      }
      else{
        path_slice = strrchr(temp_path, '/') + 1;
      }
      if (final_dest_path[strlen(final_dest_path)-1] != '/'){
        strncat(final_dest_path, "/", PATHSIZE_PLUS);
      }
      strncat(final_dest_path, path_slice, PATHSIZE_PLUS);
    }
  }
  return strndup(final_dest_path, PATHSIZE_PLUS);
}

char *get_output_path(const char *base_path, const char *src_path, const char *dest_path, int recurse){
  char output_path[PATHSIZE_PLUS];
  char *path_slice;

  //remove a trailing slash
  strncpy(output_path, dest_path, PATHSIZE_PLUS);
  while (output_path[strlen(output_path) - 1] == '/'){
    output_path[strlen(output_path) - 1] = '\0';
  }

  //path_slice = strstr(src_path, base_path);
  if (recurse == 0){
    if(strstr(src_path, "/") != NULL){
      path_slice = (char *) strrchr(src_path, '/')+1;
    }
    else{
      path_slice = (char *) src_path;
    }
  }
  else{
    if (strncmp(base_path, ".", PATHSIZE_PLUS) == 0){
      path_slice = (char *) src_path;
    }
    else{
      path_slice = strndup(src_path + strlen(base_path) + 1, PATHSIZE_PLUS);
    }
  }
  strncat(output_path, "/", PATHSIZE_PLUS);
  strncat(output_path, path_slice, PATHSIZE_PLUS);
  return strndup(output_path, PATHSIZE_PLUS);

}

//local functions only
void send_command(int target_rank, int type_cmd){
  //Tell a rank it's time to begin processing
  if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}

void send_path_list(int target_rank, int command, int num_send, path_node **list_head, path_node **list_tail, int *list_count){
  int path_count = 0, position = 0;
  int worksize, workcount;


  if (path_count >= *list_count){
    workcount = path_count;
  }
  else{
    workcount = *list_count;
  }

  worksize = workcount * PATHSIZE_PLUS;
  char *workbuf = (char *) malloc(worksize * sizeof(char));

  while(path_count < workcount){
    path_count++;
    MPI_Pack((*list_head)->path, PATHSIZE_PLUS, MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
    dequeue_path(list_head, list_tail, list_count);
  }
  //send the command to get started
  send_command(target_rank, command);

  //send the # of paths
  if (MPI_Send(&workcount, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  //recalculate worksize
  if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  free(workbuf);
}

//manager
void send_manager_nonfatal_inc(){
  send_command(MANAGER_PROC, NONFATALINCCMD);
}

void send_manager_regs(int num_send, path_node **reg_list_head, path_node **reg_list_tail, int *reg_list_count){
  //sends a chunk of regular files to the manager
  send_path_list(MANAGER_PROC, REGULARCMD, num_send, reg_list_head, reg_list_tail, reg_list_count);
}

void send_manager_dirs(int num_send, path_node **dir_list_head, path_node **dir_list_tail, int *dir_list_count){
  //sends a chunk of regular files to the manager
  send_path_list(MANAGER_PROC, DIRCMD, num_send, dir_list_head, dir_list_tail, dir_list_count);
}

void send_manager_tape(int num_send, path_node **tape_list_head, path_node **tape_list_tail, int *tape_list_count){
  //sends a chunk of regular files to the manager
  send_path_list(MANAGER_PROC, TAPECMD, num_send, tape_list_head, tape_list_tail, tape_list_count);
}

void send_manager_new_input(int num_send, path_node **new_input_list_head, path_node **new_input_list_tail, int *new_input_list_count){
  //sends additional input files to the manager
  send_path_list(MANAGER_PROC, INPUTCMD, num_send, new_input_list_head, new_input_list_tail, new_input_list_count);
}

void send_manager_work_done(){
  //the worker is finished processing, notify the manager
  send_command(MANAGER_PROC, WORKDONECMD);
}

//worker
void write_output(char *message){
  //write a single line using the outputproc

  //set the command type
  send_command(OUTPUT_PROC, OUTCMD);

  //send the message
  if (MPI_Send(message, MESSAGESIZE, MPI_CHAR, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}


void write_buffer_output(char *buffer, int buffer_size, int buffer_count){
  //write a buffer to the output proc

  //set the command type
  send_command(OUTPUT_PROC, BUFFEROUTCMD);

  //send the size of the buffer
  if (MPI_Send(&buffer_count, 1, MPI_INT, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  if (MPI_Send(buffer, buffer_size, MPI_PACKED, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
}

void send_worker_stat_path(int target_rank, int num_send, path_node **input_queue_head, path_node **input_queue_tail, int *input_queue_count){
  //send a worker a list of paths to stat
  send_path_list(target_rank, NAMECMD, num_send, input_queue_head, input_queue_tail, input_queue_count);
}

void send_worker_readdir(int target_rank, int num_send, path_node **dir_work_queue_head, path_node **dir_work_queue_tail, int *dir_work_queue_count){
  //send a worker a list of paths to stat
  send_path_list(target_rank, DIRCMD, num_send, dir_work_queue_head, dir_work_queue_tail, dir_work_queue_count);
}

void send_worker_copy_path(int target_rank, int num_send, path_node **work_queue_head, path_node **work_queue_tail, int *work_queue_count){
  //send a worker a list of paths to stat
  send_path_list(target_rank, COPYCMD, num_send, work_queue_head, work_queue_tail, work_queue_count);
}


void send_worker_exit(int target_rank){
  //order a rank to exit
  send_command(target_rank, EXITCMD);
}


//functions that use workers
void errsend(int fatal, char *error_text){
  //send an error message to the outputproc. Die if fatal.
  char errormsg[MESSAGESIZE];

  if (fatal){
    snprintf(errormsg, MESSAGESIZE, "ERROR FATAL: %s\n",error_text);
  }
  else{
    snprintf(errormsg, MESSAGESIZE, "ERROR NONFATAL: %s\n",error_text);
  }
  
  write_output(errormsg);

  if (fatal){
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
  else{
    send_manager_nonfatal_inc();
  }
}

int get_free_rank(int *proc_status, int start_range, int end_range){
  //given an inclusive range, return the first encountered free rank
  int i;

  for (i = start_range; i <= end_range; i++){
    if (proc_status[i] == 0){
      return i;
    }
  }
  return -1;
}

int processing_complete(int *proc_status, int nproc){
  //are all the ranks free?
  int i;
  int count = 0;
  for (i = 0; i < nproc; i++){
    if (proc_status[i] == 1){
      count++;
    }
  }
  return count;
}

//Queue Function Definitions
void enqueue_path(path_node **head, path_node **tail, char *path, int *count){
  //stick a path on the end of the queue
  path_node *new_node = malloc(sizeof(path_node));
  strncpy(new_node->path, path, PATHSIZE_PLUS);  
  new_node->next = NULL;
  
  if (*head == NULL){
    *head = new_node;
    *tail = *head;
  }
  else{
    (*tail)->next = new_node;
    *tail = (*tail)->next;
  }
  /*  
    while (temp_node->next != NULL){
      temp_node = temp_node->next;
    }   
    temp_node->next = new_node;
  }*/
  *count += 1;
} 

void dequeue_path(path_node **head, path_node **tail, int *count){
  //remove a path from the front of the queue
  path_node *temp_node = *head;
  if (temp_node == NULL){
    return;
  }
  *head = temp_node->next;
  free(temp_node);
  *count -= 1;
}

void print_queue_path(path_node *head){
    //print the entire queue
    while(head != NULL){
      printf("%s\n", head->path);
      head = head->next;
    }
}

void delete_queue_path(path_node **head, int *count){
  path_node *temp = *head;
  while(temp){
    *head = (*head)->next;
    free(temp);
    temp = *head;
  }
  *count = 0;
}


