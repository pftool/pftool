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


//Queue Function Definitions
void enqueue_path(path_node **head, char *path, int *count){
  //stick a path on the end of the queue
  path_node *new_node = malloc(sizeof(path_node));
  path_node *temp_node = *head;
  
  strncpy(new_node->path, path, PATHSIZE_PLUS);  
  new_node->next = NULL;
  
  if (temp_node == NULL){
    *head = new_node;
  }
  else{
    while (temp_node->next != NULL){
      temp_node = temp_node->next;
    }   
    temp_node->next = new_node;
  }
  *count += 1;
} 

void dequeue_path(path_node **head, int *count){
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

//functions that work with ranks
void errsend(int rank, int fatal, char *error_text){
  //send an error message to the outputproc. Die if fatal.
  char errormsg[MESSAGESIZE];

  if (fatal){
    snprintf(errormsg, MESSAGESIZE, "ERROR FATAL: %s\n",error_text);
  }
  else{
    snprintf(errormsg, MESSAGESIZE, "ERROR NONFATAL: %s\n",error_text);
  }
  
  write_output(rank, errormsg);

  if (fatal){
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

}

void write_output(int rank, char *message){
  //write a single line using the outputproc

  //set the command type
  send_command(OUTPUT_PROC, OUTCMD);

  //send the rank
  if (MPI_Send(&rank, 1, MPI_INT, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
  //send the message
  if (MPI_Send(message, MESSAGESIZE, MPI_CHAR, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}

void write_buffer_output(int rank, char *buffer, int buffer_size, int buffer_count){
  //write a buffer to the output proc

  //set the command type
  send_command(OUTPUT_PROC, BUFFEROUTCMD);

  //send the rank
  if (MPI_Send(&rank, 1, MPI_INT, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
  
  //send the size of the buffer
  if (MPI_Send(&buffer_count, 1, MPI_INT, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  if (MPI_Send(buffer, buffer_size, MPI_PACKED, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }


}

void stat_path(int rank, int target_rank, int num_stat, path_node **head, int *count){
  //stat a chunk of paths (passed in via linked list)
  int stat_count = 0, position = 0;
  int worksize = PATHSIZE_PLUS * num_stat;
  char *workbuf = (char *) malloc(worksize * sizeof(char));
  
  send_command(target_rank, NAMECMD);
  
  //send the rank
  if (MPI_Send(&rank, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  while(*head != NULL && stat_count != num_stat){
    stat_count++;
    MPI_Pack((*head)->path, PATHSIZE_PLUS, MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
    dequeue_path(head, count);
  }

  //send the number of files stated
  if (MPI_Send(&stat_count, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  
  //recalculate worksize
  worksize = PATHSIZE_PLUS * stat_count;
  if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  free(workbuf);
  
}

void work_done(int rank){
  //the worker is finished processing, notify the manager
  send_command(MANAGER_PROC, WORKDONECMD);

  //send the rank
  if (MPI_Send(&rank, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
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

void exit_rank(int target_rank){
  //order a rank to exit
  send_command(target_rank, EXITCMD);
}

void send_command(int target_rank, int type_cmd){
  //Tell a rank it's time to begin processing
  if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}

