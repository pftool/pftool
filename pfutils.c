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
#include <gpfs.h>
#include <dmapi.h>

#include "pfutils.h"
#include "debug.h"

#include <syslog.h>

/* Syslog related data structure */
char *ident = "PFTOOL-LOG:";		/* pflog identification */

void usage () {
	/* print usage statement */
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	printf (" \n");
	printf ("\npftool: parallel file tool utilities\n");
	printf ("1. Walk through directory tree structure and gather statistics on files and\n");
	printf ("   directories encountered.\n");
	printf ("2. Apply various data moving operationbased on the selected options \n");
	printf ("\n");
	printf ("mpirun -np totalprocesses pftool [options]\n");
	printf (" Options\n");
	printf (" --path [-p] path                                : path to start parallel tree walk (required argument)\n");
	printf (" --copypath [-c] copypath                        : destination path for data movement\n");
	printf (" --Recurse [-R] recursive                        : recursive operation down directory tree Active=1, InActive=0 (default 0)\n");
	printf (" --help [-h]                                     : Print Usage information\n");
	printf (" \n");
	printf (" Using man pftool for the details of pftool information \n");
	printf (" \n");
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	return;
}

void errsend(int rank, int fatal, char *error_text){
  char *errworkbufsend = malloc(WORKSIZE * sizeof(char));
  char errormsg[ERROR_SIZE];
  int position;
  
  MPI_Pack(&rank, 1, MPI_INT, errworkbufsend, WORKSIZE, &position, MPI_COMM_WORLD);
  
  //modify the message if it's a fatal error
  if (fatal){
    snprintf(errormsg, ERROR_SIZE, "ERROR FATAL: %s",error_text);
  }
  else{
    snprintf(errormsg, ERROR_SIZE, "ERROR NONFATAL: %s",error_text);
  }
  MPI_Pack(&errormsg, ERROR_SIZE, MPI_CHAR, errworkbufsend, WORKSIZE, &position, MPI_COMM_WORLD);

  if (MPI_Send(errworkbufsend, position, MPI_PACKED, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  
  if (fatal){
    MPI_Abort(MPI_COMM_WORLD, -1);
  }  

  free(errworkbufsend);

}



//Queue Funciton Definitions

//Input Queue
void enqueue_path(path_node **head, char *path, int *count){
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
  path_node *temp_node = *head;
  if (temp_node == NULL){
    return;
  }
  *head = temp_node->next;
  free(temp_node);
  *count -= 1;
}

void print_queue_path(path_node *head){
    while(head != NULL){
      printf("%s\n", head->path);
      head = head->next;
    }
}
