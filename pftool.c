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

//Parallel Filesystem
#include <gpfs.h>
#include "gpfs_fcntl.h"

// include that is associated with pftool itself
#include "pftool.h"
#include "debug.h"
#include "recall_api.h"

#define STGPOOLSERVER_PORT 1664

//External Declarations
extern void usage();

int main(int argc, char *argv[]){
  //general variables
  int i;

  //mpi
  int rank, nproc;

  //getopt
  int c;
  int recurse = 0;


  //paths
  char src_path[PATHSIZE_PLUS], dest_path[PATHSIZE_PLUS], temp_path[PATHSIZE_PLUS], temp_path2[PATHSIZE_PLUS];
  char *path_slice;
  struct stat src_stat, dest_stat;
  int statrc;

  //Process using getopt
  while ((c = getopt(argc, argv, "p:c:rh")) != -1) 
    switch(c){
      case 'p':
        //Get the source/beginning path
        snprintf(src_path, PATHSIZE_PLUS, "%s", optarg);        
        break;
      case 'c':
        //Get the destination path
        snprintf(dest_path, PATHSIZE_PLUS, "%s", optarg);        
        break;
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
      if (statrc < 0){
        mkdir(dest_path, S_IRWXU);
      }
      else if (S_ISDIR(src_stat.st_mode) && (S_ISDIR(dest_stat.st_mode))) { 
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
        snprintf(temp_path2, PATHSIZE_PLUS, "%s/%s", dest_path, path_slice);  
        strcpy(dest_path, temp_path2);
        mkdir(dest_path, S_IRWXU);
      }    
    }
  }

  MPI_Bcast(dest_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);    
      
  statrc = lstat(src_path, &src_stat);
  if (statrc == 0) {
    if (S_ISDIR(src_stat.st_mode) && src_path[strlen(src_path) - 1] != '/') {
      strncat(src_path, "/", PATHSIZE_PLUS);
    }
  }

  statrc = lstat(dest_path, &dest_stat);
  if (statrc == 0) {
    if (S_ISDIR(dest_stat.st_mode) && dest_path[strlen(dest_path) - 1] != '/') {
      strncat(dest_path, "/", PATHSIZE_PLUS);
    }
  }

  if (rank == MANAGER_PROC && optind < argc){
    for (i = optind; i < argc; ++i){
      printf(" => %s\n", argv[i]);
    }
  }
  
  //printf("Source: %s Dest: %s\n", src_path, dest_path);
  

  //Program Finished
  MPI_Finalize(); 
  return 0;
}
