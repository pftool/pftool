#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include "recall_api.h"


static char recall_base_directory[PATHSIZE_PLUS];

void set_base_dir(const char *base_dir){
  /*Set the base directory up for all other functions*/
  strcpy(recall_base_directory,  base_dir);
}

char *create_path(const char *extension){
  /*Creates a path based on recall_base_directory*/
  char tmp_path[PATHSIZE_PLUS];
  snprintf(tmp_path, PATHSIZE_PLUS, "%s/%s", recall_base_directory, extension);
  return strdup(tmp_path); 
}

int create_recall_dir(char *dest_path){
  /*Create a recall directory and return it to dest_path*/
  char *recall_dir;
  char hostname[1024];
  int pid;
  time_t current_time;
  
  current_time = time(NULL);
  recall_dir = create_path("recalls");
  gethostname(hostname, 1023);
  pid = getpid();
  snprintf(dest_path, PATHSIZE_PLUS, "%s/%s-%d-%ld", recall_dir, hostname, pid, current_time);
  
  mkdir(dest_path, 0700);
  return 1;
}

int open_rank_file(FILE **fp, const char *dest_path, const char *rank_name){
  /*open a rank file for writing returning the fp for writing*/
  char dest_rank[PATHSIZE_PLUS];
  
  snprintf(dest_rank, PATHSIZE_PLUS, "%s/%s", dest_path, rank_name);
  *fp = fopen(dest_rank, "w");
  if (*fp == NULL){
    perror("open");
    exit(-1);
  }
  
  return 1;
}

int write_rank_file(FILE *fp, const char *line, int newline){
  /*write a string + newline if requested to a rank file*/
  fwrite(line, 1, strlen(line), fp);
  if (newline == 1){
    fwrite("\n", 1, 1, fp);
  }
  return 1;
}

int write_array_rank_file(FILE *fp, char * const *lines, int lines_length){
  int i;
  for (i = 0; i < lines_length; i++){
    write_rank_file(fp, lines[i], 1);
  }
  return 1;
}

int close_rank_file(FILE **fp, const char *dest_path, const char *rank_name){
  /*close the rank fp and rename the rank file appropriately*/
  char src_rank[PATHSIZE_PLUS];
  char dest_rank[PATHSIZE_PLUS];
  time_t current_time;
  
  fclose(*fp);
  current_time = time(NULL);
  
  snprintf(src_rank, PATHSIZE_PLUS, "%s/%s", dest_path, rank_name);
  snprintf(dest_rank, PATHSIZE_PLUS, "%s/%s.%ld.done", dest_path, rank_name, current_time);
  if (rename(src_rank, dest_rank) != 0){
    perror( NULL );
    return 0;
  }
    
  return 1;
}

void rank_finished(const char *dest_path){
  char rank[PATHSIZE_PLUS];
  FILE *fp;
  
  snprintf(rank, PATHSIZE_PLUS, "%s/Finished.stat", dest_path);
  fp = fopen(rank, "w");
  if (fp == NULL){
    perror("open");
    exit(-1);
  }
  
  fclose(fp);
}

int return_first_result(FILE **fp, const char *dest_path, char *result_path){
  
  DIR *dip;
  struct dirent *dit;
  int return_val = 0;

  if ((dip = opendir(dest_path)) == NULL){
    perror("opendir");
  }
  
  while((dit = readdir(dip)) != NULL){
    if (strstr(dit->d_name, "done") != 0){
      if (strstr(dit->d_name, "results") != 0){
        //printf("%s/%s\n", dest_path, dit->d_name);
        sprintf(result_path, "%s/%s", dest_path, dit->d_name);
        
        *fp = fopen(result_path, "r");
        if (*fp == NULL){
          perror("open");
        }
        return_val = 1;
      }
    }
  }
  
  
  if(closedir(dip) == -1){
    perror("closedir");
  }
  return return_val;
}

int is_finished_recalling(const char *dest_path){
  DIR *dip;
  struct dirent *dit;
  int dircount = 0;
  int finished_encountered = 0;
  
  
  if ((dip = opendir(dest_path)) == NULL){
    perror("opendir");
    return 0;
  }
  
  while((dit = readdir(dip)) != NULL){
    dircount++;
    if (strstr(dit->d_name, "Finished.recall") != 0){
      finished_encountered = 1;
    }
  }
  
  if(closedir(dip) == -1){
    perror("closedir");
  }
  
  //printf("dircount ==> %d, finished_encountered ==> %d\n",dircount, finished_encountered);
  
  
  if (dircount == 3 && finished_encountered == 1){
    return 1;
  }
  else{
    return 0;
  }
}

int get_result_data(FILE *fp, char **data){
  char line[PATHSIZE_PLUS];
  int data_length = 0;
  while(fgets(line, PATHSIZE_PLUS, fp) != NULL){
    //printf("%s", line);
    data[data_length] = malloc(PATHSIZE_PLUS * sizeof(char));
    sprintf(data[data_length], "%s", line);
    data_length++;
  }
  return data_length;
}

void result_finished(FILE **fp, const char *result_path){
  fclose(*fp);
  if (remove(result_path) != 0){
    perror("Error deleting file");
  }
}

