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
  struct stat st;
  int rc;

  rc = lstat(path, &st);
  if (rc < 0){
    fprintf(stderr, "Failed to stat path %s\n", path);
    MPI_Abort(MPI_COMM_WORLD, -1);
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

void get_dest_path(const char *beginning_path, const char *dest_path, path_item *dest_node, int recurse, int makedir){
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

    rc = lstat(final_dest_path, &dest_st);
    if (S_ISDIR(beg_st.st_mode) && makedir == 1){   
      mkdir(final_dest_path, S_IRWXU);
    }
  }


  rc = lstat(final_dest_path, &dest_st);
  if (rc >= 0){
    (*dest_node).st = dest_st;
  }
  else{
    (*dest_node).st.st_mode = 0;
  }
  strncpy((*dest_node).path, final_dest_path, PATHSIZE_PLUS);
}

char *get_output_path(const char *base_path, path_item src_node, path_item dest_node, int recurse){
  char output_path[PATHSIZE_PLUS];
  char *path_slice;

  //remove a trailing slash
  strncpy(output_path, dest_node.path, PATHSIZE_PLUS);
  while (output_path[strlen(output_path) - 1] == '/'){
    output_path[strlen(output_path) - 1] = '\0';
  }

  //path_slice = strstr(src_path, base_path);
  if (recurse == 0){
    if(strstr(src_node.path, "/") != NULL){
      path_slice = (char *) strrchr(src_node.path, '/')+1;
    }
    else{
      path_slice = (char *) src_node.path;
    }
  }
  else{
    if (strncmp(base_path, ".", PATHSIZE_PLUS) == 0){
      path_slice = (char *) src_node.path;
    }
    else{
      path_slice = strndup(src_node.path + strlen(base_path) + 1, PATHSIZE_PLUS);
    }
  }
  if (S_ISDIR(dest_node.st.st_mode)){
    strncat(output_path, "/", PATHSIZE_PLUS);
    strncat(output_path, path_slice, PATHSIZE_PLUS);
  }
  return strndup(output_path, PATHSIZE_PLUS);

}

int copy_file(const char *src_file, const char *dest_file, off_t offset, off_t length, struct stat src_st){
  //take a src, dest, offset and length. Copy the file and return 0 on success, -1 on failure
  MPI_Status status;
  int rc;
  //1 MB copy size
  int blocksize = 1048576, completed = 0;
  char *buf;
  char errormsg[MESSAGESIZE];

  //FILE *src_fd, *dest_fd;  
  char source_file[PATHSIZE_PLUS], destination_file[PATHSIZE_PLUS];
  MPI_File src_fd, dest_fd;

  int mode;
  struct utimbuf ut;

  //can't be const for MPI_IO
  strncpy(source_file, src_file, PATHSIZE_PLUS);
  strncpy(destination_file, dest_file, PATHSIZE_PLUS);

  //incase someone accidently set and offset+length that exceeds the file bounds
  if ((src_st.st_size - offset) < length){
    length = src_st.st_size - offset;
  } 

  //a file less then 1 MB
  if (length < blocksize){
    blocksize = length;
  }

  buf = malloc(blocksize * sizeof(char));
  memset(buf, 0, sizeof(buf));

  //MPI_File_read(src_fd, buf, 2, MPI_BYTE, &status);

  

  //open the source file for reading in binary mode
  rc = MPI_File_open(MPI_COMM_SELF, source_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &src_fd);
  if (rc != 0){
    sprintf(errormsg, "Failed to open file %s for read", src_file);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  //first create a file and open it for appending (file doesn't exist)
  rc = MPI_File_open(MPI_COMM_SELF, destination_file, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &dest_fd);
  if (rc != 0){
    sprintf(errormsg, "Failed to open file %s for write", dest_file);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  //seek to the specified offset for the source
  rc = MPI_File_seek(src_fd, offset, MPI_SEEK_SET);
  if (rc != 0){
    sprintf(errormsg, "Failed to fseek file: %s offset: %ld", src_file, offset);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  //seek to the specified offset for the dest;
  rc = MPI_File_seek(dest_fd, offset, MPI_SEEK_SET);
  if (rc != 0){
    sprintf(errormsg, "Failed to fseek file: %s offset: %ld", dest_file, offset);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  while (completed != length){
    //1 MB is too big
    if ((length - completed) < blocksize){
      blocksize = length - completed;
    }

    
    rc = MPI_File_read(src_fd, buf, blocksize, MPI_BYTE, &status);
    if (rc != 0){
      sprintf(errormsg, "Failed to fread file: %s", dest_file);
      errsend(NONFATAL, errormsg);
      return -1;
    }

    rc = MPI_File_write(dest_fd, buf, blocksize, MPI_BYTE, &status );
    if (rc != 0){
      sprintf(errormsg, "Failed to write file: %s", dest_file);
      errsend(NONFATAL, errormsg);
      return -1;
    }

    completed += blocksize;
  }


  MPI_File_close(&dest_fd);
  MPI_File_close(&src_fd);
  
  /*rc = fclose(src_fd);
  if (rc != 0){
    sprintf(errormsg, "Failed to close file: %s", src_file);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  rc = fclose(dest_fd);
  if (rc != 0){
    sprintf(errormsg, "Failed to close file: %s", dest_file);
    errsend(NONFATAL, errormsg);
    return -1;
  }*/

  rc = chown(dest_file, src_st.st_uid, src_st.st_gid);
  if (rc != 0){
    sprintf(errormsg, "Failed to change ownership of file: %s to %d:%d", dest_file, src_st.st_uid, src_st.st_gid);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  mode = src_st.st_mode & 07777;
  rc = chmod(dest_file, mode);
  if (rc != 0){
    sprintf(errormsg, "Failed to chmod file: %s to %o", dest_file, mode);
    errsend(NONFATAL, errormsg);
    return -1;
  }

  ut.actime = src_st.st_atime;
  ut.modtime = src_st.st_mtime;
  rc = utime(dest_file, &ut);
  if (rc != 0){
    sprintf(errormsg, "Failed to set atime and mtime for file: %s", dest_file);
    errsend(NONFATAL, errormsg);
  }

  free(buf);
  return 0;
}

//local functions only
void send_command(int target_rank, int type_cmd){
  //Tell a rank it's time to begin processing
  if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}

void send_path_list(int target_rank, int command, int num_send, path_list **list_head, path_list **list_tail, int *list_count){
  int path_count = 0, position = 0;
  int worksize, workcount;


  if (num_send <= *list_count){
    workcount = num_send;
  }
  else{
    workcount = *list_count;
  }

  worksize = workcount * sizeof(path_item);
  char *workbuf = (char *) malloc(worksize * sizeof(char));

  while(path_count < workcount){
    path_count++;
    MPI_Pack(&(*list_head)->data, sizeof(path_item), MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
    dequeue_node(list_head, list_tail, list_count);
  }
  //send the command to get started
  send_command(target_rank, command);

  //send the # of paths
  if (MPI_Send(&workcount, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  free(workbuf);
}

void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count){
  int i;
  int position = 0;
  int worksize;
  char *workbuf;
  path_item work_node;

  if (*buffer_count > MESSAGEBUFFER){
    errsend(FATAL, "send_path_buffer: buffer_count is incorrectly > MESSAGEBUFFER\n");
  }
  worksize = *buffer_count * sizeof(path_item);
  workbuf = (char *) malloc(worksize * sizeof(char)); 
  
  for (i = 0; i < *buffer_count; i++){
    work_node = buffer[i];
    MPI_Pack(&work_node, sizeof(path_item), MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
  }

  
  send_command(target_rank, command);
  
  if (MPI_Send(buffer_count, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  *buffer_count = 0;
  free(workbuf);
}

void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, int *workbufsize){
  int size = (*workbuflist)->size;

  int worksize = sizeof(path_item) * size;
  send_command(target_rank, command);

  if (MPI_Send(&size, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  if (MPI_Send((*workbuflist)->buf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  dequeue_buf_list(workbuflist, workbufsize);
}

//manager
void send_manager_nonfatal_inc(){
  send_command(MANAGER_PROC, NONFATALINCCMD);
}

void send_manager_copy_stats(int num_copied_files, int num_copied_bytes){
  send_command(MANAGER_PROC, COPYSTATSCMD);
  
  //send the # of paths
  if (MPI_Send(&num_copied_files, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }

  //send the # of paths
  if (MPI_Send(&num_copied_bytes, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
    MPI_Abort(MPI_COMM_WORLD, -1); 
  }
}


void send_manager_regs_buffer(path_item *buffer, int *buffer_count){
  //sends a chunk of regular files to the manager
  send_path_buffer(MANAGER_PROC, REGULARCMD, buffer, buffer_count);
}

void send_manager_dirs_buffer(path_item *buffer, int *buffer_count){
  //sends a chunk of regular files to the manager
  send_path_buffer(MANAGER_PROC, DIRCMD, buffer, buffer_count);
}

void send_manager_tape_buffer(path_item *buffer, int *buffer_count){
  //sends a chunk of regular files to the manager
  send_path_buffer(MANAGER_PROC, TAPECMD, buffer, buffer_count);
}

void send_manager_new_buffer(path_item *buffer, int *buffer_count){
  //send manager new inputs
  send_path_buffer(MANAGER_PROC, INPUTCMD, buffer, buffer_count);
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

void send_worker_stat_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize){
  //send a worker a list buffers with paths to stat
  send_buffer_list(target_rank, NAMECMD, workbuflist, workbufsize);
}

void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, int *workbufsize){
  //send a worker a buffer list of paths to stat
  send_buffer_list(target_rank, DIRCMD, workbuflist, workbufsize);
}

void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize){
  //send a worker a list buffers with paths to copy 
  send_buffer_list(target_rank, COPYCMD, workbuflist, workbufsize);
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
void enqueue_path(path_list **head, path_list **tail, char *path, int *count){
  //stick a path on the end of the queue
  path_list *new_node = malloc(sizeof(path_list));
  strncpy(new_node->data.path, path, PATHSIZE_PLUS);  
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


void print_queue_path(path_list *head){
    //print the entire queue
    while(head != NULL){
      printf("%s\n", head->data.path);
      head = head->next;
    }
}

void delete_queue_path(path_list **head, int *count){
  //delete the entire queue;
  path_list *temp = *head;
  while(temp){
    *head = (*head)->next;
    free(temp);
    temp = *head;
  }
  *count = 0;
}

void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count){
  //enqueue a node using an existing node (does a new allocate, but allows us to pass nodes instead of paths)
  path_list *temp_node = malloc(sizeof(path_list));
  temp_node->data = new_node->data;
  temp_node->next = NULL;

  if (*head == NULL){
    *head = temp_node;
    *tail = *head;
  }
  else{
    (*tail)->next = temp_node;
    *tail = (*tail)->next;
  }
  *count += 1;
}


void dequeue_node(path_list **head, path_list **tail, int *count){
  //remove a path from the front of the queue
  path_list *temp_node = *head;
  if (temp_node == NULL){
    return;
  }
  *head = temp_node->next;
  free(temp_node);
  *count -= 1;
}


void enqueue_buf_list(work_buf_list **workbuflist, int *workbufsize, char *buffer, int buffer_size){
  work_buf_list *current_pos = *workbuflist;
  work_buf_list *new_buf_item = malloc(sizeof(work_buf_list));

  if (*workbufsize < 0){
    *workbufsize = 0;
  }

  new_buf_item->buf = buffer;
  new_buf_item->size = buffer_size;
  new_buf_item->next = NULL;  


  if (current_pos == NULL){
    *workbuflist = new_buf_item;
    (*workbufsize)++;
    return;
  }

  
  while (current_pos->next != NULL){
    current_pos = current_pos->next;
  }
  current_pos->next = new_buf_item;
  
  (*workbufsize)++;

}

void dequeue_buf_list(work_buf_list **workbuflist, int *workbufsize){
  work_buf_list *current_pos;
  
  if (*workbuflist == NULL){
    return;
  }

  current_pos = (*workbuflist)->next;
  free((*workbuflist)->buf);
  free(*workbuflist);
  *workbuflist = current_pos;

  (*workbufsize)--;

}

void delete_buf_list(work_buf_list **workbuflist, int *workbufsize){
  
  while (*workbuflist){
    dequeue_buf_list(workbuflist, workbufsize);
  }
  *workbufsize = 0;
}

void pack_list(path_list *head, int count, work_buf_list **workbuflist, int *workbufsize){
    path_list *iter = head;
    int position;
  
    char *buffer;
    int buffer_size = 0;
    int worksize;    

    
    worksize = sizeof(path_item) * MESSAGEBUFFER;
    buffer = (char *)malloc(worksize);
  
    position = 0;
    while (iter != NULL){
      MPI_Pack(&iter->data, sizeof(path_item), MPI_CHAR, buffer, worksize, &position, MPI_COMM_WORLD);
      iter = iter->next;
      buffer_size++;
      if (buffer_size % MESSAGEBUFFER == 0){
        enqueue_buf_list(workbuflist, workbufsize, buffer, buffer_size);
        buffer_size = 0;
        buffer = (char *)malloc(worksize);
      }
    }
    enqueue_buf_list(workbuflist, workbufsize, buffer, buffer_size);
}
