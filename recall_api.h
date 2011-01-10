#ifndef      __RECALLAPI_H
#define      __RECALLAPI_H


#define PATHSIZE_PLUS FILENAME_MAX+30

void set_base_dir(const char *base_dir);
int create_recall_dir(char *dest_path);
int open_rank_file(FILE **fp, const char *dest_path, const char *rank_name);
int write_rank_file(FILE *fp, const char *line, int newline);
int write_array_rank_file(FILE *fp, char * const *lines, int lines_length);
int close_rank_file(FILE **fp, const char *dest_path, const char *rank_name);
void rank_finished(const char *dest_path);
int return_first_result(FILE **fp, const char *dest_path, char *result_path);
int is_finished_recalling(const char *dest_path);
int get_result_data(FILE *fp, char **data);
void result_finished(FILE **fp, const char *result_path);


#endif
