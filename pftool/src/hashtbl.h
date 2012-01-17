#ifndef HASHTBL_H_INCLUDE_GUARD
#define HASHTBL_H_INCLUDE_GUARD

#include<stdlib.h>

typedef size_t hash_size;

typedef struct hashtbl {
  hash_size size;
  struct hashnode_s **nodes;
  hash_size (*hashfunc)(const char *);
} HASHTBL;

struct hashnode_s {
  char *key;
  double data;
  struct hashnode_s *next;
};

HASHTBL *hashtbl_create(hash_size size, hash_size (*hashfunc)(const char *));
void hashtbl_destroy(HASHTBL *hashtbl);
int hashtbl_insert(HASHTBL *hashtbl, const char *key, double data);
int hashtbl_remove(HASHTBL *hashtbl, const char *key);
int hashtbl_update(HASHTBL *hashtbl, const char *key, double data);
double hashtbl_get(HASHTBL *hashtbl, const char *key);
int hashtbl_resize(HASHTBL *hashtbl, hash_size size);

#endif

