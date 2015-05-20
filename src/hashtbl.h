/*
*This material was prepared by the Los Alamos National Security, LLC (LANS) under
*Contract DE-AC52-06NA25396 with the U.S. Department of Energy (DOE). All rights
*in the material are reserved by DOE on behalf of the Government and LANS
*pursuant to the contract. You are authorized to use the material for Government
*purposes but it is not to be released or distributed to the public. NEITHER THE
*UNITED STATES NOR THE UNITED STATES DEPARTMENT OF ENERGY, NOR THE LOS ALAMOS
*NATIONAL SECURITY, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY WARRANTY, EXPRESS
*OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR RESPONSIBILITY FOR THE ACCURACY,
*COMPLETENESS, OR USEFULNESS OF ANY INFORMATION, APPARATUS, PRODUCT, OR PROCESS
*DISCLOSED, OR REPRESENTS THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
*/

#ifndef HASHTBL_H_INCLUDE_GUARD
#define HASHTBL_H_INCLUDE_GUARD

#include<stdlib.h>

#include "hashdataCTM.h"						// defines the HASHDATA type

typedef size_t hash_size;						// type that holds the size of the hash table
typedef struct hashtbl {
    hash_size size;
    struct hashnode_s **nodes;
    hash_size (*hashfunc)(const char *);
} HASHTBL;

struct hashnode_s {
    char *key;
    HASHDATA *data;							// use a pointer to data to avoid copying data. As pftool is currently designed,
    struct hashnode_s *next;						// this table should be internal to only one process...
};

HASHTBL *hashtbl_create(hash_size size, hash_size (*hashfunc)(const char *));
void hashtbl_destroy(HASHTBL *hashtbl);
int hashtbl_insert(HASHTBL *hashtbl, const char *key, HASHDATA *data);
HASHDATA *hashtbl_remove(HASHTBL *hashtbl, const char *key);
int hashtbl_update(HASHTBL *hashtbl, const char *key, HASHDATA *data);	// update function is spmewhat useless when using pointers to the data - cds 4/2015
HASHDATA *hashtbl_get(HASHTBL *hashtbl, const char *key);
int hashtbl_resize(HASHTBL *hashtbl, hash_size size);

#endif

