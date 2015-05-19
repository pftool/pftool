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

#include "hashtbl.h"

#include <string.h>
#include <stdio.h>

static char *mystrdup(const char *s) {
    char *b;
    if(!(b=malloc(strlen(s)+1))) {
        return NULL;
    }
    strcpy(b, s);
    return b;
}

/**
* Defines the hash function for this hash table.
* It simply adds up values of the characters in the
* key string. This is a default function if none
* is given when allocating a hash table.
*
* @param key	the key value to hash
*
* @return the hash value of the key
*/
static hash_size def_hashfunc(const char *key) {
    hash_size hash=0;
    while(*key) {
        hash+=(unsigned char)*key++;
    }
    return hash;
}

/**
* Allocates a hash table and assiigns the hash function.
*
* @param size		how big to make the table (how many buckets!)
* @param hashfunc	the hash function for the table. If null,
* 			then the def_hashfunc() is used.
*
* @return the allocated hash table. NULL is returned if there are
* 	problems.
*/
HASHTBL *hashtbl_create(hash_size size, hash_size (*hashfunc)(const char *)) {
    HASHTBL *hashtbl;
    if(!(hashtbl=malloc(sizeof(HASHTBL)))) {
        return NULL;
    }
    if(!(hashtbl->nodes=calloc(size, sizeof(struct hashnode_s *)))) {
        free(hashtbl);
        return NULL;
    }
    hashtbl->size=size;
    if(hashfunc) {
        hashtbl->hashfunc=hashfunc;
    }
    else {
        hashtbl->hashfunc=def_hashfunc;
    }
    return hashtbl;
}

void hashtbl_destroy(HASHTBL *hashtbl) {
    hash_size n;
    struct hashnode_s *node, *oldnode;
    for(n=0; n<hashtbl->size; ++n) {
        node=hashtbl->nodes[n];
        while(node) {
            free(node->key);
            oldnode=node;
            node=node->next;
            free(oldnode);
        }
    }
    free(hashtbl->nodes);
    free(hashtbl);
}

int hashtbl_insert(HASHTBL *hashtbl, const char *key, HASHDATA *data) {
    struct hashnode_s *node;
    hash_size hash=hashtbl->hashfunc(key)%hashtbl->size;
    /*  fprintf(stderr, "hashtbl_insert() key=%s, hash=%d, data=%s\n", key, hash, (char*)data);*/
    node=hashtbl->nodes[hash];
    while(node) {
        if(!strcmp(node->key, key)) {
            node->data=data;
            return 0;
        }
        node=node->next;
    }
    if(!(node=malloc(sizeof(struct hashnode_s)))) {
        return -1;
    }
    if(!(node->key=mystrdup(key))) {
        free(node);
        return -1;
    }
    node->data=data;
    node->next=hashtbl->nodes[hash];
    hashtbl->nodes[hash]=node;
    return 0;
}

/**
* Remove data associated with a key from the hash table.
* Note that a pointer to the actual data being removed from
* table is returned. This allows the calling routine to deal
* appropriately with the data. 
*
* @param hashtbl	the hash table to modify
* @param key		the key of the data to remove
*
* @return a non-NULL pointer to the data being removed.
* 	NULL is returned if the data was NOT successfully
* 	removed.
*/
HASHDATA *hashtbl_remove(HASHTBL *hashtbl, const char *key) {
    struct hashnode_s *node, *prevnode=NULL;
    hash_size hash=hashtbl->hashfunc(key)%hashtbl->size;
    HASHDATA *data;						// data to remove (and return)

    node=hashtbl->nodes[hash];
    while(node) {
        if(!strcmp(node->key, key)) {
            free(node->key);
            if(prevnode) {
                prevnode->next=node->next;
            }
            else {
                hashtbl->nodes[hash]=node->next;
            }
	    data = node->data;
            free(node);
            return(data);
        }
        prevnode=node;
        node=node->next;
    }
    return((HASHDATA *)NULL);
}

/**
* Returns a pointer to the data for the given key.
*
* @param hashtbl	the hash table to search
* @param key		the key to the data to return
*
* @return the data associated with the key. NULL is 
* 	returned if the data is not found.
*/
HASHDATA *hashtbl_get(HASHTBL *hashtbl, const char *key) {
    struct hashnode_s *node;
    hash_size hash=hashtbl->hashfunc(key)%hashtbl->size;
    /*  fprintf(stderr, "hashtbl_get() key=%s, hash=%d\n", key, hash);*/
    node=hashtbl->nodes[hash];
    while(node) {
        if(!strcmp(node->key, key)) {
            return node->data;
        }
        node=node->next;
    }
    return((HASHDATA *)NULL);
}

int hashtbl_update(HASHTBL *hashtbl, const char *key, HASHDATA *data) {
    struct hashnode_s *node;
    hash_size hash=hashtbl->hashfunc(key)%hashtbl->size;
    /*  fprintf(stderr, "hashtbl_get() key=%s, hash=%d\n", key, hash);*/
    node=hashtbl->nodes[hash];
    while(node) {
        if(!strcmp(node->key, key)) {
            node->data = data;
            return 0;
        }
        node=node->next;
    }
    return -1;
}

int hashtbl_resize(HASHTBL *hashtbl, hash_size size) {
    HASHTBL newtbl;
    hash_size n;
    struct hashnode_s *node,*next;
    newtbl.size=size;
    newtbl.hashfunc=hashtbl->hashfunc;
    if(!(newtbl.nodes=calloc(size, sizeof(struct hashnode_s *)))) {
        return -1;
    }
    for(n=0; n<hashtbl->size; ++n) {
        for(node=hashtbl->nodes[n]; node; node=next) {
            next = node->next;
            hashtbl_insert(&newtbl, node->key, node->data);
            hashtbl_remove(hashtbl, node->key);
        }
    }
    free(hashtbl->nodes);
    hashtbl->size=newtbl.size;
    hashtbl->nodes=newtbl.nodes;
    return 0;
}


