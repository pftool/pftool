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

static hash_size def_hashfunc(const char *key) {
    hash_size hash=0;
    while(*key) {
        hash+=(unsigned char)*key++;
    }
    return hash;
}


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

int hashtbl_insert(HASHTBL *hashtbl, const char *key, double data) {
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

int hashtbl_remove(HASHTBL *hashtbl, const char *key) {
    struct hashnode_s *node, *prevnode=NULL;
    hash_size hash=hashtbl->hashfunc(key)%hashtbl->size;
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
            free(node);
            return 0;
        }
        prevnode=node;
        node=node->next;
    }
    return -1;
}

double hashtbl_get(HASHTBL *hashtbl, const char *key) {
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
    return -1;
}

int hashtbl_update(HASHTBL *hashtbl, const char *key, double data) {
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


