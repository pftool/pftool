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


//
// Defines for the CTM (Chunk Transfer Metadata) structure
//

#ifndef      __CTM_H
#define      __CTM_H

#include "config.h"
#include <attr/xattr.h>

#include <asm/bitsperlong.h>
#ifndef BITS_PER_LONG
#  define BITS_PER_LONG __BITS_PER_LONG
#endif

// configuration (via config.h) will define CTM_MODE to one of these values.
// Use './configure --enable-ctm=value', where <value> is { xattrs | files | no }
// The default is CTM_PREFER_FILES.
#define CTM_PREFER_NONE   0
#define CTM_PREFER_XATTRS 1
#define CTM_PREFER_FILES  2

// Typedefs that define the functions to access Chunk Transfer Metadata (CTM) from a persistant store
typedef struct ctm_struct CTM;				// declaration of the CTM type. Defined below
typedef struct ctm_impl CTM_IMPL;			// declaration of the CTM_IMPL type. Defined below
typedef int (*ctm_read_fn_t)(CTM *ctmptr, long numchunks, size_t chunksize);
typedef int (*ctm_write_fn_t)(CTM *ctmptr);
typedef int (*ctm_delete_fn_t)(const char *chnkfname);

// different methods in the way chunk metadata is stored and accessed
enum ctm_impltype {
	CTM_NONE,					// no CTM routines/functions. Typically means there is no file to transfer
	CTM_FILE,					// use file-based routines/functions to manage chunk metadata
	CTM_XATTR,					// user xattr routines/functions to manage chunk metadata
	CTM_UNKNOWN					// don't know how to access the chunk metadata
};
typedef enum ctm_impltype CTM_ITYPE;

// The structure that contains the pointer-to-functions that access Chunk Transfer Metadata (CTM) from a persistant store
struct ctm_impl {
	ctm_read_fn_t read;				// the CTM read() pointer. Reads a persistent CTM store
	ctm_write_fn_t write;				// the CTM write() pointer. Writes to a persistent CTM store
	ctm_delete_fn_t del;				// the CTM delete() pointer. Deletes or removes data from the persistent CTM store
};

// The structure to manage information of transferred chunks of a file
struct ctm_struct {
	CTM_ITYPE chnkimpl;				// indicates the method chunk metadata is stored and accessed
	int chnkstore;					// a flag or counter, depending on the persistent store implementation of the metadata
	char *chnkfname;				// path/name to the transferring file or the chunk file (hashed name), depending on implementation
	long chnknum;					// number of chunks to transfer
	size_t chnksz;					// size of the chunk for this file during a transfer
	unsigned long *chnkflags;			// a bit array of longs (64 bit), which indicate if a chunk has been transferred or not
	CTM_IMPL impl;					// structure holding the function pointers for this CTM storage implementation
};

// Bit Array macros
#define ComputeBitArraySize(N)	( (((N)/BITS_PER_LONG)+1L) )
#define GetBitArraySize(F)	( (ComputeBitArraySize((F)->chnknum)) )
#define SizeofBitArray(F)	( (size_t)(GetBitArraySize(F)*sizeof(unsigned long)) )
#define SetBit(A,k)     ( A[(k/BITS_PER_LONG)] |= (1L << (k%BITS_PER_LONG)) )
#define ClearBit(A,k)   ( A[(k/BITS_PER_LONG)] &= ~(1L << (k%BITS_PER_LONG)) )            
#define TestBit(A,k)    (( A[(k/BITS_PER_LONG)] & (1L << (k%BITS_PER_LONG)) ) != 0)

// Function Declarations
int chunktransferredCTM(CTM *ctmptr,int idx);
void freeCTM(CTM **pctmptr);
int putCTM(CTM *ctmptr);
void setCTM(CTM *ctmptr, long chnkidx);
char *tostringCTM(CTM *ctmptr, char **rbuf, int *rlen);
int transferredCTM(CTM *ctmptr);
int check_ctm_match(const char* filename, const char* src_to_hash);

CTM *getCTM(const char *transfilename, long numchunks, size_t chunksize);
int updateCTM(CTM *ctmptr, long chnkidx);
int removeCTM(CTM **pctmptr);
int hasCTM(const char *transfilename);
void purgeCTM(const char *transfilename);
size_t allocateCTMFlags(CTM *ctmptr);

#endif //__CTM_H
