//
// Defines for the CTF (Chunk Transfer File) structure
//

#ifndef      __CTF_H
#define      __CTF_H

#include <xfs/xfs.h>					// include because of BITS_PER_LONG define

#define CTF_DEFAULT_DIRECTORY ".pftool/chunkfiles"	// the default directory where Chunk Transfer Files are created

// The structure to manage information of transferred chunks of a file
struct ctf_struct {
	char *chnkfname;				// full path to the chunk file - this is a hashed name
	long chnknum;					// number of chunks to transfer
	size_t chnksz;					// size of the chunk for this file during a transfer
	unsigned long *chnkflags;			// a bit array of longs (64 bit), which indicate if a chunk has been transferred or not
};
typedef struct ctf_struct CTF;

// Bit Array macros
#define ComputeBitArraySize(N)	( (((N)/BITS_PER_LONG)+1L) )
#define GetBitArraySize(F)	( (ComputeBitArraySize((F)->chnknum)) )
#define SetBit(A,k)     ( A[(k/BITS_PER_LONG)] |= (1 << (k%BITS_PER_LONG)) )
#define ClearBit(A,k)   ( A[(k/BITS_PER_LONG)] &= ~(1 << (k%BITS_PER_LONG)) )            
#define TestBit(A,k)    ( A[(k/BITS_PER_LONG)] & (1 << (k%BITS_PER_LONG)) )

// Function Declarations
struct stat *foundCTFFile(const char *transfilename);
CTF *getCTF(const char *transfilename, long numchunks, size_t chunksize);
int putCTF(const char *transfilename, CTF *ctfptr);
int removeCTF(CTF *ctfptr);
void setCTF(CTF *ctfptr, long chnkidx);
int transferredCTF(CTF *ctfptr);

#endif //__CTF_H
