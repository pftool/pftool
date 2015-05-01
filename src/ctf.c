/*
* Function that implement and support Chunk Transfer Files. 
* These files are used when transferring large files, and
* contain meta data regarding the transfer and its progress.
* They can be used when attempting to restart a transfer
* of large, chunckable file.
*/

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "pfutils.h"
#include "str.h"
#include "ctf.h"

char *CTFDir = (char *)NULL;				// private global that holds the name of the user's Chunk Transfer File (CTF) directory

//
// CTF ROUTINES ...
//

/**
* This function returns the directory in which CTF files
* should be constructed. If the locally global variable
* CTFDir is set, then it is returned. Otherwise, the
* value is generated from the environment, and the directory
* is created, if it does not exist.
*
* Note that this function is NOT reenterant. A mutex would have
* to be used to make sure that one and only one thread accessed
* CTFDir at a time.
*
* @return the directory to create CTF files in. NULL will
* 	be returned if CTFDir is NOT set properly
*/
char *getCTFDir() {
	if(!CTFDir) {					// if the CTFDir has not been initialized - do it now
	  struct stat sbuf;				// buffer to hold stat information

	  CTFDir = (char *)malloc(PATH_MAX +1);
	  sprintf(CTFDir,"%s/%s", getenv("HOME"),CTF_DEFAULT_DIRECTORY);
	  if( stat(CTFDir,&sbuf) < 0) {			// directory probably does not exist
	    if(errno == ENOENT) {
	      if(mkpath(CTFDir,S_IRWXU)) {		// problems creating the directory
	        free(CTFDir); CTFDir = (char *)NULL;
	      }
	    }
	    else {					// we have other problems with this directory!
	      free(CTFDir); CTFDir = (char *)NULL;
	    }
	  }
	  else if (!S_ISDIR(sbuf.st_mode)) {		// path is in fact NOT a directory!
	    free(CTFDir); CTFDir = (char *)NULL;
	  }
	}
	return(CTFDir);
}

/**
* This function generates the CTF file name, based on the 
* transfer file name. The transfer file name should be an
* absolute path.
*
* @param transfilename	the name of the file to transfer
*
* @return the MD5 chunk Transfer File file name. NULL is
* 	returned if there are problems generating the 
* 	name.
*/
char *_genCTFFilename(const char *transfilename) {
	char *name = (char *)NULL;			// the generated name
	char *ctfdir = getCTFDir();			// holds the CTF directory (where CTF files are gnerated)
	char tmpname[PATH_MAX+1];			// temporary name buffer
	char *md5fname;					// MD5 version of transfer file

	if(!transfilename || !ctfdir)  return(name);	// if no filename -> nothing to do. (should be strIsBlank()) or problems retrieving directory

	name = (char *)malloc(PATH_MAX+1);
	md5fname = str2md5(transfilename);
	sprintf(tmpname, "%s/%s", ctfdir, md5fname);
	if(md5fname) free(md5fname);			// we are done with this name ... clean up memory

	return(name=strdup(tmpname));
}

/**
* Function to indicate if a CTF structure has an
* allocated file name.
* 
* @param ctfptr		pointer to a CTF structure to destroy
*
* @return non-zero (TRUE) id structure has a filename
* 	pointer allocated.
*/
int _hasCTFFile(CTF *ctfptr) {
	return((int)(ctfptr && ctfptr->chnkfname != (char*)NULL));
//	return(ctfptr && !strIsBlank(ctfptr->chnkfname));// essentally a wrapper for strIsBlank()
}

/**
* Routine to set the file name for a CTF structure.
*
* @param ctfptr		pointer to a CTF structure to destroy
* @param fname		the MD5 file name for the CTF structure.
* 			See _genCTFFilename().
*/
void _setCTFFile(CTF *ctfptr, const char *fname) {
//	if(ctfptr && !strIsBlank(fname)) {		// the is an allocated structure and a name to assign
	if(ctfptr && fname) {
	  if(_hasCTFFile(ctfptr)) free(ctfptr->chnkfname);
	  ctfptr->chnkfname = strdup(fname);
	}
}

/**
* Allocates a new flag array for s CTF 
* structure.
*
* @param ctfptr		pointer to a CTF structure to modify
* @param numchnks	the number of chunks to transfer.
* 			This is the length of the chnkflags
* 			array.
*/
void _allocateCTFFlags(CTF *ctfptr,long numchnks) {
	long ba_size = ComputeBitArraySize(numchnks);	// this is the size of the bit array to allocate

	if(ctfptr) {
	  ctfptr->chnkflags = (unsigned long *)malloc(sizeof(unsigned long)*ba_size);
	  ctfptr->chnknum = numchnks;
	  memset(ctfptr->chnkflags,0,(sizeof(unsigned char)*ba_size));
	}

	return;
	
}

/**
* This function returns a newly allocated CTF structure
*
* @param transfilename	the name of the file to transfer
* @param numchnks	the number of chunks to transfer.
* 			This is the length of the chnkflags
* 			array.
* @param sizechnks	the size of the chunks for this 
* 			file. Should NOT be zero!
*
* @return an empty CTF structure. A NULL pointer is returned
* 	if there are problems allocating the structure.
*/
CTF *_createCTF(const char *transfilename, long numchnks, size_t sizechnks) {
	CTF *new = (CTF *)NULL;				// pointer to the new structure

	if(numchnks > 0L) {				// if there are chunks to transfer ....
	  new = (CTF *)malloc(sizeof(CTF));

	  new->chnkfname = _genCTFFilename(transfilename);
	  new->chnksz = sizechnks;
	  _allocateCTFFlags(new,numchnks);
	}
	return(new);
}

/**
* This function frees the memory used by a CTF structure. It also
* sets the pointer to NULL;
*
* @param pctfptr	pointer to a CTF structure pointer 
* 			to destroy. It is an OUT parameter.
*/
void _destroyCTF(CTF **pctfptr) {
	CTF *ctfptr = (*pctfptr);

	if(ctfptr) {
	  if(ctfptr->chnkflags) free(ctfptr->chnkflags);
	  if(_hasCTFFile(ctfptr)) free(ctfptr->chnkfname);
	  free(ctfptr);
	  ctfptr = (CTF*)NULL;
	}
	return;
}

/**
* Low-level function to write a CTF structure using
* a file descriptor.
*
* @param ctfptr	the structure to write
*
* @return number of bytes written, If return
* 	is < 0, then there were problems writing,
* 	and the number can be taken as the errno.
*/
ssize_t _writeCTF(int fd, CTF *ctfptr) {
	size_t n;					// number of bytes written
	ssize_t tot = 0;				// total number of bytes written
	size_t ckflags_size = ((ctfptr->chnknum)*sizeof(unsigned char));

		// Write out the chunk count
	if((n = write_field(fd,&ctfptr->chnknum,sizeof(ctfptr->chnknum))) < 0)
	  return(n);
	tot += n;
		// Write out the chunk size
	if((n = write_field(fd,&ctfptr->chnksz,sizeof(ctfptr->chnksz))) < 0)
	  return(n);
	tot += n;

		// Write out the flags
	if((n = write_field(fd,(void *)ctfptr->chnkflags,(GetBitArraySize(ctfptr)*sizeof(unsigned long)))) < 0)
	  return(n);
	tot += n;

	return(tot);
}

/**
* Low level function to read from the file descriptor into
* a CTF structure. Note that this function may allocate and
* return a populated CTF structure.
*
* @param fd		the file descriptor to read from
* @param pcftptr	pointer to a CTF structure pointer. 
* 			This can be an OUT parameter
*
* @return number of bytes read, If return
* 	is < 0, then there were problems writing,
* 	and the number can be taken as the errno.
*/
ssize_t _readCTF(int fd, CTF **pctfptr) {
	long cknum;					// holds the number of chunks from file
	long ba_size;					// size of the bit array for the chunk flags
	size_t cksz;					// holds the chunk size of the transfer
	ssize_t n;					// current bytes read
	ssize_t tot = (ssize_t)0;			// total bytes read

	if((n=read(fd,&cknum,sizeof(long))) <= 0)	// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;
	if(!cknum) return((ssize_t)(-EINVAL));		// read zero chunks for file -> invalid 

	if((n=read(fd,&cksz,sizeof(size_t))) <= 0)	// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;
	if(!cknum) return((ssize_t)(-EINVAL));		// read zero chunks for file -> invalid 

	if(*pctfptr) {					// structure already exists
	  (*pctfptr)->chnksz = cksz;			// assign chunk size from value in file
	  if(cknum != (*pctfptr)->chnknum) {		// ... and number of chunks differ -> reallocated flags
	    if((*pctfptr)->chnkflags) free((*pctfptr)->chnkflags);
	    _allocateCTFFlags((*pctfptr),cknum);	// resize CTF flag array
	  }
	}
	else
	  *pctfptr = _createCTF((char *)NULL,cknum,cksz);	// Now we can allocate the CTF structure
	ba_size = ComputeBitArraySize(cknum);			// cknum holds actual number of chunks -> compute actual array length

	if((n=read(fd,(*pctfptr)->chnkflags,(ba_size*sizeof(unsigned long)))) <= 0)	// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;

	return(tot);
}

/**
* Function to indicate if the CTF file associated with
* a CTF structure actually exists in the filesystem. As
* this function returns an allocated pointer, if the file
* exists, then the returned pointer needs to be dealt with!
*
* @param transfilename	the name of the file to transfer
*
* @return a populated stat structure is returned if the
* 	file exists. Otherwise a NULL pointer is returned
* 	if not. Note that this function's return code
* 	is directly opposite of the system's stat().
*/
struct stat *foundCTFFile(const char *transfilename) {
	struct stat *sbuf = (struct stat*)NULL;		// the returned stat buffer
	char *ctffname;					// the CTF file path

	if(!(ctffname=_genCTFFilename(transfilename)))	// Build CTF file name. If no name generated -> no file
	  return(sbuf);

	sbuf = (struct stat *)malloc(sizeof(struct stat));
	if(stat(ctffname,sbuf)) {			// file does NOT exist.
	  free(sbuf);					// free the stat buffer
	  sbuf = (struct stat*)NULL;			// clear the pointer
	}
	return(sbuf);
}

/**
* This function returns a pointer to a CTF
* (Chunk Transfer File) structure. This file/structure 
* holds infromation as to what chunks of a file have 
* been transferred.
*
* Note that this function manages an actual file, so
* that the information about what chunks need to be
* transferred. can be maintained through transfer
* failures.
*
* If the underlying 
*
* @param transfilename	the name of the file to transfer
* @param numchunks	a parameter that is looked at
* 			in the case of a new transfer.
* @param chunksize	a parameter that is used in
* 			the case of a new transfer
*
* @return a point to a CTF structure. If there are errors,
* 	a NULL pointer is returned.
*/
CTF *getCTF(const char *transfilename, long numchunks, size_t chunksize) {
	char *ctffname;					// the CTF file path
	struct stat sbuf;				// holds stat info
	CTF *ctfptr = (CTF *)NULL;			// the CTF pointer to return
	int ctffd;					// file descriptor of CTF file
	
	if(!(ctffname=_genCTFFilename(transfilename)))	// Build CTF file name. If no name generated -> exit
	  return(ctfptr);

		// Manage CTF file
	if(stat(ctffname,&sbuf)) {			// file does NOT exist.
	  if((ctffd = creat(ctffname,S_IRWXU)) < 0) {	// if error on create ...
	    free(ctffname);				// ... clean up memory
	    return(ctfptr);
	  }
	  ctfptr = _createCTF(transfilename,numchunks,chunksize);	// initialize a new structure with given # of chunks
	  if(_writeCTF(ctffd,ctfptr) < (ssize_t)0) 	// if error on write ...
	    _destroyCTF(&ctfptr);			// ... clean up memory
	}
	else {						// file exists -> read it to populate CTF structure
	  if((ctffd = open(ctffname,O_RDONLY)) < 0) {	// if error on open ...
	    free(ctffname);				// ... clean up memory
	    return(ctfptr);
	  }
	  if(_readCTF(ctffd,&ctfptr) < 0) { 		// if error on read ...
	    if(ctfptr) _destroyCTF(&ctfptr);		// ... clean up memory, if allocated
	  }
	  _setCTFFile(ctfptr,ctffname);			// make sure file name is set
	}

	close(ctffd);					// close file if open
	free(ctffname);					// done with CTF file name
	return(ctfptr);
}

/**
* This function stores a CTF structure into a CTF file.
*
* @param transfilename	the name of the file to transfer
* @param cftptr		pointer to a CTF structure to 
* 			store. 
*
* @return 0 if there are no problems store the structure
* 	information into a CTF file. Otherwise a number corresponding
* 	to errno is returned. -1 is returned if ctffname
* 	cannot be generated.
*/
int putCTF(const char *transfilename, CTF *ctfptr) {
	char *ctffname;					// the CTF file path
	struct stat sbuf;				// holds stat info
	int ctffd;					// file descriptor of CTF file
	int rc = 0;					// return code for function
	int n;						// number of bytes written to CTF file

	if(!ctfptr) return(EINVAL);			// Nothing to write, because there is no structure!

	if(_hasCTFFile(ctfptr)) 			// If we already know the name -> use it
	  ctffname = strdup(ctfptr->chnkfname);		// ... a copy of the name allows for the uniform handling of the pointer
	else if(!(ctffname=_genCTFFilename(transfilename)))	// Build CTF file name. If no name generated -> exit
	  return(-1);
 
		// Manage CTF file
	if(stat(ctffname,&sbuf)) {			// file does NOT exist.
	  if((ctffd = creat(ctffname,S_IRWXU)) < 0) 	// if error on create ...
	    rc = errno;					// ... save off errno 
	}
	else {						// file exists -> read it to populate CTF structure
	  if((ctffd = open(ctffname,O_WRONLY)) < 0) 	// if error on open ...
	    rc = errno;					// ... save off errno
	}

	if(!rc) {					// opened or created without errors ...
	  if((n = (int)_writeCTF(ctffd,ctfptr)) < 0)	// write the structure to the file failed
	    rc = errno;					// ... save off errno
	}

	if(ctffd >= 0) close(ctffd);			// close file if open
	free(ctffname);					// done with CTF file name
	return(rc);
}
 
/**
* This removes a CTF file and destroys or frees a CTF
* structure.
*
* @param cftptr		pointer to a CTF structure to 
* 			remove. 
*
* @return 0 if file was removed. otherwise a number
* 	 corresponding to errno is returned.
*/
int removeCTF(CTF *ctfptr) {
	int rc = 0;					// return code for function

	if(_hasCTFFile(ctfptr)) 			// if a file name has been allocated, assume file exists,
	  rc = unlink(ctfptr->chnkfname);		// and unlink it
	_destroyCTF(&ctfptr);
	return(rc);
}

/**
* This routine sets the flag for a given
* chunk index to true.
*
* @param cftptr		pointer to a CTF structure to 
* 			set.
* @param chnkidx	index of the chunk flag
*/
void setCTF(CTF *ctfptr, long chnkidx) {
	if(ctfptr)
	  SetBit(ctfptr->chnkflags,chnkidx);
	return;
}

/**
* This function tests the chuck flags of
* a CTF structure. If they are all set
* (i.e. set to 1), then TRUE is returned.
*
* @param cftptr		pointer to a CTF structure
* 			to test
*
* @return TRUE (i.e. non-zero) if all chunk flags
* 	are set.Otherwise FALSE (i.e. zero) is returned.
*/
int transferredCTF(CTF *ctfptr) {
	int rc = 0;					// return code
	int i = 0;					// index into 

	if(ctfptr) {
	  while(i < ctfptr->chnknum && TestBit(ctfptr->chnkflags,i)) i++;
	  rc = (int)(i >= ctfptr->chnknum);
	}
	return(rc);
}

