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
#include "ctm.h"
#include "ctm_impl.h"					// holds implementation specific declarations


#define CTF_DEFAULT_DIRECTORY ".pftool/chunkfiles"	// the default directory where Chunk Transfer Files are created
#define CTF_UPDATE_STORE_LIMIT 3			// throttle for how often the CTF file is actually written when stored. 

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
char *_getCTFDir() {
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
* Low-level function to write a CTM structure using
* a file descriptor.
*
* @param fd	the file descriptor to write to
* @param ctmptr	the structure to write
*
* @return number of bytes written, If return
* 	is < 0, then there were problems writing,
* 	and the number can be taken as the errno.
*/
ssize_t _writeCTF(int fd, CTM *ctmptr) {
	size_t n;					// number of bytes written
	ssize_t tot = 0;				// total number of bytes written

		// Write out the chunk count
	if((n = write_field(fd,&ctmptr->chnknum,sizeof(ctmptr->chnknum))) < 0)
	  return(n);
	tot += n;
		// Write out the chunk size
	if((n = write_field(fd,&ctmptr->chnksz,sizeof(ctmptr->chnksz))) < 0)
	  return(n);
	tot += n;

		// Write out the flags
	if((n = write_field(fd,(void *)ctmptr->chnkflags,SizeofBitArray(ctmptr))) < 0)
	  return(n);
	tot += n;

	return(tot);
}

ssize_t _writeCTFv2(int fd, CTM *ctmptr)
{
        size_t n;                                       // number of bytes written
        ssize_t tot = 0;                                // total number of bytes written

	//first write src+mtime hash
	//if((n = write_field(fd, ctmptr->srcHash, SIG_DIGEST_LENGTH*2+1)) < 0)
	if(lseek(fd, SIG_DIGEST_LENGTH*2 + 1, SEEK_CUR) < 0)
	  return(-1);
	//tot += n;
	//if(fsync(fd) < 0)
	//	return -errno;

	//if((n = write_field(fd, ctmptr->timestamp, MARFS_DATE_STRING_MAX)) < 0)
	if(lseek(fd, MARFS_DATE_STRING_MAX, SEEK_CUR) < 0)
	  return(-1);
	//tot += n;
	//if(fsync(fd) < 0)
	//	return -errno;

                // Write out the chunk count
        if((n = write_field(fd,&ctmptr->chnknum,sizeof(ctmptr->chnknum))) < 0)
          return(n);
        tot += n;
	if(fsync(fd) < 0)
		return -errno;

                // Write out the chunk size
        if((n = write_field(fd,&ctmptr->chnksz,sizeof(ctmptr->chnksz))) < 0)
          return(n);
        tot += n;
	if(fsync(fd) < 0)
		return -errno;

                // Write out the flags
        if((n = write_field(fd,(void *)ctmptr->chnkflags,SizeofBitArray(ctmptr))) < 0)
          return(n);
        tot += n;
	if(fsync(fd) < 0)
		return -errno;

        return(tot);
}


/**
* Low level function to read from the file descriptor into
* a CTM structure. Note that this function may allocate and
* return a populated CTF structure.
*
* @param fd		the file descriptor to read from
* @param pctmptr	pointer to a CTM structure pointer. 
* 			This can be an OUT parameter
*
* @return number of bytes read, If return
* 	is < 0, then there were problems writing,
* 	and the number can be taken as the errno.
*/
ssize_t _readCTF(int fd, CTM **pctmptr) {
	long cknum;						// holds the number of chunks from file
	size_t cksz;						// holds the chunk size of the transfer
	size_t bufsz = (size_t)0;				// size of the flag buffer
	ssize_t n;						// current bytes read
	ssize_t tot = (ssize_t)0;				// total bytes read

	if((n=read(fd,&cknum,sizeof(long))) <= 0)		// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;
	if(!cknum) return((ssize_t)(-EINVAL));			// read zero chunks for file -> invalid 

	if((n=read(fd,&cksz,sizeof(size_t))) <= 0)		// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;
	if(!cknum) return((ssize_t)(-EINVAL));			// read zero chunks for file -> invalid 

	if(!(*pctmptr)) return((ssize_t)(-EINVAL));		// NULL structure passed in -> invalid

	(*pctmptr)->chnksz = cksz;				// assign chunk size from value in file
	if(cknum != (*pctmptr)->chnknum) {			// ... and number of chunks differ -> reallocate flag array
          (*pctmptr)->chnknum = cknum;				// assign chunk number from file
	  if((*pctmptr)->chnkflags) free((*pctmptr)->chnkflags);
	  bufsz = allocateCTMFlags((*pctmptr));			// resize CTM flag array
	}
	else if(!(*pctmptr)->chnkflags)				// flag array needs to be allocated
	  bufsz = allocateCTMFlags((*pctmptr));			// create CTM flag array

	if(bufsz < (ssize_t)0)					// problems allocating flags
	  return((ssize_t)bufsz);				// ... return error, which is negative
	else if(!bufsz)						// bufsz needs to be computed ...
	  bufsz = (size_t)(sizeof(unsigned long)*GetBitArraySize(*pctmptr));

	if((n=read(fd,(*pctmptr)->chnkflags,bufsz)) <= 0)	// if error on read ...
	  return((ssize_t)(-errno));
	tot += n;

	return(tot);
}

ssize_t _readCTFv2(int fd, CTM **pctmptr) {
        long cknum;                                             // holds the number of chunks from file
        size_t cksz;                                            // holds the chunk size of the transfer
        size_t bufsz = (size_t)0;                               // size of the flag buffer
        ssize_t n;                                              // current bytes read
        ssize_t tot = (ssize_t)0;                               // total bytes read
	char timestamp[MARFS_DATE_STRING_MAX];
	//skip hash
	if(lseek(fd, SIG_DIGEST_LENGTH*2+1, SEEK_CUR) < 0)
		return -errno;

	//skip timestamp
	if((n=read(fd, timestamp, MARFS_DATE_STRING_MAX)) < 0)
		return (ssize_t)-errno;
	tot += n;

        if((n=read(fd,&cknum,sizeof(long))) <= 0)               // if error on read ...
          return((ssize_t)(-errno));
        tot += n;
        if(!cknum) return((ssize_t)(-EINVAL));                  // read zero chunks for file -> invalid

        if((n=read(fd,&cksz,sizeof(size_t))) <= 0)              // if error on read ...
          return((ssize_t)(-errno));
        tot += n;
        if(!cknum) return((ssize_t)(-EINVAL));                  // read zero chunks for file -> invalid

        if(!(*pctmptr)) return((ssize_t)(-EINVAL));             // NULL structure passed in -> invalid

	//copy timestamp
	memcpy((*pctmptr)->timestamp, timestamp, MARFS_DATE_STRING_MAX);
	printf("read tiemtsmp %s\n", timestamp);
        (*pctmptr)->chnksz = cksz;                              // assign chunk size from value in file
        if(cknum != (*pctmptr)->chnknum) {                      // ... and number of chunks differ -> reallocate flag array
          (*pctmptr)->chnknum = cknum;                          // assign chunk number from file
          if((*pctmptr)->chnkflags) free((*pctmptr)->chnkflags);
          bufsz = allocateCTMFlags((*pctmptr));                 // resize CTM flag array
        }
        else if(!(*pctmptr)->chnkflags)                         // flag array needs to be allocated
          bufsz = allocateCTMFlags((*pctmptr));                 // create CTM flag array

        if(bufsz < (ssize_t)0)                                  // problems allocating flags
          return((ssize_t)bufsz);                               // ... return error, which is negative
        else if(!bufsz)                                         // bufsz needs to be computed ...
          bufsz = (size_t)(sizeof(unsigned long)*GetBitArraySize(*pctmptr));

        if((n=read(fd,(*pctmptr)->chnkflags,bufsz)) <= 0)       // if error on read ...
          return((ssize_t)(-errno));
        tot += n;

        return(tot);
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
char *genCTFFilename(const char *transfilename) {
	char *ctfdir = _getCTFDir();			// holds the CTF directory (where CTF files are generated)
	char tmpname[PATH_MAX+1];			// temporary name buffer
	char *md5fname;					// MD5 version of transfer file
	printf("CTFDIR %s\n", ctfdir);
	if(strIsBlank(transfilename) || !ctfdir)	// if no filename -> nothing to do ... or problems retrieving directory  
	  return((char *)NULL);	

	md5fname = str2sig(transfilename);
	sprintf(tmpname, "%s/%s", ctfdir, md5fname);
	if(md5fname) free(md5fname);			// we are done with this name ... clean up memory
	printf("md5fname %s\n", tmpname);
	return(strdup(tmpname));
}

/**
* Function to indicate if the CTF file associated with
* a file actually exists in the filesystem.
*
* @param transfilename	the name of the file to transfer
*
* @return TRUE if the CTF file exists. Otherwise FALSE.
*/
int foundCTF(const char *transfilename) {
	struct stat sbuf;				// the returned stat buffer
	char *ctffname;					// the CTF file path

	if(!(ctffname=genCTFFilename(transfilename)))	// Build CTF file name. If no name generated -> no file
	  return(FALSE);
	if(stat(ctffname,&sbuf)) 			// file does NOT exist.
	  return(FALSE);
	return(TRUE);
}

/**
* This function populates a CTM (Chunk Transfer Metadata) structure. 
* This structure holds infromation as to what chunks of a file have 
* been transferred.
*
* Note that this function manages an actual file, so that the 
* information about what chunks need to be * transferred. can be 
* maintained through transfer failures.
*
* @param ctmptr		pointer to a CTM structure to
* 			populate. It is an OUT parameter.
* @param numchunks	a parameter that is looked at
* 			in the case of a new transfer.
* @param chunksize	a parameter that is used in
* 			the case of a new transfer
*
* @return a positive number if the population of the structure is
* 	completed. Otherwise a negative result is returned.
*/
int populateCTF(CTM *ctmptr, long numchunks, size_t chunksize, const char* srcStr, const char* timestamp) {
	int ret;
	int syserr;							// holds any system errrno
	struct stat sbuf;						// holds stat info of md5 file
	int ctffd;							// file descriptor of CTF file

	memset(&sbuf, 0, sizeof(struct stat));
	if(!ctmptr || strIsBlank(ctmptr->chnkfname))			// make sure we have a valid structure
	  return(-1);

	stat(ctmptr->chnkfname, &sbuf);
	printf("CTM FILE SIZE %ld\n", sbuf.st_size);
	if (sbuf.st_size <= MARFS_DATE_STRING_MAX + SIG_DIGEST_LENGTH * 2 + 1)
	{
		//either CTM file does not exist, or the CTM is just a stub
		if(srcStr != NULL)
		{
			char* srcHash = str2sig(srcStr);
			memcpy(ctmptr->srcHash, srcHash, SIG_DIGEST_LENGTH * 2 + 1);	//get src hash into ctm struct
			free(srcHash);
		}
		
		if (timestamp != NULL)
		{
			memcpy(ctmptr->timestamp, timestamp, MARFS_DATE_STRING_MAX);	//get time stamp into ctm struct
		}

		ctmptr->chnknum = numchunks;	// now assign number of chunks to CTM structure
		ctmptr->chnksz = chunksize;	// assign chunk size to CTM structure
		if((syserr=(int)allocateCTMFlags(ctmptr)) <= 0)	// allocate the chunk flag bit array
			return(syserr);
	}
	else
	{
		if((ctffd = open(ctmptr->chnkfname,O_RDONLY)) < 0)	// file exists -> read it to populate CTF structure
			return(syserr = -errno);
		if((syserr=(int)_readCTFv2(ctffd,&ctmptr)) < 0)		// read ctm file
			return(syserr);
		close(ctffd);
	}

	return(1);
}

/**
* This function stores a CTM structure into a CTF file.
*
* @param ctmptr		pointer to a CTM structure to 
* 			store. 
*
* @return 0 if there are no problems store the structure
* 	information into a CTF file. Otherwise a number corresponding
* 	to errno is returned. It may be a negative value.
*/
int storeCTF(CTM *ctmptr) {
	struct stat sbuf;						// holds stat info
	int ctffd;							// file descriptor of CTF file
	int rc = 0;							// return code for function
	int n;								// number of bytes written to CTF file

	if(!ctmptr || strIsBlank(ctmptr->chnkfname)) 
	  return(EINVAL);						// Nothing to write, because there is no structure, or it is invalid!

	if(ctmptr->chnkstore < CTF_UPDATE_STORE_LIMIT) {		// this function has not been called enough times to cause a file to be written
	  ctmptr->chnkstore++;						// increment counter of calls
	  return(rc);
	}
		// Manage CTF file
	if(stat(ctmptr->chnkfname,&sbuf)) {				// file does NOT exist. We are testing the md5 filename (generated when allocating the CTM structure)
	  if((ctffd = creat(ctmptr->chnkfname,S_IRWXU)) < 0)		// if error on create ...
	    rc = errno;							// ... save off errno 
	}
	else {								// file exists -> open it to write CTM structure
	  if((ctffd = open(ctmptr->chnkfname,O_WRONLY)) < 0) 		// if error on open ...
	    rc = errno;							// ... save off errno
	}

	if(!rc) {							// opened or created without errors ...
	  if((n = (int)_writeCTFv2(ctffd,ctmptr)) < 0)			// write the structure to the file failed
	    rc = n;							// ... save off error from _writeCTF()
	}

	if(ctffd >= 0) close(ctffd);					// close file if open
	if(!rc) ctmptr->chnkstore = 0;					// re-initialize countier if there are no problems storing
	return(rc);
}
 
/**
* This removes a CTM file if one exists.
*
* @param chnkfname	name of chunk file to
* 			unlink. (the md5 name) 
*
* @return 0 if file was removed. otherwise a number
* 	 corresponding to errno is returned.
*/
int unlinkCTF(const char *chnkfname) {
	struct stat sbuf;						// holds stat info
	int rc = 0;							// return code for function

	if(strIsBlank(chnkfname)) 
	  return(EINVAL);						// Nothing to delete, because nofile name was given

	if(!stat(chnkfname,&sbuf)) {					// only unlink if the file exists
	  if(unlink(chnkfname) < 0)					//  ... check if there are errors
	    rc = errno;
	}
	return(rc);
}

/**
* This function assigns CTF functions to the given CTM_IMPL
* structure.
*
* @param ctmimplptr	pointer to a CTM_IMPL structure
* 			to update. This is an OUT parameter.
*/
void registerCTF(CTM_IMPL *ctmimplptr) {
	ctmimplptr->read = populateCTF;
	ctmimplptr->write = storeCTF;
	ctmimplptr->del = unlinkCTF;
	return;
}

