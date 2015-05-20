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
* Function that implement and support Chunk Transfer Attributes (CTA). 
* These extended file attributes (xattr) are used when transferring large 
* files, and contain meta data regarding the transfer and its progress.
* They can be used when attempting to restart a transfer of large, 
* chunckable file.
*
* Note that an alternative way to implement CTA is to use one xattr,
* rather than three, as I have used here. Using only one may reduce
* the transfer overhead, by reducing the initial number of xattr calls
* that are made. The implementation down side is having to parse and 
* otherwise manage the multiple pieces of information in a single xattr
* value. Given that CTM is only generated for large files, then overhead
* of xattr calls is very small compared to the overall time of the transfer
* itself. - cds 05/2015
*/

#include <errno.h>
#include <sys/types.h>
#include <sys/xattr.h>

#include "pfutils.h"
#include "str.h"
#include "ctm.h"
#include "ctm_impl.h"					// holds implementation specific declarations

#define CTA_CHNKNUM_XATTR "user.xfer.chknum"
#define CTA_CHNKSZ_XATTR  "user.xfer.chksz"
#define CTA_CHNKFLAGS_XATTR "user.xfer.chkflags"

//
// CTA ROUTINES ...
//

/**
* This function populates a CTM (Chunk Transfer Metadata) structure. 
* This structure holds infromation as to what chunks of a file have 
* been transferred.
*
* Note that this function manages extended attributes on the 
* transferred file, so that the information about what chunks need 
* to be transferred, can be maintained through transfer failures.
*
* @param ctmptr		pointer to a CTM structure to
* 			populate. It is an OUT parameter.
* @param numchunks	a parameter that is looked at
* 			in the case of a new transfer.
* @param chunksize	a parameter that is used in
* 			the case of a new transfer
*
* @return a positive number if the population of the structure is
* 	completed. Otherwise a negative result is returned. (-1)
* 	means that the ctmptr was invalid. (-ENOTSUP) means that
* 	the xattr entries could not be read.
*/
int populateCTA(CTM *ctmptr, long numchunks, size_t chunksize) {
	ssize_t axist;							// hold the size of the returned chunknum xattr. Acts as a flag
	long anumchunks;						// value of number of chunks from the xattr
	size_t achunksize;						// value of chunk size from the xattr
	size_t arrysz;							// the size of the chunk flag bit array buffer in bytes

	if(!ctmptr || strIsBlank(ctmptr->chnkfname))			// make sure we have a valid structure
	  return(-1);
									// if xattrs cannot be retieved ...
	if((axist = getxattr(ctmptr->chnkfname, CTA_CHNKNUM_XATTR, (void *)&anumchunks, sizeof(long))) < 0) {
	  int syserr = errno;						// preserve errno

	  if(syserr == ENOATTR) {					// no xattr for chnknum exists for file
	    anumchunks = numchunks;					// use the parameters passed in
	    achunksize = chunksize;
	  }
	  else
	    return(-(syserr));						// any other error at this point is not handled
	}								// xattrs exist -> read chunk size
	else if(getxattr(ctmptr->chnkfname, CTA_CHNKSZ_XATTR, (void *)&achunksize, sizeof(size_t)) < 0)
	  return(-ENOTSUP);						// error at this point means there are other issue -> return any error
	
	ctmptr->chnknum = anumchunks;					// now assign number of chunks to CTM structure
	ctmptr->chnksz = achunksize;					// assign chunk size to CTM structure
	if((arrysz=allocateCTMFlags(ctmptr)) <= 0)			// allocate the chunk flag bit array
	  return(-1);							//    problems? -> return an error

	if(axist >= 0) {						// if first call to getxattr() >= 0 -> can read the chunk flags
	  if(getxattr(ctmptr->chnkfname, CTA_CHNKFLAGS_XATTR, (void *)(ctmptr->chnkflags), arrysz) < 0) 
	    return(-ENOTSUP);						// error at this point means there are other issue -> return any error
	}

	return(1);
}

/**
* This function stores a CTM structure into extended attributes. Note that
* the field chnkstore is used to allow for CTA_CHNKNUM_XATTR and 
* CTA_CHNKSZ_XATTR to be stored only on the first invocation of this 
* function. Subsequent calls will only update the chunk flags.
*
* @param ctmptr		pointer to a CTM structure to 
* 			store. 
*
* @return 0 if there are no problems store the structure
* 	information into the xattrs. Otherwise a number corresponding
* 	to errno is returned. It may be a negative value.
*/
int storeCTA(CTM *ctmptr) {
	int rc = 0;							// return code for function
	int n;								// number of bytes written to CTA file

	if(!ctmptr || strIsBlank(ctmptr->chnkfname)) 
	  return(EINVAL);						// Nothing to write, because there is no structure, or it is invalid!

	if(setxattr(ctmptr->chnkfname, CTA_CHNKFLAGS_XATTR, (void *)(ctmptr->chnkflags), SizeofBitArray(ctmptr), 0) < 0)
	  rc = errno;
	if(!ctmptr->chnkstore && !rc) {					// if these xattrs have not been stored, store them now
	  if(setxattr(ctmptr->chnkfname, CTA_CHNKNUM_XATTR, (void *)&(ctmptr->chnknum), sizeof(long), 0) < 0)
	    rc = errno;
  	  if(!rc && (setxattr(ctmptr->chnkfname, CTA_CHNKSZ_XATTR, (void *)&(ctmptr->chnksz), sizeof(size_t), 0) < 0))
	    rc = errno;
	  if(!rc) ctmptr->chnkstore = TRUE;				// no errors? -> mark chkstore as done.
	}
	return(rc);
}
 
/**
* This removes a CTM xattrs from a file's metadata entry.
*
* @param chnkfname	name of the file to test if
* 			xattrs need to be remove. 
*
* @return 0 if file was removed. otherwise a number
* 	 corresponding to errno is returned.
*/
int deleteCTA(const char *chnkfname) {
	int rc = 0;							// return code for function

	if(strIsBlank(chnkfname)) 
	  return(EINVAL);						// Nothing to delete, because nofile name was given

	if(removexattr(chnkfname, CTA_CHNKNUM_XATTR) < 0)		// remove the xattr for number of chunks
	  rc = errno;							// problems? -> assign the rc
	if(removexattr(chnkfname, CTA_CHNKSZ_XATTR) < 0)		// remove the xattr for chunksize
	  rc = (!rc)?errno:rc;						// assign only of not previously assigned
	if(removexattr(chnkfname, CTA_CHNKFLAGS_XATTR) < 0)		// remove the xattr for chunk flags
	  rc = (!rc)?errno:rc;						// assign only of not previously assigned
	return(rc);
}

/**
* This function assigns CTA functions to the given CTM_IMPL
* structure.
*
* @param ctmimplptr	pointer to a CTM_IMPL structure
* 			to update. This is an OUT parameter.
*/
void registerCTA(CTM_IMPL *ctmimplptr) {
	ctmimplptr->read = populateCTA;
	ctmimplptr->write = storeCTA;
	ctmimplptr->delete = deleteCTA;
	return;
}

/**
* Function to indicate if the CTM xattrs associated with
* a file actually exists. Man page for getxattr() indicates
* that errno my be set to NOATTR if an attribute does not
* exist. Initial unit testing should that a size of 0 is
* returned by getxattr when an attribute does not exist.
* Hence, the return code/attribute size is also tested
* to determine if the CTM attributes exist for the
* given file.
*
* @param transfilename	the name of the file to test
*
* @return TRUE if all of CTM xattrs exists. Otherwise FALSE.
*/
int foundCTA(const char *transfilename) {
	void *nullbuf = (void *)NULL;					// a test buffer. We are not interested in retrieving values
	ssize_t rc;							// return code of getxattr(), which is also the size of xattr

									// testing for number of chunks xattr (and making sure file exists)
	if((rc=getxattr(transfilename, CTA_CHNKNUM_XATTR, nullbuf, 0)) <= 0) {
	  if(!rc || errno == ENOENT || errno == ENOATTR || errno == ENOTSUP) return(FALSE);
	}
									// testing for chunk size xattr
	if((rc=getxattr(transfilename, CTA_CHNKSZ_XATTR, nullbuf, 0)) <= 0) {
	  if(!rc || errno == ENOATTR) return(FALSE);
	}
									// testing for chunk flags xattr
	if((rc=getxattr(transfilename, CTA_CHNKFLAGS_XATTR, nullbuf, 0)) <= 0) {
	  if(!rc || errno == ENOATTR) return(FALSE);
	}
	return(TRUE);
}

