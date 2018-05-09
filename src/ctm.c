/**
* Implements routines for a CTM (Chunk Transfer Metadata) structure
* In order to add new implemenations, both this file and the ctm_impl.h
* files will need to modified/updated.
*/

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/xattr.h>

#include "pfutils.h"
#include "str.h"
#include "ctm.h"
#include "ctm_impl.h"					// holds implementation specific declarations

#define CTM_TEST_XATTR "user.xfer._test_"

/**
* Returns a CTM Impleentation in String format.
* See ctm.h for the list of implementations.
*
* @param implIdx	the implementation type
*
* @return a string representation of the implementation
*/
const char *_impl2str(CTM_ITYPE implidx) {
	static const char *IMPLSTR[] = {
      "No CTM"
      ,"File CTM"
      ,"xattr CTM"
      ,"Unsupported CTM"
   };
	return((implidx > CTM_UNKNOWN)?"Unknown CTM":IMPLSTR[implidx]);
}

/**
* This function determines how CTM is stored in the persistent
* store, based on where the file is stored. That is to say,
* it determines if xattrs or files are used to store CTM for
* the given file. If the file is not specified, then a value
* of CTM_NONE is returned.
*
* @param transfilename  the name of the file to test
*
* @return an CTM_ITYPE that indicates how the persistent
*  store is (or would be) implemented for the given
*  file.
*/
CTM_ITYPE _whichCTM(const char *transfilename) {

#if CTM_MODE == CTM_PREFER_FILES
   return CTM_FILE;

#elif CTM_MODE == CTM_PREFER_XATTRS
   CTM_ITYPE itype =  CTM_NONE;        // implementation type. Start with no CTM implementation
   if(!strIsBlank(transfilename)) {    // non-blank filename -> test if tranferred file supports xattrs
     if(!setxattr(transfilename,CTM_TEST_XATTR,"novalue",strlen("novalue")+1,0)) {
       removexattr(transfilename,CTM_TEST_XATTR);  // done with test -> remove it.
       itype = CTM_XATTR;           // yes, it does!
     }
     else {
       int errcpy = errno;          // make a copy of errno ...

       switch (errcpy) {
         case ENOENT : itype = CTM_XATTR;    // if file does not exist -> assume xattrs are supported
             break;
         case ENOTSUP:           // xattrs are not supported
         default     : itype = CTM_FILE;     // another error? -> use files
       } // end error switch
     } // end setxattr() else
   }
   return(itype);

#else
   return CTM_NONE;

#endif
}

/**
* This function returns a newly allocated CTM structure.
* This function also tests to see if xattrs are supported
* by the file to transfer.
*
* @param transfilename	the name of the file to transfer
*
* @return an empty CTM structure. A NULL pointer is returned
* 	if there are problems allocating the structure.
*/
CTM *_createCTM(const char *transfilename) {
	CTM *newCTM = (CTM *)NULL;				// pointer to the newCTM structure
	CTM_ITYPE itype =  _whichCTM(transfilename);	// implementation type. Get how the CTM is store in persistent store

	if(itype == CTM_NONE || itype == CTM_UNKNOWN)	// no or unsupported type? -> return NULL
	  return(newCTM);

	newCTM = (CTM *)malloc(sizeof(CTM));		// now we allocate the structure
	memset(newCTM,0,sizeof(CTM));			// clear the memory of the newCTM CTM structure
	newCTM->chnkimpl = itype;				// assign implmentation
	switch ((int)newCTM->chnkimpl) {			// now fill out structure, based on how CTM store is implemented
	  case CTM_XATTR : newCTM->chnkfname = strdup(transfilename);
			   registerCTA(&newCTM->impl);	// assign implementation
			   break;
	  case CTM_FILE  :
          default        : if(newCTM->chnkfname = genCTFFilename(transfilename))
			     registerCTF(&newCTM->impl);	// assign implementation
			   else
			     freeCTM(&newCTM);		// problems generating filename for CTM -> abort creation and clean up memory
	}

	return(newCTM);
}

/**
* Allocates a new flag array for a CTM 
* structure. Note that the structure should have
* the number of chunks already assigned.
*
* @param ctmptr		pointer to a CTM structure to modify
*
* @return the size ofthe bufer used for chunk flags. a negative
* 	value is returned if there are problems with allocating
* 	the bit array buffer.
*/
size_t allocateCTMFlags(CTM *ctmptr) {
	size_t bufsz;						//size of the bit array buffer

	if(!ctmptr || ctmptr->chnknum <= 0L)
	  return((size_t)(-1));
								// compute the buffer size
	bufsz = (size_t)(sizeof(unsigned long)*ComputeBitArraySize(ctmptr->chnknum));
	ctmptr->chnkflags = (unsigned long *)malloc(bufsz);
	memset(ctmptr->chnkflags,0,bufsz);

	return(bufsz);
}

/**
* This function frees the memory used by a CTM structure. It also
* sets the pointer to NULL;
*
* @param pctmptr	pointer to a CTM structure pointer 
* 			to destroy. It is an OUT parameter.
*/
void freeCTM(CTM **pctmptr) {
	CTM *ctmptr = (*pctmptr);

	if(ctmptr) {
	  if(ctmptr->chnkflags) free(ctmptr->chnkflags);
	  if(!strIsBlank(ctmptr->chnkfname)) free(ctmptr->chnkfname);
	  free(ctmptr);
	  *pctmptr = (CTM*)NULL;				// make sure that the pointer to the structure is zeroed out. Note in order to change value, need to
	}							// assign pctmptr to point to NULL, rather thsn ctmptr.
	return;
}

/**
* This function returns a CTM (Chunk Transfer Metadata) structure. 
* This structure holds information as to what chunks of a file have 
* been transferred.
*
* @param transfilename	the name of the file to transfer
* @param numchnks	the number of chunks to transfer.
* 			This is the length of the chnkflags
* 			array.
* @param sizechnks	the size of the chunks for this 
* 			file. Should NOT be zero!
*
* @return a populated CTM structure. A NULL pointer is returned
* 	if there are problems allocating the structure.
*/
CTM *getCTM(const char *transfilename, long numchnks, size_t sizechnks) {
	CTM *newCTM = _createCTM(transfilename);			// allocate the newCTM structure

	if(newCTM) {						// we have an allocated CTM structure. Read from persistent store
	  if(newCTM->impl.read(newCTM,numchnks,sizechnks) < 0)
	    freeCTM(&newCTM);					// problems reading metadata -> abort get and clean up memory
	}
	return(newCTM);
}

/**
* This function stores a CTM structure into a persistent store
*
* @param ctmptr		pointer to a CTM structure to 
* 			store. 
*
* @return 0 if there are no problems storing the structure
* 	information into the persistent store. Otherwise a number 
* 	corresponding to errno is returned.
*/
int putCTM(CTM *ctmptr) {
	if(!ctmptr) return(EINVAL);				// Nothing to write, because there is no structure!
	return(ctmptr->impl.write(ctmptr));
}

/**
* This function updates or sets the chunk flag of the given
* index in the bit array associated with the CTM structure.
* It then stores the updated CTM structure to the persistent
* store.
*
* @param ctmptr		pointer to a CTM structure to 
* 			update. 
* @param chnkidx	index of the chunk flag
*
* @return 0 if there are no problems updating/storing the structure
* 	information into the persistent store. Otherwise a number 
* 	corresponding to errno is returned.
*/
int updateCTM(CTM *ctmptr, long chnkidx) {
	setCTM(ctmptr,chnkidx);
	return(putCTM(ctmptr));
}

/**
* This removes CTM from the assciated file and destroys or frees 
* a CTM structure.
*
* @param pctmptr	pointer to a CTM structure pointer 
* 			to remove. It is an OUT parameter.
*
* @return 0 if file was removed. otherwise a number
* 	 corresponding to errno is returned.
*/
int removeCTM(CTM **pctmptr) {
	CTM *ctmptr = (*pctmptr);				// what pctmptr points to
	int rc = 0;						// return code for function

	if(!ctmptr) return(EINVAL);				// Nothing to remove, because there is no structure!
	rc=ctmptr->impl.del(ctmptr->chnkfname);		// delete CTM from persistent store
	freeCTM(pctmptr);					// deallocate the CTM structure
	return(rc);
}

/**
* Function to indicate if the persistent CTM store exists
* for the given file.
*
* @param transfilename	the name of the file to test
*
* @return TRUE if the CTM exists. Otherwise FALSE.
*/
int hasCTM(const char *transfilename) {
	CTM_ITYPE itype =  _whichCTM(transfilename);		// implementation type. Get how the CTM is stored in persistent store

	switch ((int)itype) {					// test, based on how CTM store is implemented
	  case CTM_NONE    :					// no or unsupported type? -> return FALSE
	  case CTM_UNKNOWN : return(FALSE);
	  case CTM_XATTR   : return(foundCTA(transfilename));
	  case CTM_FILE    :
          default          : return(foundCTF(transfilename));
	}
}

/**
* Function to purge CTM data from a given file.
*
* @param transfilename	the name of the file to
* 			remove/purge CTM data from
*/
void purgeCTM(const char *transfilename) {
	CTM_ITYPE itype =  _whichCTM(transfilename);		// implementation type. Get how the CTM is stored in persistent store
	char *chnkfname;					// holds the md5 name if CTM is implemented with CTF files

	switch ((int)itype) {					// test, based on how CTM store is implemented
	  case CTM_XATTR   : deleteCTA(transfilename);		// don't care about the return code
			     break;
								// have to generate the md5 name for CTF files
	  case CTM_FILE    : chnkfname = genCTFFilename(transfilename);

			     unlinkCTF(chnkfname);		// don't care about return code
			     if(chnkfname) free(chnkfname);	// we done with the temporary name
			     break;
	  case CTM_NONE    :					// no or unsupported type? -> nothing to do
	  case CTM_UNKNOWN : 
          default          : break;
	}
	return;
}

/**
* This routine sets the flag for a given
* chunk index to TRUE.
*
* @param ctmptr		pointer to a CTM structure to 
* 			set.
* @param chnkidx	index of the chunk flag
*/
void setCTM(CTM *ctmptr, long chnkidx) {
	if(ctmptr) SetBit(ctmptr->chnkflags,chnkidx);
	return;
}

/**
* This function tests a single chunk index to
* see if the chunk has been transferred - this is.
* that the specified chunk flag is set. If the 
* ctmptr is not set (i.e. NULL), then this function
* returns FALSE.
*
* @param ctmptr		pointer to a CTM structure
* 			to test
* @param idx		the index of the desired 
* 			chunk flag to test
*
* @return TRUE (i.e. non-zero) if the specified 
* 	chunk flag is set. Otherwise FALSE (i.e. zero)
*/
int chunktransferredCTM(CTM *ctmptr,int idx) {
	if(!ctmptr) return(FALSE);			// no structure -> nothing to check
	return(TestBit(ctmptr->chnkflags,idx));
}

/**
* This function tests the chuck flags of
* a CTM structure. If they are all set
* (i.e. set to 1), then TRUE is returned.
*
* @param ctmptr		pointer to a CTM structure
* 			to test
*
* @return TRUE (i.e. non-zero) if all chunk flags
* 	are set.Otherwise FALSE (i.e. zero) is returned.
*/
int transferredCTM(CTM *ctmptr) {
	int rc = 0;					// return code
	int i = 0;					// index into 

	if(ctmptr) {
	  while(i < ctmptr->chnknum && TestBit(ctmptr->chnkflags,i)) i++;
	  rc = (int)(i >= ctmptr->chnknum);
	}
	return(rc);
}

/**
* Returns a string representation of s CTM
* structure. The return string may be allocated
* in this function. So any calling routine
* needs to manage it (i.e. free it).
*
* @param ctmptr		pointer to a CTM structure
* 			to convert
* @param rbuf		a buffer to hold the string.
* 			If the buffer is not allocated,
* 			then it is allocated.
* @param rlen		the length of rbuf - if allocated
*
* @return a string representation of the given CTM
* 	structure, If the ctmptr is NULL, then NULL
* 	is returned. If rbuf is allocated when this
* 	function is called, then what rbuf is pointing
* 	to is returned.
*/
char *tostringCTM(CTM *ctmptr, char **rbuf, int *rlen) {
	char *flags = (char *)NULL;			// buffer to hold flag values
	int i = 0;

	if(!ctmptr) return((char *)NULL);

	flags = (char *)malloc((2*ctmptr->chnknum)+1);
	for(i=0; i<ctmptr->chnknum; i++)
	  snprintf(flags+(2*i),3,"%d,",TestBit(ctmptr->chnkflags,i));
	i = strlen(flags);
	flags[i-1] = '\0';

	if(!(*rbuf)) {					// rbuf NOT allocated
	  *rlen = strlen(ctmptr->chnkfname) + sizeof(size_t) + sizeof(long) + strlen(flags) + 20;
	  *rbuf = (char *)malloc(*rlen);
	}
	snprintf(*rbuf,*rlen,"%s: %s, %ld, (%ld)\n\t[%s]", _impl2str(ctmptr->chnkimpl),ctmptr->chnkfname, ctmptr->chnknum, ctmptr->chnksz,flags);
	free(flags);

	return(*rbuf);
}

int check_ctm_match(const char* filename, const char* src_to_hash)
{
	int ret = 0;
	int fd;
	char* ctm_name, src_hash; //must be freed
	char ctm_src_hash[SIG_DIGEST_LENGTH * 2 + 1];
	struct stat sbuf;

	ctm_name = genCTFFilename(filename);
	src_hash = str2sig(src_to_hash);
        printf("in ctm src_to_hash %s; src hash %s\n", src_to_hash, src_hash);
	if (stat(ctm_name))
	{
		ret = 0; //there is no ctm, not match
	}
	else
	{
		if((fd = open(ctm_name, O_RDONLY)) < 0)
		{
			free(ctm_name);
			free(src_hash);
			return -errno;
		}
		//read src hash
		if(read(fd, ctm_src_hash, SIG_DIGEST_LENGTH * 2 + 1) < 0)
		{
			free(ctm_name);
			free(src_hash);
			return -errno;
		}
		
		if(!strcmp(ctm_src_hash, src_hash, SIG_DIGEST_LENGTH * 2 + 1))
		{
			//we have a match!
			ret = 2;
		}
	}

	free(ctm_name);
	free(src_hash);
	return ret;
}
