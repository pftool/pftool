/*
* Implements the generation of synthetic data. There are also function/routines to
* manage a buffer filled with synthetic data.
*/

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "syndata.h"

/**
* This function returns the length of the pattern that fills
* the given buffer. Note that the buffer is filled with the
* contents of a pattern file.
*
* @param pfile		the name of the pattern file
*			to use as a basis.
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @return the length of the pattern buffer. A negative
*	value is returned if there is an error
*/
int synFillPattern(char *pfile, char *inBuf, int inBufLen)
{
	int	fd;					// pattern file fd
	int	n;					// number of bytes read from pattern file

	if(!inBuf || inBufLen <= 0) 
		return(n = -42);			// input buffer does not have any size or is null

	if( (fd = open(pfile, O_RDONLY)) <= 0)
		return(n = -errno);

	n = read(fd, inBuf, inBufLen);			// read in pattern
	if(n < 0 ) n = -errno;
	close(fd);

	return(n);
}

/**
* This function returns the length of the given buffer. In other
* words, it simply returns inBufLen. This function fills the buffer
* by repeating the given constant pattern. If the pattern has no
* length, then the buffer is simply filled with zeros
*
* @param pattern	a charater pattern to repeat in the
*			buffer. The pattern  is NULL or the
*			empty string, the inBuf is filled
*			with zeros
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @return the length of the pattern buffer. A negative
*	value is returned if there is an error
*/
int synCopyPattern(char *pattern, char *inBuf, int inBufLen)
{
	int 	ii;

	if(!inBuf || inBufLen <= 0) 
		return(-42);									// input buffer does not have any size or is null
	if(!pattern || pattern[0] == '\0')							// No pattern? fill inBuf with zeros
		bzero(inBuf,inBufLen);
	else {											// repeat pattern in buffer
		int plen = strlen(pattern);							// the length of the pattern
		int mlen = (inBufLen < plen)?inBufLen:plen;					// copy length

		for (ii=0; ii<inBufLen; ii += mlen) {
			if(ii && (ii + mlen)>inBufLen)						// Make sure memcpy stays within inBuf!
				mlen = inBufLen - ii;
			memcpy(inBuf + ii, pattern, mlen);
		}
	}
	return(inBufLen);
}

/**
* This function returns the length of the given buffer. In other
* words, it simply returns inBufLen. This function fills the buffer
* with random character data.
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @param rseed		an integer, used to modify the random
*			seed
* @return the length of the pattern buffer. A negative
*	value is returned if there is an error
*/
int synGeneratePattern(char *inBuf, int inBufLen, int rseed)
{
   int            ii;
   int            seed;         // seed for rand()
   struct timeval tv;           // used in calculating the seed

	if(!inBuf || inBufLen <= 0) 
		return(-42);									// input buffer does not have any size or is null

	// try to start random with a semi-random seed
	seed = (gettimeofday(&tv,NULL) < 0) ? rseed : (int)(tv.tv_sec + tv.tv_usec + rseed);

	for(ii=0; ii<inBufLen; ii++)
		inBuf[ii] = (char)(128.0 * rand_r((unsigned int*)&seed)/(RAND_MAX + 1.0));

	return(inBufLen);
}

/**
* This function returns a buffer of a given size filled
* with synthetic data, based on a pattern in the given buffer. 
* Data in the buffer should be semi-random. If interested
* in tape compressabiltiy, the data in the buffer should be 
* designed to reflect the amount of compressability desired.
*
* If more randomness in the data is desired, then the
* randomizeData flag should be set when calling this
* function. Randomizing may alter the compressability of
* the data. It will also add time to the generation of
* the data.
*
* @param patbuf		the buffer containing the synthetic
*			pattern
* @param patlen		the length of the pattern buffer
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @param randomizeData	a flag to indicate the generated data
*			should be randomized
* @return 0 if the data is generated without error. Otherwise
*	non-zero is returned.
*/
int synFillData(char *patbuf, int patlen, char *inBuf, int inBufLen, int randomizeData)
{
	int	err = 0;				// the return value
	int	ii;					// copy index for inBuf
	int	n;					// number of bytes read from pattern file
	int	seed;					// seed for rand()
	struct timeval tv;				// used in calculating the seed
	int	wsize;					// the amount of the pattern buffer to write, if randomizing
	int	mlen;					// the amount of the pattern to copy into the buffer, via memcpy()
	int	pstart;					// the offset into the pattern buffer

	if (!randomizeData) {				// Just copy the straight pattern - no funny stuff!
		mlen = (inBufLen < patlen)?inBufLen:patlen;
		for (ii=0; ii<inBufLen; ii += mlen) {
			if(ii && (ii + mlen)>inBufLen)						// Make sure memcpy stays within inBuf!
				mlen = inBufLen - ii;
			memcpy(inBuf + ii, patbuf, mlen);
		}
		return(err);
	}

	// To randomize the data a bit, we are going to copy in
	// half of the pattern buffer per iteration of the loop,
	// starting at a random spot in the first half of the pattern.
	// The starting point will change for each interation.
	wsize = patlen/2;									// write only half of pattern at a time
	seed = (gettimeofday(&tv,NULL) < 0)?42:(int)(tv.tv_sec + tv.tv_usec);			// try to start random with a semi-random seed
	mlen = (inBufLen < wsize)?inBufLen:wsize;
	for (ii=0; ii<inBufLen; ii += mlen) {
		pstart = (int)(((double)wsize) * rand_r((unsigned int*)&seed)/(RAND_MAX + 1.0));		// generate the pattern offset
		if(ii && (ii + mlen)>inBufLen)							// Make sure memcpy stays within inBuf!
			mlen = inBufLen - ii;
		memcpy(inBuf + ii, patbuf + pstart, mlen);
	}

	return(err);
}

/**
* This function returns a buffer of a given size filled
* with synthetic data, based on a pattern in a given file. 
* Data in the file should be semi-random. If interested
* in tape compressabiltiy, the data in the file should be 
* designed to reflect amount of compressability desired.
*
* If more randomness in the data is desired, then the
* randomizeData flag should be set when calling this
* function. Randomizing may alter the compressability of
* the data. It will also add time to the generation of
* the data.
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @param randomizeData	a flag to indicate the generated data
*			should be randomized
* @return 0 if the data is generated without error. Otherwise
*	non-zero is returned.
*/
int synGenerateData(char *pfile,char *inBuf, int inBufLen, int randomizeData)
{
	char	patbuf[SYN_PATTERN_SIZE];			// pattern buffer
	int 	pbuflen = SYN_PATTERN_SIZE;			// length of pattern buffer
	int	err = 0;				// the return value
	int	n;					// number of bytes read from pattern file

	if(!inBuf || inBufLen <= 0) 
		return(err = 42);			// input buffer does not have any size or is null

	if ( (n = synFillPattern(pfile,patbuf,pbuflen)) <= 0)
		return(err=n);
	pbuflen = n;					// note that n is the length of the pattern

	return(err = synFillData(patbuf,pbuflen,inBuf,inBufLen,randomizeData));
}


//
// Synthetic Buffer Functions and Routines
//

/**
* This function returns a buffer filled with a synthetic data pattern.
*
* If length is negative, we use its absolute-value as the seed to generate
* a random pattern (of length SYN_PATTERN_SIZE).  If length is zero, we use
* SYN_PATTERN_SIZE as the length.  Otherwise, length specifies the length
* of the pattern to generate.
*
* If not generating random-data, pname is either a file-name, or one of the
* strings "zero" or "lzinf" (which mean to generate all-zeros, of the
* specified length), or a literal string to use as the pattern.  In the
* latter case, the first character must be non-zero
*
* If the pname parameter is a string, it is used as a repeatable
* pattern. pname is a valid pattern if the first character is printable
* and NOT a space. There are 2 special pattern strings - namely
* "zero" and "lzinf", which fill the pattern buffer with zeros.
*
* If pname does NOT start with a printable character, or is NULL, then
* a randomized pattern is generated in the SyndataBuffer.
*
* Avoid calling stat(), if parameters don't require it.
*
* @param pname       the name of the pattern file to use as a basis, or an
*        explicit pattern, or either of the strings "zero" or "lzinf"
*        (which mean to use all-zeros as the pattern).
*
* @param length      the size of the buffer to create.
*        If length  > 0, it's treated as a length.
*        If length == 0, SYN_PATTERN_SIZE is used.
*        If length  < 0, it's treated as the negative of a random-seed
*                        for a random pattern (of size SYN_PATTERN_SIZE).
*
* @return a pointer to a SyndataBuffer. NULL is returned if there are
*        errors
*/
SyndataBuffer* syndataCreateBufferWithSize(char *pname, int length)
{
   SyndataBuffer *out;             // The buffer to return
   int len = (length > 0) ? length : SYN_PATTERN_SIZE;      // the length of the created buffer
   struct stat st_test;             // a Stat buffer to test for existence

   out = (SyndataBuffer*)malloc(sizeof(SyndataBuffer));
   out->buf = (char*)malloc(len);
   out->length = len;

   int rc;

   if (length < 0) {
      // convert length positive, and use it as a random seed
      int rseed = -length;
      rc = synGeneratePattern(out->buf,out->length,rseed);
   }
   else if (!pname) {
      int rseed = 0;
      rc = synGeneratePattern(out->buf,out->length,rseed);
   }
   else if (!strcasecmp(pname, "zero") ||
            !strcasecmp(pname, "lzinf")) {
      // caller wants all-zeros
      rc = synCopyPattern(NULL,out->buf,out->length);
   }
   else if (!stat(pname,&st_test)) {
      // patten file exists - use it to generate the pattern
      rc = synFillPattern(pname,out->buf,out->length);
   }
   else if (pname && isgraph(*pname)) {
      // user provided an explicit pattern string
      rc = synCopyPattern(pname,out->buf,out->length); /* uses strlen() on pattern */
   }
   else {
      rc = -1;
   }

   // check for errors
   if (rc <= 0) {
      syndataDestroyBuffer(out);
      return ((SyndataBuffer*)NULL);
   }

   out->length = len;               // Make sure length of pattern is acurate!
   return(out);
}

/**
* This function returns a buffer filled with a synthetic data
* pattern. If the patName parameter is a valid file, then
* the pattern generated is based on the contents of that file.
*
* @param pname		the name of the pattern file
*			to use as a basis, so some other
*			pattern designation.
* @return a pointer to a SyndataBuffer. NULL is returned if
* 	there are errors
*/
SyndataBuffer* syndataCreateBuffer(char *pname)
{
	return(syndataCreateBufferWithSize(pname,SYN_PATTERN_SIZE));
}

/**
* This routine frees space used by a SyndataBuffer.
* If the buffer is not allocated, or otherwise empty,
* nothing done.
*
* @param synbuf		the SyndataBuffer to free
*
* @return always returns null sysdata_buffer.
*/
SyndataBuffer* syndataDestroyBuffer(SyndataBuffer *synbuf)
{
	if(synbuf) {
	   if(synbuf->buf) free(synbuf->buf);
	   free(synbuf);
	}

	return((SyndataBuffer*)NULL);
}

/**
* This function tests to see of a SyndataBuffer has been 
* initialized or allocated.
*
* @param synbuf		the SyndataBuffer to free
*
* @return a non-zero value (i.e. TRUE) if this buffer
* 	has been initialized. Otherwise zero (i.e. FALSE)
*	is returned.
*/
int syndataExists(SyndataBuffer *synbuf)
{
	return(synbuf && synbuf->length > 0);
}

/**
* This function fills a given buffer with the data from
* the provided sysdata_buffer. See synFillData(). Note
* that the reading of the SyndataBuffer will be random.
*
* @param synbuf		the SyndataBuffer to fill from
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @return 0 if the data is generated without error. Otherwise
*	non-zero is returned.
*/
int syndataFill(SyndataBuffer *synbuf, char *inBuf, int inBufLen)
{
	if(!syndataExists(synbuf)) return(42);
	return(synFillData(synbuf->buf,synbuf->length,inBuf,inBufLen,1));
}

