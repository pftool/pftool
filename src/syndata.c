/*
* Implements the generation of synthetic data. There are also function/routines to
* manage a buffer filled with synthetic data.
*/

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
int synFillPattern(char *pfile,char *inBuf, int inBufLen)
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
	int 	ii;
	int	seed;					// seed for rand()
	struct timeval tv;				// used in calculating the seed
	
	if(!inBuf || inBufLen <= 0) 
		return(-42);									// input buffer does not have any size or is null

	seed = (gettimeofday(&tv,NULL) < 0)?rseed:(int)(tv.tv_sec + tv.tv_usec + rseed);	// try to start random with a semi-random seed
	for(ii=0; ii<inBufLen; ii++)
		inBuf[ii] = (char)(128.0 * rand_r(&seed)/(RAND_MAX + 1.0));
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
		pstart = (int)(((double)wsize) * rand_r(&seed)/(RAND_MAX + 1.0));		// generate the pattern offset
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
* This function returns a buffer filled with a synthetic data
* pattern. If the pname parameter is a valid file, then
* the pattern generated is based on the contents of that file.
*
* If the pname parameter is a string, it is used as a repeatable
* pattern. pname is a valid pattern if the first character is printable
* and NOT a space. There are 2 special pattern strings - namely
* "zero" and "lzinf", which fill the pattern buffer with zeros.
*
* If pname does NOT start with a printable character, or is NULL, then
* a randomized pattern is generated in the syndata_buffer.
*
* @param pname		the name of the pattern file
*			to use as a basis, so some other
*			pattern designation.
* @param length		the size of the buffer to create.
*			if a number <= 0 is specified, then
*			SYN_PATTERN_SIZE is used.
*
* @return a pointer to a syndata_buffer. NULL is returned if
* 	there are errors
*/
syndata_buffer* syndataCreateBufferWithSize(char *pname, int length)
{
	syndata_buffer *out;					// The buffer to return
	int len = (length)?length:SYN_PATTERN_SIZE;		// the length of the created buffer
	struct stat st_test;					// a Stat buffer to test for existence

	out = (syndata_buffer*)malloc(sizeof(syndata_buffer));
	out->buf = (char*)malloc(len);
	out->length = len;

	if (!stat(pname,&st_test)) {				// patten file exists - use it to generate the pattern
		if ( (len = synFillPattern(pname,out->buf,out->length)) <= 0) 
			return(syndataDestroyBuffer(out));
	}
	else if (pname && isgraph(*pname)) {			// a pattern string is specified if 1st character is printable
		int isZero = (!strcasecmp(pname,"zero") || !strcasecmp(pname, "lzinf"));

		if ( (len = synCopyPattern((isZero?"":pname),out->buf,out->length)) <= 0) 
			return(syndataDestroyBuffer(out));
        }
	else {							// patten file does not exist and not a valid pattern - need to generate the pattern
		int rseed = (pname && *pname)?(int)*pname:42;

		if ( (len = synGeneratePattern(out->buf,out->length,rseed)) <= 0)
			return(syndataDestroyBuffer(out));
	}
	out->length = len;					// Make sure length of pattern is acurate!
	
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
* @return a pointer to a syndata_buffer. NULL is returned if
* 	there are errors
*/
syndata_buffer* syndataCreateBuffer(char *pname)
{
	return(syndataCreateBufferWithSize(pname,SYN_PATTERN_SIZE));
}

/**
* This routine frees space used by a syndata_buffer.
* If the buffer is not allocated, or otherwise empty,
* nothing done.
*
* @param synbuf		the syndata_buffer to free
*
* @return always returns null sysdata_buffer.
*/
syndata_buffer* syndataDestroyBuffer(syndata_buffer *synbuf)
{
	if(synbuf) {
	   if(synbuf->buf) free(synbuf->buf);
	   free(synbuf);
	}

	return((syndata_buffer*)NULL);
}

/**
* This function tests to see of a syndata_buffer has been 
* initialized or allocated.
*
* @param synbuf		the syndata_buffer to free
*
* @return a non-zero value (i.e. TRUE) if this buffer
* 	has been initialized. Otherwise zero (i.e. FALSE)
*	is returned.
*/
int syndataExists(syndata_buffer *synbuf)
{
	return(synbuf && synbuf->length > 0);
}

/**
* This function fills a given buffer with the data from
* the provided sysdata_buffer. See synFillData(). Note
* that the reading of the syndata_buffer will be random.
*
* @param synbuf		the syndata_buffer to fill from
*
* @param inBuf		the buffer to write the data into
*
* @param inBufLen	the size of the buffer
*
* @return 0 if the data is generated without error. Otherwise
*	non-zero is returned.
*/
int syndataFill(syndata_buffer *synbuf, char *inBuf, int inBufLen)
{
	if(!syndataExists(synbuf)) return(42);
	return(synFillData(synbuf->buf,synbuf->length,inBuf,inBufLen,1));
}

