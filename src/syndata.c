/*
* Implements the generation of synthetic data. There are also function/routines to
* manage a buffer filled with synthetic data, as well as read sythetic paths and
* data tree specifications. See Path-syndata.h for more information.
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

#include "str.h"
#include "syndata.h"

// a private global table holding path prefixes that form synthetic data paths
static const char *syndataPrefixes[] =  { \
			"/dev/synthetic" \
			,"/dev/syndata" \
			,(char*)NULL};

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

//
// Synthetic Data Tree Specification Functions
//


/**
* Allocates and initialized a SyndataTreeSpec structure
*
* @param level	maximum number of levels in a directory tree
*
* @param dirs		maximum number of directories per level
* 
* @param files		maximum number of files per directory
* 
* @param start_level level at which directories are populated
*
* @return NULL if there are problems allocating the structure. Otherwise an 
* 	a pointer to an initialized SyndataTreeSpec structure is returned.
*/
SyndataTreeSpec *syndataInitTreeSpec(int level, size_t dirs, size_t files, int start_level)
{
	SyndataTreeSpec *newspec;

	if(!(newspec = (SyndataTreeSpec *)malloc(sizeof(SyndataTreeSpec))))
              return((SyndataTreeSpec *)NULL);

	newspec->max_level = level;
	newspec->max_dirs = dirs;
	newspec ->max_files = files;
	newspec->start_files = start_level;

	return(newspec);
}

#define MAX_SPEC_NUM 256				// the maximum # of digits for a specification element
/**
* This function parses the string representation of a Synthetic Data Tree
* Specification.  The format of such a specification is as follows:
*	[L<# of levels>][D<# of directories/level>][F<# of files/directory>][+<level # to start populating directories>]
*
* Note that all options in the specification are optional, and can be given in any
* order. If any option is missing, then default values are used. Thus, the spec 
* argument can be null or the empty string, and a valid SyndataTreeSpec structure 
* will be returned. The default values for each option are given as follows:
*		L	- 1 if spec is empty string or 0 if spec is null (meaning no data tree)
*		D	- 0 if L <= 1 or 1 otherwise
* 		F	- 1
*		+	-  level value(start populating directories at last level)
*
* This function should only be called by routines in this file.
*
* @param spec	the Synthetic Data Tree specification in string format
*
* @return NULL if there are any issues parsing the spec. This means that
*	spec is invalid, or there is a memory allocation error. Otherwise 
*	a pointer to a valid SynDataTreeSpec structure.
*/
SyndataTreeSpec *syndataParseTreeSpec(char *spec) 
{
	char *i = spec;					// index into the spec string
	char numbuf[MAX_SPEC_NUM];			// buffer to hold digits

	int level = (i)?1:0;				// holds number of levels
	size_t dirs = 0;				// holds directories per level
	size_t files = 1;				// holds files per directory
	int populate = (level-1);			// holds level at which to start populating files in directories
	int pop_specified = false;			// a flag to indicate of starting level specified in spec

	while(i && (*i)) { 				// run through the spec string
		int inum=0; 				// index for numbuf

		bzero(numbuf, sizeof(numbuf));		// cleans the number buffer
	        switch(*i) {
 		  case 'L' : while(isdigit(*(++i))) numbuf[inum++] = *i;
		             if(inum) level = atoi(numbuf);
		             break;
		  case 'D' : while(isdigit(*(++i))) numbuf[inum++] = *i;
		             if(inum) dirs = strtol(numbuf,(char **)NULL,10);
		             break;
		  case 'F': while(isdigit(*(++i))) numbuf[inum++] = *i;
		         if(inum) files = strtol(numbuf,(char **)NULL,10);
		         break;
 		  case '+': while(isdigit(*(++i))) numbuf[inum++] = *i;
		            if(inum) populate = atoi(numbuf);
		            pop_specified = true;
		            break;
                   default: return((SyndataTreeSpec *)NULL);
               }
	       if(inum >= MAX_SPEC_NUM) 		// number of digits in element is too big!
		 return((SyndataTreeSpec *)NULL);	// something else is going on ....
        } // end parsing loop

	  // Now make sure the spec makes sense
	if( level <= 1)
	  dirs = 0;					// no directories if only one level
	else {						// more than 1 level
          if(dirs < 1) dirs = 1;			// multiple levels -> at least 1 directory
	  if(!pop_specified) populate = (level-1);	// if no starting level specified -> do it on last level
        }

        return(syndataInitTreeSpec(level,dirs,files,populate));
}

//
// Synthetic Data Path Functions
//

/**
* Function to indicate if the given string is a Synthetic Data path. 
* True is returned! If so. Otherwise false is returned.
*
* @param path	the full path to test
*
* @return false (0) is returned if the string is not a synthetic data path.
*	Otherwise true.
*/
int isSyndataPath(char *path) 
{
	return(strHasPrefix(syndataPrefixes,path));	// syndataPrefix is a private array holding strings indicating synthetic data paths 
}

/**
* Function to extract the directory level in a the data tree for 
* the given path. In order to do this, this function needs to
* determine if the path is in fact a diretory in a synthetic data
* tree. If not, then a negative value is returned.
*
* As a by-product of this function, it can be determined to be 
* a directory or not. See isSyntheticDir() for a descussion of
* what makes a sythentic data diretory.
*
* @param path	the full path to extract the level from
*
* @return the directory level of the given synthetic path. If the
* 	path is not a directory, a negative value is returned.
*
* @see isSyntheticDir()
*/
int syndataGetDirLevel(const char *path) 
{
	char *copy;					// a copy of the path argument
	char *pelement;					// last path element
	char *ielem;					// pointer into the last path element
	int level = -1, j=0;				// directory level, array index

	if(strIsBlank(path)) return(level);		// no path means no directory
	ielem = pelement = basename(copy = strdup(path));// best to use a copy when calling basename()

	// This tests to see if the last element in the path has the form d[0-9]+_[0-9]+
	if(*(ielem++) == 'd') {
	  char lvlbuf[128];
	  int i;

	  for(i=0; (isdigit(*ielem)); i++,ielem++)
 	    lvlbuf[i] = *ielem;
	  if(*ielem == '_') {
	    while(isdigit(*(++ielem)));
	    if(*ielem == '\0' && strlen(lvlbuf))	// should be at end of the path element at this point
	      level = atoi(lvlbuf);
	  }
        } // end of format test

	// This test to see if a valid tree specification is included in the path
	if(level < 0) {					// still have not found level
	  int j = 0;					// array index

	  for(ielem = pelement; (*ielem && *ielem!='.' && *ielem!='_' && *ielem!='-'); ielem++);
	  if(*ielem) *ielem = '\0';			// strips any tree specification from pelement

	  while(syndataPrefixes[j] && !strstr(syndataPrefixes[j],pelement)) j++;
	  if(syndataPrefixes[j]) {			// pelement contains a prefix string -> we may have a tree spec
	    SyndataTreeSpec *spec = syndataGetTreeSpec(path);

	    if(spec && spec->max_level)			// if it is a tree specification && max_level > 0 -> it a directory
	      level = 0;				// at 0th level
	    if(spec) free(spec);			// we're done with this tree specification
	  }
	} // end of spec test

	free(copy);					// we are done - cleaning up
	return(level);
}

/**
* Function to indicate if a given path is a synthetic data directory.
* Note that in order to be a directory, a "/" needs to follow the 
* synthetic data prefix. The next element in the path should be a 
* Synthetic Data Tree Specification. No (or an empty) specification 
* is a valid specification. Synthetic data directories are also 
* specified by the last element in the path having a name that 
* corresponds to the following format:
*	d<current level>_<directory #>
*
* If the last element in the path corresponds to that format,then
* it is considered a synthetic data directory.
*
* @param path	the full path to test
*
* @returns false (0) is returned if the string is not a synthetic 
* 	data directory. Otherwise true.
*/
int isSyndataDir(const char *path) 
{
	return(syndataGetDirLevel(path) >= 0);
}

/**
* This function returns a SyndataTreeSpec structure that corresponds to second
* path element in a synthetic data path.
*
* @param path	the full path to extract the tree specification from
*
* @returns NULL if there are any issues parsing the spec. This means that
*	spec is invalid. Otherwise a pointer to a valid SynDataTreeSpec structure.
*/
SyndataTreeSpec *syndataGetTreeSpec(const char *path)
{
	char *copy;					// a copy of the path argument
	char *specStr;					// current path element
	int j = 0;					// prefix index
	SyndataTreeSpec *spec = (SyndataTreeSpec *)NULL;// the tree specification to return

	if(strIsBlank(path)) 
	  return(spec);					// no path means no spec
	copy=strdup(path);				// work with a copy of the path, so that we can modify it
							// assign specStr to the first character NOT in a prefix
	if(!(specStr=strStripPrefix(syndataPrefixes,copy))) {
	  free(copy);					// obviously this is not a well formed path -> no spec
	  return(spec);
	}
	while(*specStr == '.' || *specStr == '_' || *specStr == '-')
	  specStr++;					// move past delimiters, if any
	if(!(*specStr))
	  specStr = (char *)NULL;			// pointing at '\0'. This is a file spec, not a tree spec
	else {
	  while(specStr[j] && specStr[j] != '/') j++;
    	  if(specStr[j])				// assume specStr[j] points to '/'. But if not, syndataParseTreeSpec() will return NULL
	    specStr[j] = '\0';				// terminate specStr in order to have it parsed (hence, using a copy)
	}

	spec = syndataParseTreeSpec(specStr);		// Now parse to get the actual spec
	free(copy);					// we are done - cleaning up
	return(spec);	
}


/**
* This function simulates the stat() call for synthetic data
* paths. Perhaps the single most distinguing piece of information
* for a synthetic data path is the file size. This needs to be
* passed to this function in order to simulate a stat structure
* for this type of path - hence the fsize argument.
*
* @param path	the path to intialize the stat structure for
*
* @param st	a pointer to the stat structure to initialize
*
* @param lvl	a pointer to the directory level of the path
*
* @param fsize	the given file size of the path
*
* @return 0 if there are no problems setting the stat
* 	structure. If path argument is NULL, then a -ENOENT
* 	is returned. If the stat argument is NULL, then
* 	-EFAULT is returned.
*/
int syndataSetAttr(const char *path, struct stat *st, int *lvl, size_t fsize)
{
	if(strIsBlank(path)) return(-ENOENT);		// no path means the is nothing to stat ...
	if(!st) return(-EFAULT);			// not stat pointer means nothing to fill ...

	bzero((void *)st,sizeof(struct stat));		// initialize stat with zeros ...
	st->st_size = fsize;				// We are generating synthetic data, and NOT copying data in file. Need to muck with the file size
	st->st_mtim.tv_sec = time((time_t *)NULL);
	if((*lvl = syndataGetDirLevel(path)) < 0)
	  st->st_mode = (S_IFREG|S_IRUSR|S_IWUSR|S_IRGRP);

	else
	  st->st_mode = (S_IFDIR|S_IRWXU|S_IRWXG);

	return(0);
}
