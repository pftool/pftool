/*
* Contains string coversion, test, and manipulation functions
*/

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__APPLE__)
#  define COMMON_DIGEST_FOR_OPENSSL
#  include <CommonCrypto/CommonDigest.h>
#  define SHA1 CC_SHA1
#else
#  include <openssl/md5.h>
#endif

#include "str.h"

//
// Conversion functions
//

/**
* Takes a string and returns a MD5 hash string.
*
* @param str	the string to hash
*
* @return an MD5 hash in string format. This 
* 	string will have 32 digits. If there are 
* 	problems computing the hash, NULL should
* 	be returned. Note that if str is NULL,
* 	the the returned hash will be:
* 	d41d8cd98f00b204e9800998ecf8427e 
*/
char *str2md5(const char *str) {
    int n, rc;
    MD5_CTX c;
    unsigned char digest[16];
    char *out = (char*)malloc(33);
    int length = 0;

    if(str)  length = (strlen(str));
    rc = MD5_Init(&c);

    while (rc && length > 0) {
        if (length > 512) {
            rc = MD5_Update(&c, str, 512);
        } else {
            rc = MD5_Update(&c, str, length);
        }
        length -= 512;
        str += 512;
    }

    MD5_Final(digest, &c);

    for (n = 0; n < 16; ++n) {
        snprintf(&(out[n*2]), 16*2, "%02x", (unsigned int)digest[n]);
    }

    return out;
}



/**
* Converts a string containing human-readable size-specification
* (e.g. "32T") into the corresponding numeric value. Typically, the input
* string should have "byte units" included in it.  These "units" are read
* and used to compute the returned size_t.
*
* @param ss the input string to be converted
*
* @return Numeric value (as a size_t), if the string could be converted.
*         Otherwise, return (size_t)-1.
*/

size_t str2Size(char* ss)
{
   size_t  sizeNoUnits;          // return value
   int     ii;
   int     minusOne = FALSE;      // flag to indicate that string specified as <num><units>-1 (i.e. 20MB-1)
   int     nn;                  // length of unprocessed string

#define    TMP_STR_MAX  64
   char    tmpStr[TMP_STR_MAX];

   struct unitsTblStruct {
		const char*	name;               // units name
		size_t    	mult;            // multiplier associated with units
	};
	static struct unitsTblStruct unitsTbl[] = {
      {"p",    1000L*1000L*1000L*1000L*1000L},
      {"t",    1000L*1000L*1000L*1000L},
      {"g",    1000*1000*1000},
      {"m",    1000*1000},
      {"k",    1000},
      {"P",    1024L*1024L*1024L*1024L*1024L},
      {"T",    1024L*1024L*1024L*1024L},
      {"G",    1024*1024*1024},
      {"M",    1024*1024},
      {"K",    1024},
      {0,      1},
	};

	if (strIsBlank(ss))
      return ((size_t)(-1));		// No conversion took place

   strncpy(tmpStr, ss, TMP_STR_MAX);
	nn = strlen(tmpStr);

   // handle "<num><units>-1"
	if (tmpStr[nn-1] == '1' && nn > 1 && tmpStr[nn-2] == '-') {
		tmpStr[nn-2] = 0;
		nn -= 2;
		minusOne = TRUE;
	}
	if (tmpStr[nn-1] == 'B' || tmpStr[nn-1] == 'b') {	// strip off any trailing "B" in units
		tmpStr[nn-1] = 0;
		nn--;
	}
	for (ii=0; unitsTbl[ii].name; ii++) {			// translate unit name
		if (tmpStr[nn - 1] == unitsTbl[ii].name[0]) {
			tmpStr[nn - 1] = 0;
			break;
		}
	}

   // Now form the size_t
	errno = 0;						// Clear any previous errors
	sizeNoUnits = (size_t)strtoul(tmpStr,NULL,10);		// convert to a numeric type
	if(errno) return((size_t)(-1));				// Problems converting to numeric format						

	sizeNoUnits *= unitsTbl[ii].mult;			// Multiply by multiplier
	if (minusOne) sizeNoUnits--;
	return (sizeNoUnits);
}

//
// Testing functions
//

/**
* Test a string to see if it has printable characters in it,
* other than whitespace. If it does, then it is considered
* NOT blank, and this function returns FALSE. Otherwise
* TRUE is returned - including if the argument is NULL.
*
* @param s	string to test
*
* @return FALSE is returned if S has any character between '!' - '~'.
*	Otherwise TRUE is returned
int strIsBlank(char *s)
{
   if (!s) return TRUE;
   while (*s && !isgraph(*s)) ++s;
   return (*s) ? FALSE : TRUE;
}
*/

/**
* Test a string to see if it has printable characters in it,
* other than whitespace. If it does, then it is considered
* NOT blank, and this function returns FALSE. Otherwise
* TRUE is returned - including if the argument is NULL.
*
* @param s	string to test
*
* @return FALSE is returned if s has any character between '!' - '~'.
*	Otherwise TRUE is returned
*/
int strIsBlank(const char *s)
{
        for ( ; s && !isgraph(*s) && *s!='\0'; s++);
        return (!s || *s=='\0') ? TRUE : FALSE;
}

/**
* Test a string to see if the start of it forms prefix.
* Prefixs are defined in the given prefixes argument,
* which is a table of strings, terminated by a NULL entry.
*
* @param prefixes	a NULL terminated array of strings that are
* 			defined as "prefixes"
* @param s		string to test
*
* @return TRUE if s starts with any entry in prefixes[]. 
* 	Otherwise FALSE is returned
*/
int strHasPrefix(const char *prefixes[],const char *s)
{
	return(strStripPrefix(prefixes,s) != (char *)NULL);	// if we can strip the prefix off -> the prefix exists in the given string
}

//
// Parsing Functions
//

/**
* This function returns a pointer to the first character in
* a string that is NOT part of a prefix, as defined by the
* prefixes argument. In the case of a prefix only string, the 
* returned pointer will point to '\0'. If there is no defined
* prefix in the string, then NULL is returned. 
*
* @param prefixes	a NULL terminated array of strings that are
* 			defined as "prefixes"
* @param s		string to parse
*
* @return a non-null pointer into the string that points to the
* 	character NOT in a prefix. NULL is returned if the string
* 	does not contain a defined prefix.
*/
char *strStripPrefix(const char *prefixes[],const char *s)
{
	int j = 0;						// array index
								// looping through prefix entries. strncmp() tests the beginning of the string.
	while(prefixes && prefixes[j] && strncmp(s,prefixes[j],strlen(prefixes[j]))) j++;
								// if table exists and current entry is not NULL, then we found a match -> return 
								// the first charater past the prefix
	return((prefixes && prefixes[j])?((char *)(s+strlen(prefixes[j]))):(char*)NULL);
}

