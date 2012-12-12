/*
* Contains string coversion, test, and manipulation functions
*/

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "str.h"

//
// Conversion functions
//


//
// Testing functions
//

/**
* Converts a string to a size_t. Typically, the
* input string should have "byte units" included in it.
* These "units" are read and used to compute the returned
* size_t.
*
* Note that on most linux systems, size_t is the type used
* to store file sizes or values associated with files and
* offsets into files.
*
* @param ss		the input string to be converted
*
* @return negative size_t if string could not be converted to
*	size_t. The correct size_t is returned if the conversion 
*	was successful.
*/
size_t str2Size(char* ss)
{
	size_t sizeNoUnits;					// return value
	int     err;
	int     ii;
	int	minusOne = FALSE;				// flag to indicate that string specified as <num><units>-1 (i.e. 20MB-1)
	int     nn;						// length of unprocessed string
#define TMP_STR_MAX	64
	char	tmpStr[TMP_STR_MAX];

	struct unitsTblStruct {
		char*	name;           /* units name */
		size_t	mult;           /* multiplier associated with units */
	};
	static struct unitsTblStruct unitsTbl[] = {
		"p",    1000L*1000L*1000L*1000L*1000L,
		"t",    1000L*1000L*1000L*1000L,
		"g",    1000*1000*1000,
		"m",    1000*1000,
		"k",    1000,
		"P",    1024L*1024L*1024L*1024L*1024L,
		"T",    1024L*1024L*1024L*1024L,
		"G",    1024*1024*1024,
		"M",    1024*1024,
		"K",    1024,
		0,      1,
	};

	if(strIsBlank(ss)) return((size_t)(-1));		// No conversion took place

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
*/
int strIsBlank(char *s)
{
        for ( ; s && !isgraph(*s) && *s!='\0'; s++);
        return (!s || *s=='\0') ? TRUE : FALSE;
}
