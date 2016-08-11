//
// Defines for generating synthetic data
//

#ifndef      __SYNDATA_H
#define      __SYNDATA_H

#define SYN_PATTERN_SIZE	131072		// 100 KB

// A structure to manage a buffer full of synthetic data
typedef struct {
	int length;			// length of buffer
	char *buf;			// The actual pattern buffer
} SyndataBuffer;

// convenient for outsiders to mock up dummy types
typedef SyndataBuffer* SyndataBufPtr;

// Function Declarations
SyndataBuffer* syndataCreateBuffer        (char *pname);
SyndataBuffer* syndataCreateBufferWithSize(char *pname, int length);
SyndataBuffer* syndataDestroyBuffer(SyndataBuffer *synbuf);
int            syndataExists       (SyndataBuffer *synbuf);
int            syndataFill         (SyndataBuffer *synbuf, char *inBuf, int inBufLen);

#endif	//__SYNDATA_H
