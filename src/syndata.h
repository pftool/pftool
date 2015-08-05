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
} syndata_buffer;

// convenient for outsiders to mock up dummy types
typedef syndata_buffer* SyndataBufPtr;

// Function Declarations
syndata_buffer* syndataCreateBuffer        (char *pname);
syndata_buffer* syndataCreateBufferWithSize(char *pname, int length);
syndata_buffer* syndataDestroyBuffer(syndata_buffer *synbuf);
int             syndataExists       (syndata_buffer *synbuf);
int             syndataFill         (syndata_buffer *synbuf, char *inBuf, int inBufLen);

#endif	//__SYNDATA_H
