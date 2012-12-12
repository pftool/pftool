//
// Defines for generating synthetic data
//

#ifndef      __SYNDATA_H
#define      __SYNDATA_H

#define SYN_PATTERN_SIZE	131072		// 100 KB

// A structure to manage a buffer full of synthetic data
struct synbuffer {
	int length;			// length of buffer
	char *buf;			// The actual pattern buffer
};

typedef struct synbuffer syndata_buffer;


// Function Declarations
syndata_buffer* syndataCreateBufferWithSize(char *pname, int length);
syndata_buffer* syndataCreateBuffer(char *pname);
syndata_buffer* syndataDestroyBuffer(syndata_buffer *synbuf);
int syndataExists(syndata_buffer *synbuf);
int syndataFill(syndata_buffer *synbuf, char *inBuf, int inBufLen);

#endif	//__SYNDATA_H
