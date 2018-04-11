/**
 * * Wrapper functions for digital signatures or digests. By rewriting
 * * the logic of these routines, different types of signatures can be supported,
 * * or compiled in.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sig.h"

/**
 * * Takes a buffer of data and returns an allocated digest or signature.
 * * Note that it is up to the calling function/eoutine to free the
 * * memory of the digest, once done with it.
 * *
 * * @param buf	the data to hash
 * * @param buflen	the length of the buffer
 * *
 * * @return a hash in digest format. If no buflen is specified,
 * * 	or an error occures when calculating hash, then a NULL 
 * * 	digest or signature is returned.
 * */
unsigned char *signature(const uint8_t *buf, const size_t buflen) {
    int rc;
    SigCTX c;											// signature context structure
    size_t length = buflen;									// local variable for buffer length
    const uint8_t *tbuf = buf;									// local pointer into the data buffer
    unsigned char *md;										// returning signature or digest

    if(!buflen || !buf) return((unsigned char *)NULL);
    rc = SigInit(&c);

    while (rc && length > 0) {									// Allows for computation of the signature, based on
        if (length > SIG_COMPUTE_CHUNK) {							// a specified number of bytes. This allows this function
            rc = SigUpdate(&c, tbuf, SIG_COMPUTE_CHUNK);					// to handle large buffers.
        } else {
            rc = SigUpdate(&c, tbuf, length);
        }
        length = (length < SIG_COMPUTE_CHUNK)?0:(length-SIG_COMPUTE_CHUNK);			// need to be careful not to go off the end of an unsigned integer!
        tbuf += SIG_COMPUTE_CHUNK;
    }
    if(!rc) 											// error occured in computing signature
      return((unsigned char *)NULL);

    md = (unsigned char *)malloc(SIG_DIGEST_LENGTH*sizeof(unsigned char));			// allocate digest
    SigFinal(md, &c);
    return(md);
}

/**
 * * Compares two signatures or digests.This compare works very similar to 
 * * strcmp(), and exactly like memcmp(), with a default
 * * digest size.
 * *
 * * @param d0	the first digest to compare
 * * @param d1	the second digest to compare
 * *
 * * @return 0 if d0 and d1 match. non-zero if they do NOT
 * * 	match. See memcmp().
 * */
int sigcmp(unsigned char *d0, unsigned char *d1) {
    return(memcmp(d0,d1,SIG_DIGEST_LENGTH));
} 

/**
 * * Returns a signature or digest in string format. Note that
 * * the memory for this string should be freed by the calling
 * * function/routine.
 * *
 * * @param sig	the digital signature or digest to 
 * * 		convert
 * *
 * * @return an allocated string representing the signature or digest.
 * * 	NULL is returned if the signature is NULL.
 * */
char *sig2str(unsigned char *sig) {
    int n;											// index into the digest/signature
    char *out = (char*)NULL;									// string to return

    if(sig) {											// make sure we have a digest to convert
      out = (char*)malloc(SIG_DIGEST_LENGTH*2 + 1);
      for (n = 0; n < SIG_DIGEST_LENGTH; ++n)
        snprintf(&(out[n*2]), SIG_DIGEST_LENGTH*2, "%02x", (unsigned int)sig[n]);
    }

    return out;
}
