/*
*This material was prepared by the Los Alamos National Security, LLC (LANS) under
*Contract DE-AC52-06NA25396 with the U.S. Department of Energy (DOE). All rights
*in the material are reserved by DOE on behalf of the Government and LANS
*pursuant to the contract. You are authorized to use the material for Government
*purposes but it is not to be released or distributed to the public. NEITHER THE
*UNITED STATES NOR THE UNITED STATES DEPARTMENT OF ENERGY, NOR THE LOS ALAMOS
*NATIONAL SECURITY, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY WARRANTY, EXPRESS
*OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR RESPONSIBILITY FOR THE ACCURACY,
*COMPLETENESS, OR USEFULNESS OF ANY INFORMATION, APPARATUS, PRODUCT, OR PROCESS
*DISCLOSED, OR REPRESENTS THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
*/


//
// Defines for the Digital Signature implementation
//

#ifndef	__SIG_H
#define __SIG_H

#include <stdint.h>

#if defined(__APPLE__)					// need to test an apple build ... cds 03/2018
#  define COMMON_DIGEST_FOR_OPENSSL
#  include <CommonCrypto/CommonDigest.h>
#  define SHA1 CC_SHA1

typedef MD5_CTX SigCTX;					// If we are on a mac, then only use MD5

#  define SIG_DIGEST_LENGTH	(MD5_DIGEST_LENGTH)

#  define SigInit(C)		( MD5_Init(C) )
#  define SigUpdate(C,D,L)	( MD5_Update(C,D,L) )
#  define SigFinal(D,C)		( MD5_Final(D,C) )
							// Allows for the use of different digital digest/signature alorithms for non-apple platforms
#elif DIGEST == MD2					// Use MD2

#  include <openssl/md2.h>

typedef MD2_CTX SigCTX;

#  define SIG_DIGEST_LENGTH	(MD2_DIGEST_LENGTH)

#  define SigInit(C)		( MD2_Init(C) )
#  define SigUpdate(C,D,L)	( MD2_Update(C,D,L) )
#  define SigFinal(D,C)		( MD2_Final(D,C) )

#else 							// Default to MD5

#  include <openssl/md5.h>

typedef MD5_CTX SigCTX;

#  define SIG_DIGEST_LENGTH	(MD5_DIGEST_LENGTH)

#  define SigInit(C)		( MD5_Init(C) )
#  define SigUpdate(C,D,L)	( MD5_Update(C,D,L) )
#  define SigFinal(D,C)		( MD5_Final(D,C) )

#endif // Signature Implenetation

#define SIG_COMPUTE_CHUNK	512			// could be bigger! - cds 03/2018

// Function Declarations
unsigned char *signature(const uint8_t *buf, const size_t buflen);
int sigcmp(unsigned char *d0, unsigned char *d1);
char *sig2str(unsigned char *sig);

#endif //__SIG_H
