/*
 * *This material was prepared by the Los Alamos National Security, LLC (LANS) under
 * *Contract DE-AC52-06NA25396 with the U.S. Department of Energy (DOE). All rights
 * *in the material are reserved by DOE on behalf of the Government and LANS
 * *pursuant to the contract. You are authorized to use the material for Government
 * *purposes but it is not to be released or distributed to the public. NEITHER THE
 * *UNITED STATES NOR THE UNITED STATES DEPARTMENT OF ENERGY, NOR THE LOS ALAMOS
 * *NATIONAL SECURITY, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY WARRANTY, EXPRESS
 * *OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR RESPONSIBILITY FOR THE ACCURACY,
 * *COMPLETENESS, OR USEFULNESS OF ANY INFORMATION, APPARATUS, PRODUCT, OR PROCESS
 * *DISCLOSED, OR REPRESENTS THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
 * */


/**
* Header file to hold CTM (Chunk Transfer Metadata) implementation
* declarations
*/

#ifndef      __CTM_IMPL_H
#define      __CTM_IMPL_H

// CTF (Chunk Transfer File) Function Declarations
char *genCTFFilename(const char *transfilename);
int foundCTF(const char *transfilename);
void registerCTF(CTM_IMPL *ctmimplptr);
int unlinkCTF(const char *chnkfname);

// CTA (Chunk Transfer Artributes) Function Declarations
int deleteCTA(const char *chnkfname);
int foundCTA(const char *transfilename);
void registerCTA(CTM_IMPL *ctmimplptr);

#endif //__CTM_IMPL_H
