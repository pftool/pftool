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

#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "hashdataCTM.h"

/**
* Create a HASHDATA structure.
*
* @param newData	the path_item that describes the
* 			chunked file.
*
* @return a pointer to new HASHDATA structure,
* 	suitable to be assigned to a HASHTBL
* 	node. NULL is returned if there are problems
* 	allocating/creating the node.
*/
HASHDATA *hashdata_create(path_item newData) {
	long numofchnks = (long)ceil(newData.st.st_size/((double)newData.chksz));		// number of chunks this file will have
	CTM *newCTM = getCTM(newData.path,numofchnks,newData.chksz);	// the new CTM for the file. This also initializes the persistent store

	return((HASHDATA *)newCTM);
}

/**
* Destroys a HASHDATA structure.
*
* @param theData	the HASHDATA to deallocate
*/
void hashdata_destroy(HASHDATA **theData) {
	if(*theData)
	  removeCTM((CTM **)theData);					// this removes the chunk file, as well as deallocated the structure
	return;
}

/**
* Updates the HASHDATA structure. This reads given
* path_item and uses the chkidx to update the CTM
* structure apropriately.
* 
* @param theData	the HASHDATA structure to
* 			update
* @param fileinfo	the path_item to use in
* 			updating the CTM
*/
void hashdata_update(HASHDATA *theData,path_item fileinfo) {
	updateCTM((CTM *)theData,fileinfo.chkidx);			// marks the chunk transferred
	return;
}

/**
* Tests to see if a chunked file has all of its chunks
* transferred.
*
* @param theData	the HASHDATA structure to
* 			testcnt
*
* @return non-zero (i.e. TRUE) if all of the chunks have
* 	been transferred. Otherwise zero (FALSE) is 
* 	returned.
*/
int hashdata_filedone(HASHDATA *theData) {
	return(transferredCTM((CTM *)theData));
}


