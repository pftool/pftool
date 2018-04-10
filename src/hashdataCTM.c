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
HASHDATA *hashdata_create(path_item newData, path_item* srcData) {
	printf("IN HASHDATA CREATE\n");
	time_t mtime;
	char strToHash[PATHSIZE_PLUS + MARFS_DATE_STRING_MAX];
	char timestamp[MARFS_DATE_STRING_MAX];
	char* tpPtr;
	//need to construct src+mtimt hash AND get the timestamp string
	PathPtr p_work(PathFactory::create_shallow(srcData));
	snprintf(strToHash, PATHSIZE_PLUS, "%s.", p_work->path()); //first put path in to src+mtime str

	mtime = p_work->mtime();
	tpPtr = strToHash + strlen(strToHash);
	epoch_to_str(tpPtr, MARFS_DATE_STRING_MAX, &mtime); //then put mtime into src+mtime str
	
	memcpy(timestamp, srcData->timestamp, MARFS_DATE_STRING_MAX); //srcData->timestamp must have been set by copylist by now	
	printf("###src+mtime %s\n",strToHash);
	printf("###timestamp for tempfilfe %s\n", timestamp);
	long numofchnks = (long)ceil(newData.st.st_size/((double)newData.chksz));		// number of chunks this file will have
	CTM *newCTM = getCTM(newData.path,numofchnks,newData.chksz, strToHash, timestamp);	// the new CTM for the file. This also initializes the persistent store

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
	//first construct src+mtime str so that it can get written to the 
	//CTM file

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


