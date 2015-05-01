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

#include <stdlib.h>
#include <string.h>

#include "hashdata.h"

/**
* Create a HASHDATA structure.
*
* @param newData	the data to add 
*
* @return a pointer to new HASHDATA structure,
* 	suitable to be assigned to a HASHTBL
* 	node. NULL is returned if there are problems
* 	allocating/creating the node.
*/
HASHDATA *hashdata_create(size_t newData) {
	HASHDATA *new;						// the new structure

	new = (HASHDATA *)malloc(sizeof(HASHDATA));
	memset(new,0,sizeof(HASHDATA));
	
	new->cursize = newData;
	return(new);
}

/**
* Destroys a HASHDATA structure.
*
* @param theData	the HASHDATA to deallocate
*/
void hashdata_destroy(HASHDATA **theData) {
	if(*theData) {
	  free(*theData);
	  *theData = (HASHDATA *)NULL;
	}
	return;
}

