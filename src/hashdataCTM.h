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

#ifndef HASHDATA_H_INCLUDE_GUARD
#define HASHDATA_H_INCLUDE_GUARD

#include "ctm.h"
#include "pfutils.h"

// structures and types
typedef CTM  HASHDATA;					// type/structure of the data of the hash table.


// procedures and functions
void hashdata_destroy(HASHDATA **theData);
HASHDATA *hashdata_create(path_item newData, path_item* srcData);
void hashdata_update(HASHDATA *theData,path_item fileinfo);
int hashdata_filedone(HASHDATA *theData);

#endif // HASHDATA_H_INCLUDE_GUARD
