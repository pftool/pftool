/************************************************************************************
* Name:  pfutils part of pftool
*
* Description:
*  This file contains utility functions used by pftool.c
*
* Author:  Alfred Torrez / Ben McClelland / Gary Grider / HB Chen / Aaron Torres
*
**********************************************************************************************/

#include <fcntl.h>
#include <errno.h>
#include <utime.h>

/* special includes for gpfs and dmapi */
#include <gpfs.h>
#include <dmapi.h>

#include "pfutils.h"
#include "debug.h"

#include <syslog.h>

/* Syslog related data structure */
char *ident = "PFTOOL-LOG:";		/* pflog identification */
int logopt = LOG_PID | LOG_CONS;	/* syslog options setting */
int facility = LOG_USER;				/* USER */
int priority = LOG_ERR | LOG_USER;	/* ERROR and USER */

void usage () {
	/* print usage statement */
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	printf (" \n");
	printf ("\npftool: parallel file tool utilities\n");
	printf ("1. Walk through directory tree structure and gather statistics on files and\n");
	printf ("   directories encountered.\n");
	printf ("2. Apply various data moving operationbased on the selected options \n");
	printf ("\n");
	printf ("mpirun -np totalprocesses pftool [options]\n");
	printf (" Options\n");
	printf (" --path [-p] path                                : path to start parallel tree walk (required argument)\n");
	printf (" --copypath [-c] copypath                        : destination path for data movement\n");
	printf (" --resultfile [-r] outoutresultfile              : output file where results/errors can be stored \n");
	printf (" --dirproc [-d] numreaddirproc                   : number of readd-dir processes create (default 1)\n");
	printf (" --tapeproc [-t] numtapeproc                     : number of tape operation processes created (default 0)\n");
	printf (" --quiet [-q] quietmodeflag                      : quiet mode/no verbose output, Active=1, InActive=0 (default 0)\n");
	printf (" --ifnewer [-i] newerflag                        : only copy file if source is newer Active=1, InActive=0 (default 1)\n");
	printf (" --older [-o] olderdays                          : only select file if older days (default all files)\n");
	printf (" --younger [-y] youngerdays                      : only select if file younger days (default all files)\n");
	printf (" --uid [-u] userid                               : only select if file owner is user ID (default all files)\n");
	printf (" --gid [-g] groupid                              : only select if file group is group ID (default all files)\n");
	printf (" --smaller [-s] smallsize                        : only select if file is smaller than size (default all files)\n");
	printf (" --bigger [-b] bigersize                         : only select if file is bigger than size (default all files)\n");
	printf (" --mini [-m] minisize                            : mini size below this size dont bother looking up gpfs migration\n");
	printf ("                                                   info (default dont look up any migration info)\n");
	printf ("                                                   if migration lookup is desired, Serverdmapi must be the ip address of the\n");
	printf ("                                                   dmapi daemon if not running as root or running in a chrooted environment \n");
	printf (" --jumbo [-j] jumbosize                          : jumbo size below this dont bother looking up panfs layout info\n");
	printf ("                                                   (default dont look up an layout info)\n");
	printf (" --whopping [-w] whopping                        : whopping size obove this break the file up into chunks on gpfs\n");
	printf ("                                                   (default dont chunk up any files)\n");
	printf (" --Recurse [-R] recursive                        : recursive operation down directory tree Active=1, InActive=0 (default 0)\n");
	printf (" --Maxcopysize [-M]  maxcopyszie                 : max-copy size above which files will be moved in parallel\n");
	printf ("                                                   (only if src and dest are parallel file systems) (default 1048576000) \n");
	printf (" --Zerodev [-Z] zerodevice                       : when copying do everything normal to src file but read from\n");
	printf ("                                                   /dev/zero devicei Active=1, InActive=0 (default 0)\n");
	printf (" --Testmode [-T] testingmodeg                    : if in copy mode only print out what copy activity would be done\n");
	printf ("                                                   dont actually do copy Active=1, InActive=0 (default 0) \n");
	printf (" --smallcopyblocksize [-z] blocksize             : small copy block size actual file system read/write sizes (default 65536) \n");
	printf (" --Waitideltape [-W] waitidletime                : time to wait before re-assigning a tape process to a new tape in seconds (default 30)\n");
	printf (" --Database [-D] datebase                        : database name for migration volser/seqnum lookup (default no volser lookup)\n");
	printf (" --HostDatabase [-H] hostdatebase                : database host for migration volser/seqnum lookup dotted quad ip address\n");
	printf ("                                                   (default no volser lookup)\n");
	printf (" --UserDatabase [-U] userdatabase                : database user for migration volser/seqnum lookup (default no volser lookup)\n");
	printf (" --elfpoolsize [-e] elfpoolsize                  : below this size files will be stored in Elfpool (default no pool processing)\n");
	printf (" --Elfpool [-E] elfpool                          : gpfs storage pool for files less than elfpoolsize (default no pool processing) \n");
	printf (" --largepoolsize [-l] largepoolsize              : files larger than elfpoolsize and smaller than this will be stored in Largepool\n");
	printf ("                                                   (default no pool processing) \n");
	printf (" --Largepool [-L] largepool                      : gpfs storage pool for files bigger than elfpoolsize and smaller than largepoolsize\n");
	printf ("                                                   (default no pool processing)\n");
	printf (" --Xlargepool [-X] extralargepool                : gpfs storage pool for files bigger than largepoolsize (default (no pool processing) \n");
	printf ("                                                   if pool processing desired you must specfify elfpoolsize, Elfpool, largepoolsize,\n");
	printf ("                                                   Largepool, and Xlargepool,  if pool processing desired you must also specify Serverdmapi \n");
	printf ("                                                   as the ip address of the stgpool daemon not running as root or running in a chrooted environment\n");
	printf (" --Prettyoutput [-P] prettyoutput                : Pretty output - do not print volser/seqnum/layout info or other verbose information\n");
	printf ("                                                   Active=1, InActive=0 (default 1) \n");
	printf (" --Optimizepanfslayout [-O] optimizepanfslayout  : Optimize Panfs Layout on parallel copy (currently unused\n");
	printf ("                                                   may be needed for dynamic sizing of parallel copies from/to panfs)\n");
	printf (" --Nosplit [-N] nosplit                          : Dont copy single files in parallel Active=1, InActive=0 (default 0 files will be copied in parallel)\n");
	printf (" --Inputlistfile [-I] inputlistfile              : Input a file with a list of files to examine instead of a beginning path or file \n");
	printf (" --Serverdmapi [-S] serverdmapi                  : IP address of dmapi and gpfs migration query server (default no migration/volser lookup)\n");
	printf (" --Fallbackpool [-F] fallbackpool                : GPFS storage pool to send to default stg pool (default none) \n");
	printf (" --Chunkdirs [-C] chunkdirs                      : number of directories in the fusedir to spread chunked files over (default no chunking) \n");
	printf (" --fusedir [-f] fusedir                          : fuse top level directory in which chunked files will be kept (default no chunking) \n");
	printf (" --help [-h]                                     : Print Usage information\n");
	printf (" \n");
	printf (" Using man pftool for the details of pftool information \n");
	printf (" \n");
	printf ("********************** PFTOOL USAGE ************************************************************\n");
	return;
}
