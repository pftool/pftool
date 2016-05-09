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

#include "config.h"
#include <fcntl.h>
#include <errno.h>

#include "pfutils.h"
#include "Path.h"
#include "debug.h"

#include <syslog.h>
#include <signal.h>

#ifdef THREADS_ONLY
#include <pthread.h>
#include "mpii.h"
#define MPI_Abort MPY_Abort
#define MPI_Pack MPY_Pack
#define MPI_Unpack MPY_Unpack
#endif


void usage () {
    // print usage statement
    printf ("********************** PFTOOL USAGE ************************************************************\n");
    printf ("\n");
    printf ("\npftool: parallel file tool utilities\n");
    printf ("1. Walk through directory tree structure and gather statistics on files and\n");
    printf ("   directories encountered.\n");
    printf ("2. Apply various data moving operations based on the selected options \n");
    printf ("\n");
    printf ("mpirun -np <totalprocesses> pftool [options]\n");
    printf ("\n");
    printf (" Options\n");
    printf (" [-p]         path to start parallel tree walk (required argument)\n");
    printf (" [-c]         destination path for data movement\n");
    printf (" [-j]         unique jobid for the pftool job\n");
    printf (" [-w]         work type: { 0=copy | 1=list | 2=compare}\n");
    printf (" [-i]         process paths in a file list instead of walking the file system\n");
    printf (" [-s]         block size for COPY and COMPARE\n");
    printf (" [-C]         file size to start chunking (for N:1)\n");
    printf (" [-S]         chunk size for COPY\n");
    printf (" [-n]         operate on file if different\n");
    printf (" [-r]         recursive operation down directory tree\n");
    printf (" [-t]         specify file system type of destination file/directory\n");
    printf (" [-l]         turn on logging to /var/log/mesages\n");
    printf (" [-P]         force destination filesystem to be treated as parallel\n");
    printf (" [-M]         perform block-compare, default: metadata-compare\n");
    printf (" [-v]         user verbose output\n");
    printf (" [-h]         print Usage information\n");

    printf("\n");
    printf("      [if configured with --enable-fusechunker\n");
    printf (" [-f]         path to FUSE directory\n");
    printf (" [-d]         number of directories used for FUSE backend\n");
    printf (" [-W]         file size to start FUSE chunking\n");
    printf (" [-A]         FUSE chunk size for copy\n");
    printf("\n");

    printf("      [if configured with --enable-syndata\n");
    printf (" [-X]         specify a synthetic data pattern file or constant. default: none\n");
    printf (" [-x]         synthetic file size. If specified, file(s) will be synthetic data of specified size\n");
    printf (" \n");

    printf ("********************** PFTOOL USAGE ************************************************************\n");
    return;
}

// print the mode <aflag> into buffer <buf> in a regular 'pretty' format
char *printmode (mode_t aflag, char *buf) {

    static int m0[] = { 1, S_IREAD  >> 0, 'r', '-' };
    static int m1[] = { 1, S_IWRITE >> 0, 'w', '-' };
    static int m2[] = { 3, S_ISUID | S_IEXEC, 's', S_IEXEC, 'x', S_ISUID, 'S', '-' };

    static int m3[] = { 1, S_IREAD  >> 3, 'r', '-' };
    static int m4[] = { 1, S_IWRITE >> 3, 'w', '-' };
    static int m5[] = { 3, S_ISGID | (S_IEXEC >> 3), 's',
                        S_IEXEC >> 3, 'x', S_ISGID, 'S', '-'
                      };
    static int m6[] = { 1, S_IREAD  >> 6, 'r', '-' };
    static int m7[] = { 1, S_IWRITE >> 6, 'w', '-' };
    static int m8[] = { 3, S_ISVTX | (S_IEXEC >> 6), 't', S_IEXEC >> 6, 'x', S_ISVTX, 'T', '-' };
    static int *m[] = { m0, m1, m2, m3, m4, m5, m6, m7, m8 };

    int i, j, n;
    int *p = (int *) 1;
    buf[0] = S_ISREG (aflag) ? '-' : S_ISDIR (aflag) ? 'd' : S_ISLNK (aflag) ? 'l' : S_ISFIFO (aflag) ? 'p' : S_ISCHR (aflag) ? 'c' : S_ISBLK (aflag) ? 'b' : S_ISSOCK (aflag) ? 's' : '?';

    for (i = 0; i <= 8; i++) {
        for (n = m[i][0], j = 1; n > 0; n--, j += 2) {
            p = m[i];
            if ((aflag & p[j]) == p[j]) {
                j++;
                break;
            }
        }
        buf[i + 1] = p[j];
    }
    buf[10] = '\0';
    return buf;
}

/**
* This function walks the path, and creates all elements in 
* the path as a directory - if they do not exist. It
* basically does a "mkdir -p" programatically.
*
* @param thePath	the path to test and create
* @param perms		the permission mode to use when
* 			creating directories in this path
*
* @return 0 if all directories are succesfully created.
* 	errno (i.e. non-zero) if there is an error. 
* 	See "man -s 2 mkdir" for error description.
*/
int mkpath(char *thePath, mode_t perms) {
	char *slash = thePath;				// point at the current "/" in the path
	struct stat sbuf;				// a buffer to hold stat information
	int save_errno;					// errno from mkdir()

	while( *slash == '/') slash++;			// burn through any leading "/". Note that if no leading "/",
							// then thePath will be created relative to CWD of process.
	while(slash = strchr(slash,'/')) {		// start parsing thePath
	  *slash = '\0';
	  
	  if(stat(thePath,&sbuf)) {			// current path element cannot be stat'd - assume does not exist
	    if(mkdir(thePath,perms)) {			// problems creating the directory - clean up and return!
	      save_errno = errno;			// save off errno - in case of error...
	      *slash = '/';
	      return(save_errno);
	    }
	  }
	  else if (!S_ISDIR(sbuf.st_mode)) {		// element exists but is NOT a directory
	    *slash = '/';
	    return(ENOTDIR);
	  }
	  *slash = '/';slash++;				// increment slash ...
	  while( *slash == '/') slash++;		// burn through any blank path elements
	} // end mkdir loop

	if(stat(thePath,&sbuf)) {			// last path element cannot be stat'd - assume does not exist
	  if(mkdir(thePath,perms))			// problems creating the directory - clean up and return!
	    return(save_errno = errno);			// save off errno - just to be sure ...
	}
	else if (!S_ISDIR(sbuf.st_mode))		// element exists but is NOT a directory
	  return(ENOTDIR);

	return(0);
}

void hex_dump_bytes (char *b, int len, char *outhexbuf) {
    short str_index;
    char smsg[64];
    char tmsg[3];
    unsigned char *ptr;
    int start = 0;
    ptr = (unsigned char *) (b + start);  /* point to buffer location to start  */
    /* if last frame and more lines are required get number of lines */
    memset (smsg, '\0', 64);
    for (str_index = 0; str_index < 28; str_index++) {
        sprintf (tmsg, "%02X", ptr[str_index]);
        strncat (smsg, tmsg, 2);
    }
    sprintf (outhexbuf, "%s", smsg);
}

/**
* Low Level utility function to write a field of a data
* structure - any data structure.
*
* @param fd		the open file descriptor
* @param start		the starting memory address
* 			(pointer) of the field
* @param len		the length of the filed in bytes
*
* @return number of bytes written, If return
* 	is < 0, then there were problems writing,
* 	and the number can be taken as the errno.
*/
ssize_t write_field(int fd, void *start, size_t len) {
	size_t  n;					// number of bytes written for a given call to write()
	ssize_t tot = 0;				// total number of bytes written
	char*   wstart = (char*)start;				// the starting point in the buffer
	size_t  wcnt = len;				// the running count of bytes to write

	while(wcnt > 0) {
	  if(!(n=write(fd,wstart,wcnt)))		// if nothing written -> assume error
	    return((ssize_t)-errno);
	  tot += n;
	  wstart += n;					// increment the start address by n
	  wcnt -= n;					// decreamnt byte count by n
	}

	return(tot);
}

#ifdef TAPE
int read_inodes (const char *fnameP, gpfs_ino_t startinode, gpfs_ino_t endinode, int *dmarray) {
    const gpfs_iattr_t *iattrP;
    gpfs_iscan_t *iscanP = NULL;
    gpfs_fssnap_handle_t *fsP = NULL;
    int rc = 0;
    fsP = gpfs_get_fssnaphandle_by_path (fnameP);
    if (fsP == NULL) {
        rc = errno;
        fprintf (stderr, "gpfs_get_fshandle_by_path: %s\n", strerror (rc));
        goto exit;
    }
    iscanP = gpfs_open_inodescan (fsP, NULL, NULL);
    if (iscanP == NULL) {
        rc = errno;
        fprintf (stderr, "gpfs_open_inodescan: %s\n", strerror (rc));
        goto exit;
    }
    if (startinode > 0) {
        rc = gpfs_seek_inode (iscanP, startinode);
        if (rc != 0) {
            rc = errno;
            fprintf (stderr, "gpfs_seek_inode: %s\n", strerror (rc));
            goto exit;
        }
    }
    while (1) {
        rc = gpfs_next_inode (iscanP, endinode, &iattrP);
        if (rc != 0) {
            rc = errno;
            fprintf (stderr, "gpfs_next_inode: %s\n", strerror (rc));
            goto exit;
        }
        if ((iattrP == NULL) || (iattrP->ia_inode > endinode)) {
            break;
        }
        if (iattrP->ia_xperm & GPFS_IAXPERM_DMATTR) {
            dmarray[0] = 1;
        }
    }
exit:
    if (iscanP) {
        gpfs_close_inodescan (iscanP);
    }
    if (fsP) {
        gpfs_free_fssnaphandle (fsP);
    }
    return rc;
}

//dmapi lookup
int dmapi_lookup (char *mypath, int *dmarray, char *dmouthexbuf) {
    char *version;
    char *session_name = "lookupdmapi";
    static dm_sessid_t dump_dmapi_session = DM_NO_SESSION;
    typedef long long offset_t;
    void *dmhandle = NULL;
    size_t dmhandle_len = 0;
    u_int nelemr;
    dm_region_t regbufpr[4000];
    u_int nelempr;
    size_t dmattrsize;
    char dmattrbuf[4000];
    size_t dmattrsizep;
    //struct dm_attrlist my_dm_attrlist[20];
    struct dm_attrlist my_dm_attrlistM[20];
    struct dm_attrlist my_dm_attrlistP[20];
    //struct dm_attrname my_dm_attrname;
    struct dm_attrname my_dm_attrnameM;
    struct dm_attrname my_dm_attrnameP;
    char localhexbuf[128];
    if (dm_init_service (&version) < 0) {
        printf ("Cant get a dmapi session\n");
        exit (-1);
    }
    if (dm_create_session (DM_NO_SESSION, session_name, &dump_dmapi_session) != 0) {
        printf ("create_session \n");
        exit (-1);
    }
    //printf("-------------------------------------------------------------------------------------\n");
    //printf("created new DMAPI session named '%s' for %s\n",session_name,version);
    if (dm_path_to_handle ((char *) mypath, &dmhandle, &dmhandle_len) != 0) {
        goto done;
    }
    /* I probably should get more managed regions and check them all but
       I dont know how to find out how many I will have total
     */
    nelemr = 1;
    if (dm_get_region (dump_dmapi_session, dmhandle, dmhandle_len, DM_NO_TOKEN, nelemr, regbufpr, &nelempr) != 0) {
        printf ("dm_get_region failed\n");
        goto done;
    }
    PRINT_DMAPI_DEBUG ("regbufpr: number of managed regions = %d \n", nelempr);
    PRINT_DMAPI_DEBUG ("regbufpr.rg_offset = %lld \n", regbufpr[0].rg_offset);
    PRINT_DMAPI_DEBUG ("regbufpr.rg_size = %lld \n", regbufpr[0].rg_size);
    PRINT_DMAPI_DEBUG ("regbufpr.rg_flags = %u\n", regbufpr[0].rg_flags);
    if (regbufpr[0].rg_flags > 0) {
        if (regbufpr[0].rg_flags & DM_REGION_READ) {
            PRINT_DMAPI_DEBUG ("regbufpr: File %s is migrated - dmapi wants to be notified on at least read for this region at offset %lld\n", mypath, regbufpr[0].rg_offset);
            dmarray[2] = 1;
            dmattrsize = sizeof (my_dm_attrlistM);
            if (dm_getall_dmattr (dump_dmapi_session, dmhandle, dmhandle_len, DM_NO_TOKEN, dmattrsize, my_dm_attrlistM, &dmattrsizep) != 0) {
                PRINT_DMAPI_DEBUG ("dm_getall_dmattr failed path %s\n", mypath);
                goto done;
            }
            //strncpy((char *) my_dm_attrlistM[0].al_name.an_chars,"\n\n\n\n\n\n\n",7);
            //strncpy((char *) my_dm_attrlistM[0].al_name.an_chars,"IBMObj",6);
            PRINT_DMAPI_DEBUG ("M dm_getall_dmattr attrs %x size %ld\n", my_dm_attrlistM->al_name.an_chars[0], dmattrsizep);
            dmattrsize = sizeof (dmattrbuf);
            strncpy ((char *) my_dm_attrnameM.an_chars, "IBMObj", 6);
            PRINT_DMAPI_DEBUG ("MA dm_get_dmattr attr 0 %s\n", my_dm_attrnameM.an_chars);
            if (dm_get_dmattr (dump_dmapi_session, dmhandle, dmhandle_len, DM_NO_TOKEN, &my_dm_attrnameM, dmattrsize, dmattrbuf, &dmattrsizep) != 0) {
                goto done;
            }
            hex_dump_bytes (dmattrbuf, 28, localhexbuf);
            PRINT_DMAPI_DEBUG ("M dmapi_lookup localhexbuf %s\n", localhexbuf);
            strncpy (dmouthexbuf, localhexbuf, 128);
        }
        else {
            // This is a pre-migrated file
            //
            PRINT_DMAPI_DEBUG ("regbufpr: File %s is premigrated  - dmapi wants to be notified on write and/or trunc for this region at offset %lld\n", mypath, regbufpr[0].rg_offset);
            // HB 0324-2009
            dmattrsize = sizeof (my_dm_attrlistP);
            if (dm_getall_dmattr (dump_dmapi_session, dmhandle, dmhandle_len, DM_NO_TOKEN, dmattrsize, my_dm_attrlistP, &dmattrsizep) != 0) {
                PRINT_DMAPI_DEBUG ("P dm_getall_dmattr failed path %s\n", mypath);
                goto done;
            }
            //strncpy((char *) my_dm_attrlistP[0].al_name.an_chars,"IBMPMig",7);
            PRINT_DMAPI_DEBUG ("P dm_getall_dmattr attrs %x size %ld\n", my_dm_attrlistP->al_name.an_chars[0], dmattrsizep);
            dmattrsize = sizeof(dmattrbuf);
            strncpy ((char *) my_dm_attrnameP.an_chars, "IBMPMig", 7);
            PRINT_DMAPI_DEBUG ("PA dm_get_dmattr attr 0 %s\n", my_dm_attrnameP.an_chars);
            if (dm_get_dmattr (dump_dmapi_session, dmhandle, dmhandle_len, DM_NO_TOKEN, &my_dm_attrnameP, dmattrsize, dmattrbuf, &dmattrsizep) != 0) {
                goto done;
            }
            hex_dump_bytes (dmattrbuf, 28, localhexbuf);
            strncpy (dmouthexbuf, localhexbuf, 128);
            dmarray[1] = 1;
        }
    }
    else {
        // printf("regbufpr: File is resident - no managed regions\n");
        dmarray[0] = 1;
    }
done:
    /* I know this done goto is crap but I am tired
       we now free up the dmapi resources and set our uid back to
       the original user
     */
    dm_handle_free (dmhandle, dmhandle_len);
    if (dm_destroy_session (dump_dmapi_session) != 0) {
    }
    //printf("-------------------------------------------------------------------------------------\n");
    return (0);
}
#endif



// remove trailing chars w/out repeated calls to strlen();
// inline
void trim_trailing(int ch, char* path) {
   if (path) {
      for (size_t pos=strlen(path) -1; ((pos >= 0) && (path[pos] == ch)); --pos) {
         path[pos] = '\0';
      }
   }
}


// Install path into <item>.
// We assume <base_path> has size at least PATHSIZE_PLUS.
void get_base_path(char*            base_path,
                   const path_item* item,
                   int              wildcard) { // (<wildcard> is boolean)

    char dir_name[PATHSIZE_PLUS];
    struct stat st;
    int rc;
    char* path = (char*)item->path;

    ////#ifdef PLFS
    ////    rc = plfs_getattr(NULL, path, &st, 0);
    ////    if (rc != 0){
    ////#endif
    ////        rc = lstat(path, &st);
    ////#ifdef PLFS
    ////    }
    ////#endif
    ////    if (rc < 0) {
    ////        fprintf(stderr, "Failed to stat path %s\n", path);
    ////        MPI_Abort(MPI_COMM_WORLD, -1);
    ////    }
    //    item->ftype = TBD;          // so PathFactory knows path hasn't been classified
    PathPtr p(PathFactory::create(item));
    if (! p->stat()) {
       fprintf(stderr, "get_base_path -- Failed to stat path %s\n", path);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    st = p->st();


    // dirname() may alter its argument
    char* path_copy = strdup(path);
    strncpy(dir_name, dirname(path_copy), PATHSIZE_PLUS);
    free(path_copy);

    if (strncmp(".", dir_name, PATHSIZE_PLUS == 0) && S_ISDIR(st.st_mode)) {
        strncpy(base_path, path, PATHSIZE_PLUS);
    }
    else if (S_ISDIR(st.st_mode) && wildcard == 0) {
        strncpy(base_path, path, PATHSIZE_PLUS);
    }
    else {
        strncpy(base_path, dir_name, PATHSIZE_PLUS);
    }
    trim_trailing('/', base_path);
}


// To the tail of <dest_path>, add '/' if needed, then append the last part
// of the path in <beginning_node> (i.e. what 'basename' would return).
// Put the result into dest_node->path.
void get_dest_path(path_item*        dest_node, // fill this in
                   const char*       dest_path, // from command-line arg
                   const path_item*  beginning_node,
                   int               makedir,
                   int               num_paths,
                   struct options&   o) {
    int         rc;
    struct stat beg_st;
    struct stat dest_st;
    char        temp_path[PATHSIZE_PLUS];
    char*       result = dest_node->path;
    char*       path_slice;

    memset(dest_node, 0, sizeof(path_item) - PATHSIZE_PLUS + 1); // zero-out header-fields
    strncpy(result, dest_path, PATHSIZE_PLUS);                // install dest_path
    dest_node->ftype = TBD;                     // we will figure out the file type later

    strncpy(temp_path, beginning_node->path, PATHSIZE_PLUS);
    trim_trailing('/', temp_path);

    //recursion special cases
    if (o.recurse
        && (strncmp(temp_path, "..", PATHSIZE_PLUS) != 0)
        && (o.work_type != COMPAREWORK)) {

        beg_st = beginning_node->st;

        ////#ifdef PLFS
        ////        rc = plfs_getattr(NULL, dest_path, &dest_st, 0);
        ////        if (rc != 0){
        ////#endif
        ////           rc = lstat(dest_path, &dest_st);
        ////#ifdef PLFS
        ////        }
        ////#endif
        ////        if ((rc >= 0)
        ////            && S_ISDIR(dest_st.st_mode)
        ////            && S_ISDIR(beg_st.st_mode)
        ////            && (num_paths == 1))
        PathPtr d_dest(PathFactory::create(dest_path));
        dest_st = d_dest->st();
        if (d_dest->exists()
            && S_ISDIR(dest_st.st_mode)
            && S_ISDIR(beg_st.st_mode)
            && (num_paths == 1))

        {
            // append '/' to result
            if (result[strlen(result)-1] != '/') {
                strncat(result, "/", PATHSIZE_PLUS);
            }

            // append tail-end of beginning_node's path
            if (strstr(temp_path, "/") == NULL) {
                path_slice = (char *)temp_path;
            }
            else {
                path_slice = strrchr(temp_path, '/') + 1;
            }
            strncat(result, path_slice, PATHSIZE_PLUS - strlen(result) -1);
        }
    }

    // update the stat-struct, in the final result
    ////    rc = lstat(result, &dest_st);
    ////    if (rc >= 0) {
    ////        dest_node->st = dest_st;
    ////    }
    ////    else {
    ////        dest_node->st.st_mode = 0;
    ////    }
    PathPtr d_result(PathFactory::create_shallow(dest_node));
    d_result->stat();
}



// Find the last "directory" component in <src_node>->path.  Append this
// directory component onto a copy of <dest_node>->path, after first adding
// a slash.  Put the result into <output_path>.
//
// If o.recurse is non-zero, then, instead of the final component of
// src_node, use the part of src_node that extends beyond <base_path>.
//
// NOTE:  We assume <output_path> has size at least PATHSIZE_PLUS
void get_output_path(path_item*        out_node, // fill in out_node.path
                     const char*       base_path,
                     const path_item*  src_node,
                     const path_item*  dest_node,
                     struct options&   o) {

    const char*  path_slice;
    int          path_slice_duped = 0;

    // clear out possibly-uninitialized stat-field
    //    memset(&out_node->st, 0, sizeof(struct stat));
    //    out_node->path[0] = 0;
    memset(out_node, 0, sizeof(path_item) - PATHSIZE_PLUS +1);

    // marfs may want to know chunksize
    out_node->chksz  = dest_node->chksz;
    out_node->chkidx = dest_node->chkidx;

    //remove trailing slash(es)
    strncpy(out_node->path, dest_node->path, PATHSIZE_PLUS);
    trim_trailing('/', out_node->path);
    size_t remain = PATHSIZE_PLUS - strlen(out_node->path) -1;

    //path_slice = strstr(src_path, base_path);
    if (o.recurse == 0) {
        const char* last_slash = strrchr(src_node->path, '/');
        if (last_slash) {
            path_slice = last_slash +1;
        }
        else {
            path_slice = (char *) src_node->path;
        }
    }
    else {
        if (strncmp(base_path, ".", PATHSIZE_PLUS) == 0) {
            path_slice = (char *) src_node->path;
        }
        else {
            path_slice = strdup(src_node->path + strlen(base_path) + 1);
            path_slice_duped = 1;
        }
    }

    if (S_ISDIR(dest_node->st.st_mode)) {
        strncat(out_node->path, "/", remain);
        remain -= 1;
        strncat(out_node->path, path_slice, remain);
    }
    if (path_slice_duped) {
       free((void*)path_slice);
    }
}

int one_byte_read(const char *path) {
    int fd, bytes_processed;
    char data;
    int rc = 0;
    char errormsg[MESSAGESIZE];
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        sprintf(errormsg, "Failed to open file %s for read", path);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    bytes_processed = read(fd, &data, 1);
    if (bytes_processed != 1) {
        sprintf(errormsg, "%s: Read %d bytes instead of %d", path, bytes_processed, 1);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    rc = close(fd);
    if (rc != 0) {
        sprintf(errormsg, "Failed to close file: %s", path);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    return 0;
}

//take a src, dest, offset and length. Copy the file and return >=0 on
//success, -1 on failure.  [0 means copy succeeded, 1 means a "deemed"
//success.]
int copy_file(path_item*    src_file,
              path_item*    dest_file,
              size_t        blocksize,
              int           rank,
              SyndataBufPtr synbuf)
{
    //MPI_Status status;
    int         rc;
    size_t      completed = 0;
    char *      buf = NULL;
    char        errormsg[MESSAGESIZE];
    int         err = 0;        // non-zero -> close src/dest, free buf
    //FILE *src_fd;
    //FILE *dest_fd;
    int         flags;
    //MPI_File src_fd;
    //MPI_File dest_fd;
    ////    int         src_fd;
    ////    int         dest_fd = -1;
    //    off_t       offset = src_file->offset;
    //    size_t      length = src_file->length;
    off_t offset = (src_file->chkidx * src_file->chksz);
    off_t length = (((offset + src_file->chksz) > src_file->st.st_size)
                    ? (src_file->st.st_size - offset)
                    : src_file->chksz);

    ////#ifdef PLFS
    ////    int         pid = getpid();
    ////    Plfs_fd *   plfs_src_fd = NULL;
    ////    Plfs_fd *   plfs_dest_fd = NULL;
    ////#endif
    ssize_t     bytes_processed = 0;
    int         retry_count;
    int         success = 0;

    //symlink
    char        link_path[PATHSIZE_PLUS];
    int         numchars;

    PathPtr p_src( PathFactory::create_shallow(src_file));
    PathPtr p_dest(PathFactory::create_shallow(dest_file));

    // If source is a link, create similar link on the destination-side.
    //can't be const for MPI_IO
    //
    // NOTE: The only way this can be a link is if it's on a quasi-POSIX
    //       system.  So we can just readlink(), to get the link-target.
    //       But the destination could be anything, so we need a
    //       Path::symlink() to implement that.
    if (S_ISLNK(src_file->st.st_mode)) {

        // <link_path> = name of link-destination
        numchars = p_src->readlink(link_path, PATHSIZE_PLUS);
        if (numchars < 0) {
            sprintf(errormsg, "Failed to read link %s", src_file->path);
            errsend(NONFATAL, errormsg);
            return -1;
        }

        ////        rc = symlink(link_path, dest_file->path);
        ////        if (rc < 0)
        if (! p_dest->symlink(link_path))
        {
           sprintf(errormsg, "Failed to create symlink %s -> %s", dest_file->path, link_path);
           errsend(NONFATAL, errormsg);
           return -1;
        }

        if (update_stats(src_file, dest_file) != 0) {
            return -1;
        }
        return 0;
    }

    //a file less than 1 MB
    if (length < blocksize) { // a file < blocksize in size
        blocksize = length;
    }
    if (blocksize) {
        buf = (char*)malloc(blocksize * sizeof(char));
        if (! buf) {
           errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                       blocksize, p_src->path());
           return -1;
        }

        memset(buf, '\0', blocksize);
    }

    // OPEN source for reading (binary mode)
    //
    //MPI_File_read(src_fd, buf, 2, MPI_BYTE, &status);
    //rc = MPI_File_open(MPI_COMM_SELF, source_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &src_fd);


#ifdef GEN_SYNDATA
    if (!syndataExists(synbuf)) {
#endif

        ////#ifdef PLFS
        ////        if (src_file->ftype == PLFSFILE){
        ////            src_fd = plfs_open(&plfs_src_fd, src_file->path, O_RDONLY, pid+rank, src_file->st.st_mode, NULL);
        ////        }
        ////        else {
        ////#endif
        ////            src_fd = open(src_file->path, O_RDONLY);
        ////#ifdef PLFS
        ////        }
        ////#endif
        ////        if (src_fd < 0) {
        ////            sprintf(errormsg, "Failed to open file %s for read", src_file->path);
        ////            errsend(NONFATAL, errormsg);
        ////            return -1;
        ////        }
       if (! p_src->open(O_RDONLY, src_file->st.st_mode, offset, length)) {
           errsend_fmt(NONFATAL, "Failed to open file %s for read\n", p_src->path());
           if (buf)
              free(buf);
           return -1;
        }

#ifdef GEN_SYNDATA
    }
#endif
    PRINT_IO_DEBUG("rank %d: copy_file() Copying chunk "
                   "index %d. offset = %ld   length = %ld   blocksize = %ld\n",
                   rank, src_file.chkidx, offset, length, blocksize);
    // OPEN destination for writing
    //
    //rc = MPI_File_open(MPI_COMM_SELF, destination_file, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &dest_fd);

    // create appropriate flags
    if ((src_file->st.st_size <= length) || (dest_file->fstype != PAN_FS)) {
       // no chunking or not writing to PANFS - cds 6/2014
       flags = O_WRONLY | O_CREAT;
       PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT",
                       dest_file->fstype_to_str());
    }
    else {
       // Panasas FS needs O_CONCURRENT_WRITE set for file writes - cds 6/2014
       flags = O_WRONLY | O_CREAT | O_CONCURRENT_WRITE;
       PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT | O_CONCURRENT_WRITE",
                       dest_file->fstype_to_str());
    }

    // do the open
    ////#ifdef PLFS
    ////    if (src_file->dest_ftype == PLFSFILE){
    ////        dest_fd = plfs_open(&plfs_dest_fd, dest_file->path, flags, pid+rank, src_file->st.st_mode, NULL);
    ////    }
    ////    else{
    ////#endif
    ////         // dest_fd = open(dest_file->path, flags, 0600);  // 0600 not portable?
    ////         dest_fd = open(dest_file->path, flags, (S_IRUSR | S_IWUSR));
    ////#ifdef PLFS
    ////    }
    ////#endif
    ////    if (dest_fd < 0) {
    ////        sprintf(errormsg, "Failed to open file %s for write (errno = %d)", dest_file->path, errno);
    ////        errsend(NONFATAL, errormsg);
    ////        err = 1; // return -1;
    ////    }

    // EXPERIMENT: For N:N, with all files the same size, all workers trying to open files
    //             at the same time means there are periods where nothing is happening for
    //             ~4 sec, while we wait for the server.  What if tasks stagger the opens?
    //             [Single thread works at ~200 MB/s, so w/~48 workers, on 3 FTAs, this will
    //             give each FTA two alternating sets of workers offset by ~5 sec.]
    //sleep((rank % 2) * 2);

    // give destination the same mode as src, (access-bits only)
    mode_t dest_mode = src_file->st.st_mode & (S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO);
    if (! p_dest->open(flags, dest_mode, offset, length)) {
       errsend_fmt(NONFATAL, "Failed to open file %s for write (%s)\n",
                   p_dest->path(), p_dest->strerror());

       p_src->close();
       if (buf)
          free(buf);
       return -1;
    }


    // copy contents from source to destination
    while (completed != length) {

        //1 MB is too big
        if ((length - completed) < blocksize) {
            blocksize = (length - completed);
        }

        // Wasteful?  If we fail to read blocksize, we'll have a problem
        // anyhow.  And if we succeed, then we'll wipe this all out with
        // the data, anyhow.  [See also memsets in compare_file()]
        //
        //        memset(buf, '\0', blocksize);


        // READ data from source (or generate it synthetically)
        //
        //rc = MPI_File_read_at(src_fd, completed, buf, blocksize, MPI_BYTE, &status);

        PRINT_IO_DEBUG("rank %d: copy_file() Copy of %d bytes complete for file %s\n",
                       rank, bytes_processed, dest_file.path);
#ifdef GEN_SYNDATA
        if (syndataExists(synbuf)) {
           int buflen = blocksize * sizeof(char); // Make sure buffer length is the right size!

# if 0
           // Don't waste time filling the buffer.  We just want a fast
           // way to create source data


           if(rc = syndataFill(synbuf,buf,buflen)) {
              sprintf(errormsg, "Failed to copy from synthetic data buffer. err = %d", rc);
              errsend(NONFATAL, errormsg);
              err = 1; break;  // return -1
           }
# endif

           bytes_processed = buflen; // On a successful call to syndataFill(), bytes_processed equals 0
        }
        else {
#endif

           ////#ifdef PLFS
           ////           if (src_file->ftype == PLFSFILE) {
           ////              // ported to PLFS 2.5
           ////              /// bytes_processed = plfs_read(plfs_src_fd, buf, blocksize, offset+completed);
           ////              plfs_error_t err = plfs_read(plfs_src_fd, buf, blocksize, offset+completed, &bytes_processed);
           ////           }
           ////           else {
           ////#endif
           ////              bytes_processed = pread(src_fd, buf, blocksize, offset+completed);
           ////#ifdef PLFS
           ////           }
           ////#endif
           bytes_processed = p_src->read(buf, blocksize, offset+completed);

           // ---------------------------------------------------------------------------
           // MARFS EXPERIMENT.  We are seeing stalls on some reads from
           // object-servers, in the case of many concurrent requests.  To
           // deal with that, we'll try sending a new request for the part
           // of the data we haven't received.  Large number of retries
           // allows more-aggressive (i.e. shorter) timeouts waiting for a
           // blocksize read from the stream.
           //
           // This problem could also be a single object-server that is
           // unresponsive.  In the case of a MarFS configuration using
           // host-randomization, retrying the read will also have the
           // chance of targeting a different server.
           //
           // TBD: In a POSIX context, this is overkill.  You'd rather just
           // retry the read.  For the case we're seeing with
           // object-servers, we're skipping straight to what the problem
           // seems to be, there, which is that we need to issue a fresh
           // request, which is done implicitly by closing and re-opening.
           // ---------------------------------------------------------------------------
           retry_count = 0;
           while ((bytes_processed != blocksize) && (retry_count++ < 5)) {

              errsend_fmt(NONFATAL, "(RETRY) %s, at %lu+%lu, len %lu\n",
                          p_src->path(), offset, completed, blocksize);

              if (! p_src->close()) {
                 errsend_fmt(NONFATAL, "(RETRY) Failed to close src file: %s (%s)\n",
                             p_src->path(), p_src->strerror());
                 // err = 1;
              }

              if (! p_src->open(O_RDONLY, src_file->st.st_mode, offset+completed, length-completed)) {
                 errsend_fmt(NONFATAL, "(RETRY) Failed to open %s for read, off %lu+%lu\n",
                             p_src->path(), offset, completed);
                 if (buf)
                    free(buf);
                 return -1;
              }

              // try again ...
              bytes_processed = p_src->read(buf, blocksize, offset+completed);
           }
           if (retry_count && (bytes_processed == blocksize)) {
              errsend_fmt(NONFATAL, "(RETRY) success for %s, off %lu+%lu (retries = %d)\n",
                          p_src->path(), offset, completed, retry_count);
           }
           // END of EXPERIMENT


#ifdef GEN_SYNDATA
        }
#endif


        if (bytes_processed != blocksize) {
            sprintf(errormsg, "%s: Read %ld bytes instead of %zd (%s)",
                    src_file->path, bytes_processed, blocksize, p_src->strerror());
            errsend(NONFATAL, errormsg);
            err = 1; break;  // return -1
        }


        // WRITE data to destination
        //
        //rc = MPI_File_write_at(dest_fd, completed, buf, blocksize, MPI_BYTE, &status );

        ////#ifdef PLFS
        ////        if (src_file->dest_ftype == PLFSFILE) {
        ////           // ported to PLFS 2.5
        ////           /// bytes_processed = plfs_write(plfs_dest_fd, buf, blocksize, completed+offset, pid);
        ////           plfs_error_t err = plfs_write(plfs_dest_fd, buf, blocksize, completed+offset, pid, &bytes_processed);
        ////        }
        ////        else {
        ////#endif
        ////            bytes_processed = pwrite(dest_fd, buf, blocksize, completed+offset);
        ////#ifdef PLFS
        ////        }
        ////#endif
        bytes_processed = p_dest->write(buf, blocksize, offset+completed);

        // ---------------------------------------------------------------------------
        // MARFS EXPERIMENT.  As with reads, we may see stalled writes to
        // object-servers.  However, in the case of writes, we can't assume
        // that a retry is even feasible.  For example, if some data was
        // successfully written, then we can't necessarily resume writing
        // after that.  pftool arranges with MarFS that N:1 writes will all
        // start on object boundaries.  Therefore, *IFF* we are on the very
        // first write, we can conceivably close, re-open, and retry.
        //
        // This approach will at least allow host-randomization to make us
        // robust against unresponsive individual servers, provided they
        // are unresponsive when we first try to write to them.
        //
        // DEEMED SUCCESS: Another thing that could be going on is a
        // restart of an N:1 copy, where the destination is a newer Scality
        // sproxyd install, where overwrites of an existing object-ID are
        // forbidden.  In this case, the CTM bitmap, which is only updated
        // periodically, might not yet know that this particular chunk was
        // already written successfully.  In that case, provided the
        // existing object has the correct length, we "deem" the write a
        // success despite the fact that it failed.  Note that the error is
        // not reported (i.e. the PUT doesn't fail, and writes appear to be
        // succeeding) until we've written the LAST byte, perhaps because
        // PUTs from pftool have a length field, and the server doesn't
        // bother complaining about anything until the full-length request
        // has been received.
        //
        // TBD: In a POSIX context, this is overkill.  You'd rather just
        // retry the write.  For the case we're seeing with object-servers,
        // we're skipping straight to what the problem seems to be, there,
        // which is that we need to issue a fresh request, which is done
        // implicitly by closing and re-opening.
        // ---------------------------------------------------------------------------
        while ((bytes_processed != blocksize) && (retry_count++ < 5)
               && (completed == 0)) {

           errsend_fmt(NONFATAL, "(RETRY) %s, at %lu+%lu, len %lu\n",
                       p_dest->path(), offset, completed, blocksize);

           if (! p_dest->close()) {
              errsend_fmt(NONFATAL, "(RETRY) Failed to close dest file: %s (%s)\n",
                          p_dest->path(), p_dest->strerror());
              // err = 1;
           }

           if (! p_dest->open(flags, 0600, offset, length)) {
              errsend_fmt(NONFATAL, "(RETRY) Failed to open file %s for write, off %lu+%lu (%s)\n",
                          p_dest->path(), offset, completed, p_dest->strerror());

              p_src->close();
              if (buf)
                 free(buf);
              return -1;
           }

           // try again ...
           bytes_processed = p_dest->write(buf, blocksize, offset+completed);
        }
        if (retry_count && (bytes_processed == blocksize)) {
           errsend_fmt(NONFATAL, "(RETRY) success for %s, off %lu+%lu (retries = %d)\n",
                       p_dest->path(), offset, completed, retry_count);
        }
        // END of EXPERIMENT



        // see "DEEMED SUCCESS", above
        if ((bytes_processed == -blocksize)
            && (blocksize != 1) // TBD: how to distinguish this from an error?
            && ((completed + blocksize) == length)) {

           bytes_processed = -bytes_processed; // "deemed success"
           success = 1;        // caller can distinguish actual/deemed success
           // err = 0; break;  // return -1;
        }
        else if (bytes_processed != blocksize) {
           errsend_fmt(NONFATAL, "Failed %s offs %ld wrote %ld bytes instead of %zd (%s)\n",
                       dest_file->path, offset, bytes_processed, blocksize, p_dest->strerror());
           err = 1; break;  // return -1;
        }
        completed += blocksize;
    }


    // CLOSE source and destination

#ifdef GEN_SYNDATA
    if(!syndataExists(synbuf)) {
#endif

       ////#ifdef PLFS
       ////       if (src_file->ftype == PLFSFILE) {
       ////           // ported to PLFS 2.5
       ////           /// rc = plfs_close(plfs_src_fd, pid+rank, src_file->st.st_uid, O_RDONLY, NULL);
       ////           int num_ref;
       ////           rc = plfs_close(plfs_src_fd, pid+rank, src_file->st.st_uid, O_RDONLY, NULL, &num_ref);
       ////       }
       ////       else {
       ////#endif
       ////           rc = close(src_fd);
       ////           if (rc != 0) {
       ////               sprintf(errormsg, "Failed to close file: %s", src_file->path);
       ////               errsend(NONFATAL, errormsg);
       ////               err = 1; // return -1;
       ////           }
       ////#ifdef PLFS
       ////       }
       ////#endif
       if (! p_src->close()) {
          errsend_fmt(NONFATAL, "Failed to close src file: %s (%s)\n",
                      p_src->path(), p_src->strerror());

          // // This failure doesn't mean the copy failed.
          // err = 1;
       }

#ifdef GEN_SYNDATA
    }
#endif



    ////#ifdef PLFS
    ////    if (src_file->dest_ftype == PLFSFILE) {
    ////       // ported to PLFS 2.5
    ////       /// rc = plfs_close(plfs_dest_fd, pid+rank, src_file->st.st_uid, flags, NULL);
    ////       int num_ref;
    ////       rc = plfs_close(plfs_dest_fd, pid+rank, src_file->st.st_uid, flags, NULL, &num_ref);
    ////    }
    ////    else {
    ////#endif
    ////        rc = close(dest_fd);
    ////        if (rc != 0) {
    ////            sprintf(errormsg, "Failed to close file: %s", dest_file->path);
    ////            errsend(NONFATAL, errormsg);
    ////            return -1;
    ////        }
    ////#ifdef PLFS
    ////    }
    ////#endif
    if (! p_dest->close()) {
       errsend_fmt(NONFATAL, "Failed to close dest file: %s (%s)\n",
                   p_dest->path(), p_dest->strerror());
       err = 1;
    }

    if (buf)
       free(buf);

    // even error-situations have now done clean-up
    if (err)
       return -1;

    if (offset == 0 && length == src_file->st.st_size) {
        PRINT_IO_DEBUG("rank %d: copy_file() Updating transfer stats for %s\n",
                       rank, dest_file.path);
        if (update_stats(src_file, dest_file)) {
            return -1;
        }
    }

    return success;             // 0: copied, 1: deemed copy
}

int compare_file(path_item*  src_file,
                 path_item*  dest_file,
                 size_t      blocksize,
                 int         meta_data_only) {

   ////    struct stat  dest_st;
   size_t       completed = 0;
   char*        ibuf;
   char*        obuf;
   size_t       bytes_processed;
   char         errormsg[MESSAGESIZE];
   int          rc;
   int          crc;
   //    off_t        offset = src_file->offset;
   //    size_t       length = src_file->length;
   ////    off_t offset = (src_file->chkidx * src_file->chksz);
   ////    off_t length = src_file->st.st_size;
   off_t offset = (src_file->chkidx * src_file->chksz);
   off_t length = (((offset + src_file->chksz) > src_file->st.st_size)
                   ? (src_file->st.st_size - offset)
                   : src_file->chksz);

   PathPtr p_src( PathFactory::create_shallow(src_file));
   PathPtr p_dest(PathFactory::create_shallow(dest_file));


   // assure dest exists
   ////#ifdef FUSE_CHUNKER
   ////    if (dest_file->ftype == FUSEFILE){
   ////      if (stat(dest_file->path, &dest_st) == -1){
   ////         return 2;
   ////      }
   ////    }
   ////    else{
   ////#endif
   ////      // WHY NO PLFS CASE HERE?
   ////      if (lstat(dest_file->path, &dest_st) == -1) {
   ////        return 2;
   ////      }
   ////#ifdef FUSE_CHUNKER
   ////    }
   ////#endif
   if (! p_dest->stat())
      return 2;

   ////    if (src_file->st.st_size == dest_st.st_size &&
   ////            (src_file->st.st_mtime == dest_st.st_mtime  ||
   ////             S_ISLNK(src_file->st.st_mode))&&
   ////            src_file->st.st_mode == dest_st.st_mode &&
   ////            src_file->st.st_uid == dest_st.st_uid &&
   ////            src_file->st.st_gid == dest_st.st_gid) {
   if (src_file->st.st_size == dest_file->st.st_size &&
       (src_file->st.st_mtime == dest_file->st.st_mtime  ||
        S_ISLNK(src_file->st.st_mode))&&
       src_file->st.st_mode == dest_file->st.st_mode &&
       src_file->st.st_uid == dest_file->st.st_uid &&
       src_file->st.st_gid == dest_file->st.st_gid) {

      //metadata compare
      if (meta_data_only) {
         return 0;
      }

      //byte compare
      // allocate buffers and open files ...
      ibuf = (char*)malloc(blocksize * sizeof(char));
      if (! ibuf) {
         errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                     blocksize, src_file->path);
         return -1;
      }

      obuf = (char*)malloc(blocksize * sizeof(char));
      if (! obuf) {
         errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                     blocksize, dest_file->path);
         free(ibuf);
         return -1;
      }

      ////        src_fd = open(src_file->path, O_RDONLY);
      ////        if (src_fd < 0) {
      ////            sprintf(errormsg, "Failed to open file %s for compare source", src_file->path);
      ////            errsend(NONFATAL, errormsg);
      ////            return -1;
      ////        }
      if (! p_src->open(O_RDONLY, src_file->st.st_mode, offset, length)) {
         errsend_fmt(NONFATAL, "Failed to open file %s for compare source\n", p_src->path());
         free(ibuf);
         free(obuf);
         return -1;
      }

      ////        dest_fd = open(dest_file->path, O_RDONLY);
      ////        if (dest_fd < 0) {
      ////            sprintf(errormsg, "Failed to open file %s for compare destination", dest_file->path);
      ////            errsend(NONFATAL, errormsg);
      ////            return -1;
      ////        }
      if (! p_dest->open(O_RDONLY, dest_file->st.st_mode, offset, length)) {
         errsend_fmt(NONFATAL, "Failed to open file %s for compare destination\n", p_dest->path());
         free(ibuf);
         free(obuf);
         return -1;
      }

      //incase someone accidently set an offset+length that exceeds the file bounds
      if ((src_file->st.st_size - offset) < length) {
         length = src_file->st.st_size - offset;
      }
      //a file less than blocksize
      if (length < blocksize) {
         blocksize = length;
      }
      crc = 0;
      while (completed != length) {

         // Wasteful?  If we fail to read blocksize, we'll have a problem
         // anyhow.  And if we succeed, then we'll wipe this all out with
         // the data, anyhow.  [See also memsets in copy_file()]
         //
         //            memset(ibuf, 0, blocksize);
         //            memset(obuf, 0, blocksize);

         //blocksize is too big
         if ((length - completed) < blocksize) {
            blocksize = (length - completed);
         }
         ////            bytes_processed = pread(src_fd, ibuf, blocksize, completed+offset);
         bytes_processed = p_src->read(ibuf, blocksize, completed+offset);
         if (bytes_processed != blocksize) {
            sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                    src_file->path, bytes_processed, blocksize);
            errsend(NONFATAL, errormsg);
            free(ibuf);
            free(obuf);
            return -1;
         }
         ////            bytes_processed = pread(dest_fd, obuf, blocksize, completed+offset);
         bytes_processed = p_dest->read(obuf, blocksize, completed+offset);
         if (bytes_processed != blocksize) {
            sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                    dest_file->path, bytes_processed, blocksize);
            errsend(NONFATAL, errormsg);
            free(ibuf);
            free(obuf);
            return -1;
         }
         //compare - if no compare crc=1 if compare crc=0 and get out of loop
         crc = memcmp(ibuf,obuf,blocksize);
         //printf("compare_file: src %s dest %s offset %ld len %d crc %d\n",
         //       src_file, dest_file, completed+offset, blocksize, crc);
         if (crc != 0) {
            completed=length;
         }
         completed += blocksize;
      }
      ////        rc = close(src_fd);
      ////        if (rc != 0) {
      if (! p_src->close()) {
         sprintf(errormsg, "Failed to close src file: %s", src_file->path);
         errsend(NONFATAL, errormsg);
         free(ibuf);
         free(obuf);
         return -1;
      }
      ////        rc = close(dest_fd);
      ////        if (rc != 0) {
      if (! p_dest->close()) {
         sprintf(errormsg, "Failed to close dst file: %s", dest_file->path);
         errsend(NONFATAL, errormsg);
         free(ibuf);
         free(obuf);
         return -1;
      }
      free(ibuf);
      free(obuf);
      if (crc != 0)
         return 1;
      else
         return 0;
   }
   else
      return 1;

   return 0;
}




// make <dest_file> have the same meta-data as <src_file>
// We assume that <src_file>.dest_ftype applies to <dest_file>
int update_stats(path_item*  src_file,
                 path_item*  dest_file) {

    int            rc;
    char           errormsg[MESSAGESIZE];
    int            mode;

    // Make a path_item matching <dest_file>, using <src_file>->dest_ftype
    // NOTE: Path::follow() is false, by default
    path_item  dest_copy(*dest_file);
    dest_copy.ftype = src_file->dest_ftype;
    PathPtr p_dest(PathFactory::create_shallow(&dest_copy));

    // update <dest_file> owner  (without following links)
    ////#ifdef PLFS
    ////    if (src_file->dest_ftype == PLFSFILE){
    ////        rc = plfs_chown(dest_file->path, src_file->st.st_uid, src_file->st.st_gid);
    ////    }
    ////    else{
    ////#endif
    ////        rc = lchown(dest_file->path, src_file->st.st_uid, src_file->st.st_gid);
    ////#ifdef PLFS
    ////    }   
    ////#endif
    ////    if (rc != 0) {
    ////        sprintf(errormsg, "Failed to change ownership of file: %s to %d:%d",
    ////                dest_file->path, src_file->st.st_uid, src_file->st.st_gid);
    ////        errsend(NONFATAL, errormsg);
    ////    }
    if (0 == geteuid()) {
       if (! p_dest->chown(src_file->st.st_uid, src_file->st.st_gid)) {
          errsend_fmt(NONFATAL, "update_stats -- Failed to chown %s: %s\n",
                      p_dest->path(), p_dest->strerror());
       }
    }


    // ignore symlink destinations
    if (S_ISLNK(src_file->st.st_mode))
        return 0;


    
    // update <dest_file> owner  [ FUSEFILE version ]
#ifdef FUSE_CHUNKER
    if (src_file->dest_ftype == FUSEFILE){
       ////        rc = chown(dest_file->path, src_file->st.st_uid, src_file->st.st_gid);
       ////        if (rc != 0) {
       ////            sprintf(errormsg, "Failed to change ownership of fuse chunked file: %s to %d:%d",
       ////                    dest_file->path, src_file->st.st_uid, src_file->st.st_gid);
       ////            errsend(NONFATAL, errormsg);
       ////        }
       if (! p_dest->chown(src_file->st.st_uid, src_file->st.st_gid)) {
          errsend_fmt(NONFATAL, "update_stats -- Failed to chown fuse chunked file %s: %s\n",
                      p_dest->path(), p_dest->strerror());
       }
    }
#endif



    // update <dest_file> access-permissions
    mode = src_file->st.st_mode & 07777;
    ////#ifdef PLFS
    ////    if (src_file->dest_ftype == PLFSFILE){
    ////        rc = plfs_chmod(dest_file->path, mode);
    ////    }
    ////    else{
    ////#endif
    ////        rc = chmod(dest_file->path, mode);
    ////#ifdef PLFS
    ////    }
    ////#endif
    ////    if (rc != 0) {
    ////        sprintf(errormsg, "Failed to chmod file: %s to %o", dest_file->path, mode);
    ////        errsend(NONFATAL, errormsg);
    ////    }
    if (0 == geteuid()) {
        if (! p_dest->chmod(mode)) {
           errsend_fmt(NONFATAL, "update_stats -- Failed to chmod fuse chunked file %s: %s\n",
                       p_dest->path(), p_dest->strerror());
        }
    }

    // perform any final adjustments on destination, before we set atime/mtime
    PathPtr p_src(PathFactory::create_shallow(src_file));
    p_dest->post_process(p_src);

    // update <dest_file> atime and mtime
    //
    ////#ifdef PLFS
    ////    if (src_file->dest_ftype == PLFSFILE){
    ////        rc = plfs_utime(dest_file->path, &ut);
    ////    }
    ////    else{
    ////#endif
    ////        rc = utime(dest_file->path, &ut);
    ////#ifdef PLFS
    ////    }
    ////#endif
    ////    if (rc != 0) {
    ////        sprintf(errormsg, "Failed to set atime and mtime for file: %s", dest_file->path);
    ////        errsend(NONFATAL, errormsg);
    ////    }
    struct timespec times[2];
    times[0].tv_sec  = p_src->st().st_atim.tv_sec;
    times[0].tv_nsec = p_src->st().st_atim.tv_nsec;

    times[1].tv_sec  = p_src->st().st_mtim.tv_sec;
    times[1].tv_nsec = p_src->st().st_mtim.tv_nsec;

    if (! p_dest->utimensat(times, AT_SYMLINK_NOFOLLOW)) {
       errsend_fmt(NONFATAL, "update_stats -- Failed to change atime/mtime %s: %s\n",
                   p_dest->path(), p_dest->strerror());
    }


    return 0;
}


//local functions only
int request_response(int type_cmd) {
    MPI_Status status;
    int response;
    send_command(MANAGER_PROC, type_cmd);
    if (MPI_Recv(&response, 1, MPI_INT, MANAGER_PROC, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
       errsend(FATAL, "Failed to receive response\n");
    }
    return response;
}

int request_input_queuesize() {
    return request_response(QUEUESIZECMD);
}

void send_command(int target_rank, int type_cmd) {
    //Tell a rank it's time to begin processing
    PRINT_MPI_DEBUG("target rank %d: Sending command %d to target rank %d\n", target_rank, type_cmd, target_rank);
    if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send command %d to rank %d\n", type_cmd, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}


// This is unused (?)
void send_path_list(int target_rank, int command, int num_send, path_list **list_head, path_list **list_tail, int *list_count) {
    int path_count = 0, position = 0;
    int worksize, workcount;
    if (num_send <= *list_count) {
        workcount = num_send;
    }
    else {
        workcount = *list_count;
    }
    worksize = workcount * sizeof(path_item);

    char *workbuf = (char*)malloc(worksize * sizeof(char));
    if (! workbuf) {
       fprintf(stderr, "Failed to allocate %lu bytes for workbuf\n", worksize);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }

    while(path_count < workcount) {
        path_count++;
        MPI_Pack(&(*list_head)->data, sizeof(path_item), MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
        dequeue_node(list_head, list_tail, list_count);
    }
    //send the command to get started
    send_command(target_rank, command);
    //send the # of paths
    if (MPI_Send(&workcount, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send workcount %d to rank %d\n", workcount, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send workbuf to rank %d\n",  target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    free(workbuf);
}

void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count) {
    int         i;
    int         position = 0;
    int         worksize;
    char*       workbuf;
    path_item   work_node;
    path_item*  work_node_ptr;  /* avoid unnecessary copying */

    worksize = *buffer_count * sizeof(path_item);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
       fprintf(stderr, "Failed to allocate %lu bytes for workbuf\n", worksize);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    for (i = 0; i < *buffer_count; i++) {
        work_node_ptr = &buffer[i];
        MPI_Pack(work_node_ptr, sizeof(path_item), MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
    }
    send_command(target_rank, command);
    if (MPI_Send(buffer_count, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send buffer_count %d to rank %d\n", *buffer_count, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send workbuf to rank %d\n", target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    *buffer_count = 0;
    free(workbuf);
}

void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, int *workbufsize) {
    int size = (*workbuflist)->size;
    int worksize = sizeof(path_item) * size;
    send_command(target_rank, command);
    if (MPI_Send(&size, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send workbuflist size %d to rank %d\n", size, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send((*workbuflist)->buf, worksize, MPI_PACKED, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send workbuflist buf to rank %d\n", target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    dequeue_buf_list(workbuflist, workbufsize);
}

//manager
void send_manager_nonfatal_inc() {
    send_command(MANAGER_PROC, NONFATALINCCMD);
}

void send_manager_chunk_busy() {
    send_command(MANAGER_PROC, CHUNKBUSYCMD);
}

void send_manager_copy_stats(int num_copied_files, size_t num_copied_bytes) {
    send_command(MANAGER_PROC, COPYSTATSCMD);
    //send the # of paths
    if (MPI_Send(&num_copied_files, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_copied_files %d to rank %d\n", num_copied_files, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    //send the # of paths
    if (MPI_Send(&num_copied_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_copied_byes %zd to rank %d\n", num_copied_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_manager_examined_stats(int num_examined_files, size_t num_examined_bytes, int num_examined_dirs) {
    send_command(MANAGER_PROC, EXAMINEDSTATSCMD);
    //send the # of paths
    if (MPI_Send(&num_examined_files, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_examined_files %d to rank %d\n", num_examined_files, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_examined_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_examined_bytes %zd to rank %d\n", num_examined_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_examined_dirs, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_examined_dirs %d to rank %d\n", num_examined_dirs, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

#ifdef TAPE
void send_manager_tape_stats(int num_examined_tapes, size_t num_examined_tape_bytes) {
    send_command(MANAGER_PROC, TAPESTATCMD);
    //send the # of paths
    if (MPI_Send(&num_examined_tapes, 1, MPI_INT, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_examined_tapes %d to rank %d\n", num_examined_tapes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_examined_tape_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_examined_tape_bytes %zd to rank %d\n", num_examined_tape_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}
#endif


void send_manager_regs_buffer(path_item *buffer, int *buffer_count) {
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, PROCESSCMD, buffer, buffer_count);
}

void send_manager_dirs_buffer(path_item *buffer, int *buffer_count) {
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, DIRCMD, buffer, buffer_count);
}

#ifdef TAPE
void send_manager_tape_buffer(path_item *buffer, int *buffer_count) {
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, TAPECMD, buffer, buffer_count);
}
#endif

void send_manager_new_buffer(path_item *buffer, int *buffer_count) {
    //send manager new inputs
    send_path_buffer(MANAGER_PROC, INPUTCMD, buffer, buffer_count);
}

void send_manager_work_done(int ignored) {
    //the worker is finished processing, notify the manager
    send_command(MANAGER_PROC, WORKDONECMD);
}

//worker
void update_chunk(path_item *buffer, int *buffer_count) {
    send_path_buffer(ACCUM_PROC, UPDCHUNKCMD, buffer, buffer_count);
}

void write_output(const char *message, int log) {
    //write a single line using the outputproc
    //set the command type
    if (log == 0) {
        send_command(OUTPUT_PROC, OUTCMD);
    }
    else if (log == 1) {
        send_command(OUTPUT_PROC, LOGCMD);
    }
    //send the message
    if (MPI_Send((void*)message, MESSAGESIZE, MPI_CHAR, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to message to rank %d\n", OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}


void write_buffer_output(char *buffer, int buffer_size, int buffer_count) {
    //write a buffer to the output proc
    //set the command type
    send_command(OUTPUT_PROC, BUFFEROUTCMD);
    //send the size of the buffer
    if (MPI_Send(&buffer_count, 1, MPI_INT, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to buffer_count %d to rank %d\n", buffer_count, OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(buffer, buffer_size, MPI_PACKED, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to message to rank %d\n", OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_worker_queue_count(int target_rank, int queue_count) {
    if (MPI_Send(&queue_count, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to queue_count %d to rank %d\n", queue_count, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, int *workbufsize) {
    //send a worker a buffer list of paths to stat
    send_buffer_list(target_rank, DIRCMD, workbuflist, workbufsize);
}

#ifdef TAPE
void send_worker_tape_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize) {
    //send a worker a buffer list of paths to stat
    send_buffer_list(target_rank, TAPECMD, workbuflist, workbufsize);
}
#endif

void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize) {
    //send a worker a list buffers with paths to copy
    send_buffer_list(target_rank, COPYCMD, workbuflist, workbufsize);
}

void send_worker_compare_path(int target_rank, work_buf_list  **workbuflist, int *workbufsize) {
    //send a worker a list buffers with paths to compare
    send_buffer_list(target_rank, COMPARECMD, workbuflist, workbufsize);
}

void send_worker_exit(int target_rank) {
    //order a rank to exit
    send_command(target_rank, EXITCMD);
}


static void errsend_internal(int fatal, const char* errormsg) {
    write_output(errormsg, 1);

    if (fatal) {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else {
        send_manager_nonfatal_inc();
    }
}

//functions that workers use
void errsend(int fatal, const char *error_text) {
    //send an error message to the outputproc. Die if fatal.
    char errormsg[MESSAGESIZE];

    if (fatal)
       snprintf(errormsg, MESSAGESIZE, "ERROR FATAL: %s\n", error_text);
    else
       snprintf(errormsg, MESSAGESIZE, "ERROR NONFATAL: %s\n", error_text);

    errsend_internal(fatal, errormsg);
}

// This allows caller to use inline formatting, without first snprintf() to
// a local errmsg-buffer.  Like so:
//
//    errsend_fmt(nonfatal, "rank %d hello!", rank);
//
void errsend_fmt(int fatal, const char* format, ...) {
   char     errormsg[MESSAGESIZE];
   va_list  args;

   snprintf(errormsg, MESSAGESIZE, "ERROR %sFATAL: ", (fatal ? "" : "NON"));
   size_t offset = strlen(errormsg);

   va_start(args, format);
   vsnprintf(errormsg+offset, MESSAGESIZE-offset, format, args);
   va_end(args);

   errsend_internal(fatal, errormsg);
}




#ifdef FUSE_CHUNKER
int is_fuse_chunk(const char *path, struct options& o) {
  if (path && strstr(path, o.fuse_path)){
    return 1;
  } 
  else{
    return 0;
  }
}

// A path_item (which is presumed to have ftype FUSEFILE), should be a
// symlink.  The symlink name consists of at least 4 tokens, separated by
// '.'.  The fourth token is the length of the chunk.  (What chunk?)
void set_fuse_chunk_data(path_item *work_node) {
    int        i;
    int        numchars;
    char       linkname[PATHSIZE_PLUS];
    char       baselinkname[PATHSIZE_PLUS];
    const char delimiters[] =  ".";
    char*      current;
    char       errormsg[MESSAGESIZE];
    size_t     length;
    PathPtr p(PathFactory::create_shallow(work_node));

    // memset(linkname,'\0', sizeof(PATHSIZE_PLUS));
    numchars = p->readlink(linkname, PATHSIZE_PLUS);
    if (numchars < 0) {
        sprintf(errormsg, "Failed to read link %s", work_node->path);
        errsend(NONFATAL, errormsg);
        return;
    }
    linkname[numchars] = '\0';

    // "length" is found in the 4th token
    strncpy(baselinkname, basename(linkname), PATHSIZE_PLUS);
    current = strdup(baselinkname);
    strtok(current, delimiters);
    for (i = 0; i < 2; i++) {
        strtok(NULL, delimiters);
    }
    length = atoll(strtok(NULL, delimiters));
    free(current);

    // assign to <work_node>
    //    work_node->offset = 0;
    //    work_node->length = length;
    work_node->chkidx = 0;
    work_node->chksz = length;
}


// fuse-chunker path has extended attributes for each corresponding chunk.
// The attributes have names like "user.chunk_<n>", where <n> is the chunk
// number.  The corresponding value is a string like "<atime> <mtime> <uid>
// <gid>", where these are the corresponding values for that chunk.
//
// Given an offset and chunk-size (i.e. <length>), compute the chunk
// number, generate the corresponding attribute-name, retrieve the extended
// attrbitues, and parse them into the provided pointed-to values.
// 
int get_fuse_chunk_attr(const char *path, off_t offset, size_t length, struct utimbuf *ut, uid_t *userid, gid_t* groupid) {
    char   value[10000];
    int    valueLen = 0;
    char   chunk_name[50];
    int    chunk_num = 0;

    if (length == 0) {
        return -1;
    }
    chunk_num = offset/length;
    snprintf(chunk_name, 50, "user.chunk_%d", chunk_num);
#ifdef __APPLE__
    valueLen = getxattr(path, chunk_name, value, 10000, 0, 0);
#else
    valueLen = getxattr(path, chunk_name, value, 10000);
#endif
    if (valueLen != -1) {
        sscanf(value, "%10lld %10lld %8d %8d", (long long int *) &(ut->actime), (long long int *) &(ut->modtime), userid, groupid);
    }
    else {
        return -1;
    }
    return 0;
}

int set_fuse_chunk_attr(const char *path, off_t offset, size_t length, struct utimbuf ut, uid_t userid, gid_t groupid) {
    char value[10000];
    int valueLen = 0;
    char chunk_name[50];
    int chunk_num = 0;
    chunk_num = offset/length;
    snprintf(chunk_name, 50, "user.chunk_%d", chunk_num);
    sprintf(value, "%lld %lld %d %d", (long long int) ut.actime, (long long int ) ut.modtime, userid, groupid);
#ifdef __APPLE__
    valueLen = setxattr(path, chunk_name, value, 10000, 0, XATTR_CREATE);
#else
    valueLen = setxattr(path, chunk_name, value, 10000, XATTR_CREATE);
#endif
    if (valueLen != -1) {
        return 0;
    }
    else {
        return -1;
    }
}
#endif



// Takes a work node (with a path installed), stats it, and figures out
// some of its characteristics.  Updates the following fields:
//
//   work_node.ftype
//   work_node.dest_ftype
//   work_node.st
//
// The PathFactory looks at path_item.ftype, to determin what type of
// subclass to create.  As a result, when the path factory is given just a
// path_name, or an uninitialized path_item, it uses this function to
// initialize the ftype, so it can then determine which Path-subclass to
// create.  Therefore: DO NOT RETURN WITHOUT INITIALIZING FTYPE!
//
int stat_item(path_item *work_node, struct options& o) {
    char        errmsg[MESSAGESIZE];
    struct stat st;
    int         rc;

    //dmapi
#ifdef TAPE
    uid_t uid;
    int   dmarray[3];
    char  hexbuf[128];
#endif

    int  numchars;
    char linkname[PATHSIZE_PLUS];

    // defaults
    work_node->ftype      = REGULARFILE;
    work_node->dest_ftype = REGULARFILE;

    bool  got_type = false;

#ifdef S3
    // --- is it an S3 path?
    if ( (! strncmp(work_node->path, "http://",  7)) ||
         (! strncmp(work_node->path, "https://", 8))) {

       // if it matches the prefixes, it *is* an S3-path, whether it exists or not
       work_node->ftype = S3FILE;
       got_type = true;

       bool okay = S3_Path::fake_stat(work_node->path, &st); // return non-zero for success
       if (! okay) {
          return -1;
       }
    }
#endif

#ifdef MARFS
    // --- is it a MARFS path?
    if(! got_type) {
       fflush(stdout);
       if ( (! strncmp(work_node->path, marfs_config->mnt_top, marfs_config->mnt_top_len))
            && ((   work_node->path[marfs_config->mnt_top_len] == 0)
                || (work_node->path[marfs_config->mnt_top_len] == '/'))) {

           work_node->ftype = MARFSFILE;
           got_type = true;

        bool okay = MARFS_Path::mar_stat(work_node->path, &st);
        if (!okay){
           return -1;
        }
      }
    }
#endif



#ifdef PLFS
    // --- is it a PLFS path?
    if (! got_type) {
        rc = plfs_getattr(NULL, work_node->path, &st, 0);
        if (rc == 0){
            work_node->ftype = PLFSFILE;
            got_type = true;
        }
        else {
            // check the owning directory
            char* copy = strdup(work_node->path);
            rc = plfs_getattr(NULL, dirname(copy), &st, 0);
            free(copy);
            if (rc == 0) {
                work_node->ftype = PLFSFILE;
                got_type = true;
            }
        }
    }
#endif


    // --- is it '/dev/null' or '/dev/null/[...]'?
    if (! got_type) {
       if ( (! strncmp(work_node->path, "/dev/null", 9)) ) {
          if (work_node->path[9] == 0) {
            work_node->ftype = NULLFILE;
            got_type = true;

            rc = lstat("/dev/null", &st);
          }
          else if (work_node->path[9] == '/') {
             size_t len = strlen(work_node->path);
             if (work_node->path[len -1] == '/') {
                work_node->ftype = NULLDIR;
                got_type = true;

                char* homedir = getenv("HOME");
                rc = lstat(homedir, &st);
             }
             else {
                work_node->ftype = NULLFILE;
                got_type = true;

                rc = lstat("/dev/null", &st);
             }
          }
       }
    }

    // --- is it a POSIX path?
    if (! got_type) {
        rc = lstat(work_node->path, &st); // TODO: in posix path it checks to see if it should follow links
        if (rc == 0){
            work_node->ftype = REGULARFILE;
            got_type = true;
        }
        else
            return -1;
    }


#ifdef GEN_SYNDATA
    if (o.syn_size) // We are generating synthetic data, and NOT copying data in file. Need to muck with the file size
        st.st_size = o.syn_size;
#endif


    work_node->st = st;

    //dmapi to find managed files
#ifdef TAPE
    if (!S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode) && o.sourcefs == GPFSFS) {
        uid = getuid();

# ifdef THREADS_ONLY
        if (0)
# else
        if (uid == 0 && st.st_size > 0 && st.st_blocks == 0)
# endif
        {
            dmarray[0] = 0;
            dmarray[1] = 0;
            dmarray[2] = 0;
            if (read_inodes (work_node->path, work_node->st.st_ino, work_node->st.st_ino+1, dmarray) != 0) {
                snprintf(errmsg, MESSAGESIZE, "read_inodes failed: %s", work_node->path);
                errsend(FATAL, errmsg);
            }
            else if (dmarray[0] > 0) {
                dmapi_lookup(work_node->path, dmarray, hexbuf);
                if (dmarray[1] == 1) {
                    work_node->ftype = PREMIGRATEFILE;
                }
                else if (dmarray[2] == 1) {
                    work_node->ftype = MIGRATEFILE;
                }
            }
        }
        else if (st.st_size > 0 && st.st_blocks == 0) {
            work_node->ftype = MIGRATEFILE;
        }
    }
#endif



    //special cases for links
    if (S_ISLNK(work_node->st.st_mode)) {
        PathPtr p(PathFactory::create_shallow(work_node));

        // <linkname> = name of the link-destination
        numchars = p->readlink(linkname, PATHSIZE_PLUS);
        if (numchars < 0) {
            snprintf(errmsg, MESSAGESIZE, "Failed to read link %s", work_node->path);
            errsend(NONFATAL, errmsg);
            return -1;
        }
        linkname[numchars] = '\0';


#ifdef FUSE_CHUNKER
        if (
# ifdef PLFS
            // this will *always* be true, right?  We just set ftype =
            // LINKFILE, above.  Maybe the intent here was to check whether
            // ftype *was* PLFSFILE, before that?
            (work_node->ftype != PLFSFILE) &&
# endif
            // NOTE: call to realpath() leaks memory
            is_fuse_chunk(realpath(work_node->path, NULL), o)) {

            if (lstat(linkname, &st) == -1) {
                snprintf(errmsg, MESSAGESIZE, "stat_item -- Failed to stat path %s", linkname);
                errsend(FATAL, errmsg);
            }
            work_node->st = st;
            work_node->ftype = FUSEFILE;
        }
#endif


    }


#ifdef FUSE_CHUNKER
    //if it qualifies for fuse and is on the "archive" path
    if (work_node->st.st_size > o.fuse_chunk_at) {
        work_node->dest_ftype = FUSEFILE;
    }
#endif

    return 0;
}

// <fs> is actually a SrcDstFSType.  If you have <sys/vfs.h>, then initialize
// <fs> to match the type of <path>.  Otherwise, call it ANYFS.
void get_stat_fs_info(const char *path, SrcDstFSType *fs) {

#ifdef HAVE_SYS_VFS_H
    struct stat st;
    struct statfs stfs;
    char errortext[MESSAGESIZE];
    int rc;
    char use_path[PATHSIZE_PLUS];
    strncpy(use_path, path, PATHSIZE_PLUS);

    // look at <path>, or, if that fails, look at dirname(<path>)
    ////    rc = lstat(use_path, &st);
    ////    if (rc < 0) {
    ////        strcpy(use_path, dirname(use_path));
    ////        rc = lstat(use_path, &st);
    ////        if (rc < 0) {
    ////            fprintf(stderr, "Failed to lstat path %s\n", path);
    ////            MPI_Abort(MPI_COMM_WORLD, -1);
    ////        }
    ////    }
    PathPtr p(PathFactory::create(use_path));
    if (! p) {
       fprintf(stderr, "PathFactory couldn't interpret path %s\n", use_path);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else if (! p->stat()) {
       strcpy(use_path, dirname(use_path));
       p = PathFactory::create(use_path);
       if (! p) {
          fprintf(stderr, "PathFactory couldn't interpret parent-path %s\n", use_path);
          MPI_Abort(MPI_COMM_WORLD, -1);
       }
       else if (! p->stat()) {
          fprintf(stderr, "Failed to stat path %s, or parent %s\n", path, use_path);
          MPI_Abort(MPI_COMM_WORLD, -1);
       }
    }
    st = p->st();


    // if the thing we're looking at isn't link, maybe run statfs() on it.
    if (!S_ISLNK(st.st_mode)) {
       if ((   p->node().ftype == NULLFILE)
           || (p->node().ftype == NULLDIR)) {
          *fs = NULLFS;
          return;
       }
       else if (p->node().ftype == S3FILE) {
          *fs = S3FS;
          return;
       }
       else if (p->node().ftype == PLFSFILE) {
          *fs = PLFSFS;          // NOTE: less than PARALLEL_DESTFS
          return;
       }
       else if (p->node().ftype == MARFSFILE) {
          *fs = MARFSFS;
          return;
       }

       rc = statfs(use_path, &stfs);
       if (rc < 0) {
          snprintf(errortext, MESSAGESIZE, "Failed to statfs path %s", path);
          errsend(FATAL, errortext);
       }
       else if (stfs.f_type == GPFS_FILE) {
          *fs = GPFSFS;
       }
       else if (stfs.f_type == PANFS_FILE) {
          *fs = PANASASFS;
       }
# ifdef FUSE_CHUNKER
       else if (stfs.f_type == FUSE_SUPER_MAGIC) {
          //fuse file
          *fs = GPFSFS;
       }
# endif
       else {
          *fs = ANYFS;        // NOTE: less than PARALLEL_DESTFS
       }
    }
    else {
        //symlink assumed to be GPFS
        *fs = GPFSFS;
    }

#else
    *fs = ANYFS;                // NOTE: less than PARALLEL_DESTFS
#endif
}

int get_free_rank(int *proc_status, int start_range, int end_range) {
    //given an inclusive range, return the first encountered free rank
    int i;
    for (i = start_range; i <= end_range; i++) {
        if (proc_status[i] == 0) {
            return i;
        }
    }
    return -1;
}

int processing_complete(int *proc_status, int nproc) {
    //are all the ranks free?
    int i;
    int count = 0;
    for (i = 0; i < nproc; i++) {
        if (proc_status[i] == 1) {
            count++;
        }
    }
    return count;
}

//Queue Function Definitions

// push path onto the tail of the queue
void enqueue_path(path_list **head, path_list **tail, char *path, int *count) {
    path_list *new_node = (path_list*)malloc(sizeof(path_list));
    if (! new_node) {
       fprintf(stderr, "Failed to allocate %lu bytes for new_node\n", sizeof(path_list));
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    strncpy(new_node->data.path, path, PATHSIZE_PLUS);
    new_node->data.start = 1;
    new_node->data.ftype = TBD;
    new_node->next = NULL;
    if (*head == NULL) {
        *head = new_node;
        *tail = *head;
    }
    else {
        (*tail)->next = new_node;
        *tail = (*tail)->next;
    }
    /*
      while (temp_node->next != NULL){
        temp_node = temp_node->next;
      }
      temp_node->next = new_node;
    }*/
    *count += 1;
}

void print_queue_path(path_list *head) {
    //print the entire queue
    while(head != NULL) {
        printf("%s\n", head->data.path);
        head = head->next;
    }
}

void delete_queue_path(path_list **head, int *count) {
    //delete the entire queue;
    path_list *temp = *head;
    while(temp) {
        *head = (*head)->next;
        free(temp);
        temp = *head;
    }
    *count = 0;
}


// enqueue a node using an existing node (does a new allocate, but allows
// us to pass nodes instead of paths)
void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count) {
    path_list *temp_node = (path_list*)malloc(sizeof(path_list));
    if (! temp_node) {
       fprintf(stderr, "Failed to allocate %lu bytes for temp_node\n", sizeof(path_list));
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    temp_node->data = new_node->data;
    temp_node->next = NULL;
    if (*head == NULL) {
        *head = temp_node;
        *tail = *head;
    }
    else {
        (*tail)->next = temp_node;
        *tail = (*tail)->next;
    }
    *count += 1;
}

void dequeue_node(path_list **head, path_list **tail, int *count) {
    //remove a path from the front of the queue
    path_list *temp_node = *head;
    if (temp_node == NULL) {
        return;
    }
    *head = temp_node->next;
    free(temp_node);
    *count -= 1;
}



void enqueue_buf_list(work_buf_list **workbuflist, int *workbufsize, char *buffer, int buffer_size) {
    work_buf_list *current_pos = *workbuflist;
    work_buf_list *new_buf_item = (work_buf_list*)malloc(sizeof(work_buf_list));
    if (! new_buf_item) {
       fprintf(stderr, "Failed to allocate %lu bytes for new_buf_item\n", sizeof(work_buf_list));
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (*workbufsize < 0) {
        *workbufsize = 0;
    }
    new_buf_item->buf = buffer;
    new_buf_item->size = buffer_size;
    new_buf_item->next = NULL;
    if (current_pos == NULL) {
        *workbuflist = new_buf_item;
        (*workbufsize)++;
        return;
    }
    while (current_pos->next != NULL) {
        current_pos = current_pos->next;
    }
    current_pos->next = new_buf_item;
    (*workbufsize)++;
}

void dequeue_buf_list(work_buf_list **workbuflist, int *workbufsize) {
    work_buf_list *current_pos;
    if (*workbuflist == NULL) {
        return;
    }
    current_pos = (*workbuflist)->next;
    free((*workbuflist)->buf);
    free(*workbuflist);
    *workbuflist = current_pos;
    (*workbufsize)--;
}

void delete_buf_list(work_buf_list **workbuflist, int *workbufsize) {
    while (*workbuflist) {
        dequeue_buf_list(workbuflist, workbufsize);
    }
    *workbufsize = 0;
}

void pack_list(path_list *head, int count, work_buf_list **workbuflist, int *workbufsize) {
    int         position;
    char*       buffer;
    int         buffer_size = 0;
    int         worksize;
    path_list*  iter;

    worksize = MESSAGEBUFFER * sizeof(path_item);
    buffer   = (char *)malloc(worksize);
    if (! buffer) {
       fprintf(stderr, "Failed to allocate %lu bytes for buffer\n", sizeof(worksize));
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    position = 0;

    for (iter=head; iter!=NULL; iter=iter->next) {
        MPI_Pack(&iter->data, sizeof(path_item), MPI_CHAR, buffer, worksize, &position, MPI_COMM_WORLD);
        buffer_size++;
        if (buffer_size % STATBUFFER == 0 || buffer_size % MESSAGEBUFFER == 0) {
            enqueue_buf_list(workbuflist, workbufsize, buffer, buffer_size);
            buffer_size = 0;
            buffer = (char *)malloc(worksize);
            if (! buffer) {
               fprintf(stderr, "Failed to allocate %lu bytes for buffer-elt\n", sizeof(worksize));
               MPI_Abort(MPI_COMM_WORLD, -1);
            }
            position = 0;  // should this be here?
        }
    }
    if(buffer_size != 0) {
       enqueue_buf_list(workbuflist, workbufsize, buffer, buffer_size);
    }
}


#ifdef THREADS_ONLY
//custom MPI calls
int MPY_Pack(void *inbuf, int incount, MPI_Datatype datatype, void *outbuf, int outcount, int *position, MPI_Comm comm) {
    // check to make sure there is space in the output buffer position+incount <= outcount
    if (*position+incount > outcount) {
        return -1;
    }
    // copy inbuf to outbuf
    bcopy(inbuf,outbuf+(*position),incount);
    // increment position  position=position+incount
    *position=*position+incount;
    return 0;
}

int MPY_Unpack(void *inbuf, int insize, int *position, void *outbuf, int outcount, MPI_Datatype datatype, MPI_Comm comm) {
    // check to make sure there is space in the output buffer position+insize <= outcount
    if ((*position)+outcount > insize)  {
        return -1;
    }
    // copy inbuf to outbuf
    bcopy(inbuf+(*position),outbuf,outcount);
    // increment position  position=position+insize
    *position=*position+outcount;
    return 0;
}


int MPY_Abort(MPI_Comm comm, int errorcode) {
    int i = 0;
    MPII_Member *member;
    for (i = 0; i < comm->group->size; i++) {
        member = (MPII_Member *)(comm->group->members)[i];
        unlock(member->mutex);
        delete_mutex(member->mutex);
    }
    for (i = 0; i < comm->group->size; i++) {
        pthread_kill(pthread_self(), SIGTERM);
    }
    return -1;
}
#endif
