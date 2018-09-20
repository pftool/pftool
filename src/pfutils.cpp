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
#include "ctm.h"                // hasCTM()
#include "sig.h"
#include "debug.h"

#include <syslog.h>
#include <signal.h>

#include <pthread.h>            // manager_sig_handler()

// EXITCMD, or ctl-C
volatile int worker_exit = 0;


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
    printf (" [-n]         only operate on file if different (aka 'restart')\n");
    printf (" [-r]         recursive operation down directory tree\n");
    printf (" [-t]         specify file system type of destination file/directory\n");
    printf (" [-l]         turn on logging to syslog\n");
    printf (" [-P]         force destination to be treated as parallel (i.e. assume N:1 support)\n");
    printf (" [-M]         perform block-compare, default: metadata-compare\n");
    printf (" [-o]         attempt to preserve source ownership (user/group) in COPY\n");
    printf (" [-e]         excludes files that match this pattern\n");
    printf (" [-v]         output verbosity [specify multiple times, to increase]\n");
    printf (" [-g]         debugging-level  [specify multiple times, to increase]\n");
    printf (" [-D]         The maximum number of readdir ranks, -1 allows all ranks to be used\n");
    printf (" [-h]         print Usage information\n");
    printf("\n");

    printf("      [if configured with --enable-syndata\n");
    printf (" [-X]         specify a synthetic data pattern file or constant. default: none\n");
    printf (" [-x]         synthetic file size. If specified, file(s) will be synthetic data of specified size\n");
    printf (" \n");

    printf ("********************** PFTOOL USAGE ************************************************************\n");
    return;
}

/**
* Returns the PFTOOL internal command in string format.
* See pfutils.h for the list of commands.
*
* @param cmdidx   the command (or command type)
*
* @return a string representation of the command
*/
const char *cmd2str(OpCode cmdidx) {
   static const char *CMDSTR[] = {
      "EXITCMD"
      ,"UPDCHUNKCMD"
      ,"BUFFEROUTCMD"
      ,"OUTCMD"
      ,"LOGCMD"
      ,"LOGONLYCMD"
      ,"QUEUESIZECMD"
      ,"STATCMD"
      ,"COMPARECMD"
      ,"COPYCMD"
      ,"PROCESSCMD"
      ,"INPUTCMD"
      ,"DIRCMD"
      ,"WORKDONECMD"
      ,"NONFATALINCCMD"
      ,"CHUNKBUSYCMD"
      ,"COPYSTATSCMD"
      ,"EXAMINEDSTATSCMD"
   };

   return((cmdidx > EXAMINEDSTATSCMD)?"Invalid Command":CMDSTR[cmdidx]);
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
* @param thePath  the path to test and create
* @param perms    the permission mode to use when
*        creating directories in this path
*
* @return 0 if all directories are succesfully created.
*  errno (i.e. non-zero) if there is an error. 
*  See "man -s 2 mkdir" for error description.
*/
int mkpath(char *thePath, mode_t perms) {
   char *slash = thePath;       // point at the current "/" in the path
   struct stat sbuf;            // a buffer to hold stat information
   int save_errno;              // errno from mkdir()

   while( *slash == '/') slash++; // burn through any leading "/". Note that if no leading "/",
   // then thePath will be created relative to CWD of process.
   while(slash = strchr(slash,'/')) { // start parsing thePath
      *slash = '\0';
     
      if(stat(thePath,&sbuf)) {  // current path element cannot be stat'd - assume does not exist
         if(mkdir(thePath,perms)) { // problems creating the directory - clean up and return!
            save_errno = errno;     // save off errno - in case of error...
            *slash = '/';
            return(save_errno);
         }
      }
      else if (!S_ISDIR(sbuf.st_mode)) { // element exists but is NOT a directory
         *slash = '/';
         return(ENOTDIR);
      }
      *slash = '/';slash++;          // increment slash ...
      while( *slash == '/') slash++; // burn through any blank path elements
   } // end mkdir loop

   if(stat(thePath,&sbuf)) {   // last path element cannot be stat'd - assume does not exist
      if(mkdir(thePath,perms)) // problems creating the directory - clean up and return!
         return(save_errno = errno); // save off errno - just to be sure ...
   }
   else if (!S_ISDIR(sbuf.st_mode))  // element exists but is NOT a directory
      return(ENOTDIR);

   return(0);
}

// unused?
// convert up to 28 bytes of <b> to ASCII-hex.
void hex_dump_bytes (char *b, int len, char *outhexbuf) {
    char smsg[64];
    char tmsg[3];
    unsigned char *ptr;
    int start = 0;

    ptr = (unsigned char *) (b + start);  /* point to buffer location to start  */
    /* if last frame and more lines are required get number of lines */
    memset (smsg, '\0', 64);

    short str_index;
    short str_max = 28;       // 64 - (2 *28) = room for terminal-NULL
    if (len < 28)
       str_max = len;

    for (str_index = 0; str_index < str_max; str_index++) {
        sprintf (tmsg, "%02X", ptr[str_index]);
        strncat (smsg, tmsg, 2); // controlled, no overflow
    }
    sprintf (outhexbuf, "%s", smsg);
}

/**
* Low Level utility function to write a field of a data
* structure - any data structure.
*
* @param fd    the open file descriptor
* @param start    the starting memory address
*        (pointer) of the field
* @param len      the length of the filed in bytes
*
* @return number of bytes written, If return
*  is < 0, then there were problems writing,
*  and the number can be taken as the errno.
*/
ssize_t write_field(int fd, void *start, size_t len) {
   size_t  n;              // number of bytes written for a given call to write()
   ssize_t tot = 0;           // total number of bytes written
   char*   wstart = (char*)start;            // the starting point in the buffer
   size_t  wcnt = len;           // the running count of bytes to write

   while(wcnt > 0) {
     if(!(n=write(fd,wstart,wcnt)))    // if nothing written -> assume error
       return((ssize_t)-errno);
     tot += n;
     wstart += n;             // increment the start address by n
     wcnt -= n;               // decreamnt byte count by n
   }

   return(tot);
}

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

    char        dir_name[PATHSIZE_PLUS];
    struct stat st;
    int         rc;
    char*       path = (char*)item->path;

    PathPtr p(PathFactory::create(item));
    if (! p->stat()) {
       fprintf(stderr, "get_base_path -- Failed to stat path %s\n", path);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    st = p->st();

    char* path_copy = strdup(path); // dirname() may alter arg
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

    memset(dest_node, 0, sizeof(path_item));   // zero-out header-fields
    strncpy(result, dest_path, PATHSIZE_PLUS); // install dest_path
    if (result[PATHSIZE_PLUS -1])              // strncpy() is unsafe
       errsend_fmt(FATAL, "Oversize path '%s'\n", dest_path);

    dest_node->ftype = TBD;                    // we will figure out the file type later

    strncpy(temp_path, beginning_node->path, PATHSIZE_PLUS);
    if (temp_path[PATHSIZE_PLUS -1]) {         // strncpy() is unsafe
       errsend_fmt(FATAL, "Not enough room to append '%s' + '%s'\n",
                   temp_path, beginning_node->path);
    }


    trim_trailing('/', temp_path);

    //recursion special cases
    if (o.recurse
        && (strncmp(temp_path, "..", PATHSIZE_PLUS) != 0)
        && (o.work_type != COMPAREWORK)) {

        beg_st = beginning_node->st;

        PathPtr d_dest(PathFactory::create(dest_path));
        dest_st = d_dest->st();
        if (d_dest->exists()
            && S_ISDIR(dest_st.st_mode)
            && S_ISDIR(beg_st.st_mode)
            && (num_paths == 1)) {

            // append '/' to result
            size_t result_len = strlen(result);
            if (result[result_len -1] != '/') {
                strncat(result, "/", PATHSIZE_PLUS - result_len);
                if (result[PATHSIZE_PLUS -1]) { // strncat() is unsafe
                   errsend_fmt(FATAL, "Not enough room to append '%s' + '/'\n",
                               result);
                }
            }

            // append tail-end of beginning_node's path
            char* last_slash = strrchr(temp_path, '/');
            if (last_slash)
                path_slice = last_slash + 1;
            else
                path_slice = (char *)temp_path;

            strncat(result, path_slice, PATHSIZE_PLUS - strlen(result) -1);
            if (result[PATHSIZE_PLUS -1]) {     // strncat() is unsafe
               errsend_fmt(FATAL, "Not enough room to append '%s' + '%s'\n",
                           result, path_slice);
            }
        }
    }

    // update the stat-struct, in the final result
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
                     struct options&   o,
                     int               rename_flag) {

    const char*  path_slice;
    int          path_slice_duped = 0;

    // start clean
    memset(out_node, 0, sizeof(path_item));

    // marfs may want to know chunksize
    out_node->chksz  = dest_node->chksz;
    out_node->chkidx = dest_node->chkidx;

    //remove trailing slash(es)
    // NOTE: both are the same size, and dest_node has already been assured to have
    //       a terminal-NULL in get_dest_path(), so strncpy() okay.
    strncpy(out_node->path, dest_node->path, PATHSIZE_PLUS);

    trim_trailing('/', out_node->path);
    ssize_t remain = PATHSIZE_PLUS - strlen(out_node->path) -1;

    //path_slice = strstr(src_path, base_path);
    if (o.recurse == 0) {
        const char* last_slash = strrchr(src_node->path, '/');
        if (last_slash)
            path_slice = last_slash +1;
        else
            path_slice = (char *) src_node->path;
    }
    else {
        if (strcmp(base_path, ".") == 0)
            path_slice = (char *) src_node->path;
        else {
            path_slice = strdup(src_node->path + strlen(base_path) + 1);
            path_slice_duped = 1;
        }
    }


    // assure there is enough room to append path_slice
    size_t slice_len = strlen(path_slice);
    if (slice_len > remain) {
       out_node->path[0] = 0;
       return;
    }
    remain -= slice_len;

    if (S_ISDIR(dest_node->st.st_mode)) {
        strcat(out_node->path, "/");
        strcat(out_node->path, path_slice);
    }
    if (path_slice_duped)
       free((void*)path_slice);


    if ((rename_flag == 1) && (src_node->packable == 0)) {
       //need to create temporary file name

       // assure there is room
       if (remain < DATE_STRING_MAX +1) {
          out_node->path[0] = 0;
          return;
       }
       remain -= DATE_STRING_MAX +1;

       strcat(out_node->path, "+");
       strcat(out_node->path, src_node->timestamp);
    }

    out_node->path[PATHSIZE_PLUS -1] = 0;
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
int copy_file(PathPtr       p_src,
              PathPtr       p_dest,
              size_t        blocksize,
              int           rank,
              struct options& o)
{
    //MPI_Status status;
    int         rc;
    size_t      completed = 0;
    char *      buf = NULL;
    char        errormsg[MESSAGESIZE];
    int         err = 0;        // non-zero -> close src/dest, free buf
    int         flags;
    off_t offset = (p_src->node().chkidx * p_src->node().chksz);
    off_t length = (((offset + p_src->node().chksz) > p_src->size())
                    ? (p_src->size() - offset)
                    : p_src->node().chksz);
    ssize_t     bytes_processed = 0;
    int         retry_count;
    int         success = 0;

    //symlink
    char        link_path[PATHSIZE_PLUS];
    int         numchars;

    // If source is a link, create similar link on the destination-side.
    //can't be const for MPI_IO
    //
    // NOTE: The only way this can be a link is if it's on a quasi-POSIX
    //       system.  So we can just readlink(), to get the link-target.
    //       But the destination could be anything, so we need a
    //       Path::symlink() to implement that.
    if (p_src->is_link()) {

        // <link_path> = name of link-destination
        numchars = p_src->readlink(link_path, PATHSIZE_PLUS);
        if (numchars < 0) {
            errsend_fmt(NONFATAL, "Failed to read link %s\n", p_src->path());
            return -1;
        }
        else if (numchars >= PATHSIZE_PLUS) {
            errsend_fmt(NONFATAL, "readlink %s, not enough room for '\\0'", p_src->path());
            return -1;
        }
        link_path[numchars] = '\0';


        ////        rc = symlink(link_path, dest_file->path);
        ////        if (rc < 0)
        if (! p_dest->symlink(link_path)) {
           errsend_fmt(NONFATAL, "Failed to create symlink %s -> %s", p_dest->path(), link_path);
           return -1;
        }

        if (update_stats(p_src, p_dest, o) != 0) {
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
       if (! p_src->open(O_RDONLY, p_src->mode())) {
           errsend_fmt(NONFATAL, "copy_file: Failed to open file %s for read\n", p_src->path());
           if (buf)
              free(buf);
           return -1;
        }
    PRINT_IO_DEBUG("rank %d: copy_file() Copying chunk "
                   "index %d. offset = %ld   length = %ld   blocksize = %ld\n",
                   rank, p_src->node().chkidx, offset, length, blocksize);
    // OPEN destination for writing

    // create appropriate flags
    if ((p_src->size() <= length) || (p_dest->node().fstype != PAN_FS)) {
       // no chunking or not writing to PANFS - cds 6/2014
       flags = O_WRONLY | O_CREAT;
       PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT",
                       p_dest->fstype_to_str());
    }
    else {
       // Panasas FS needs O_CONCURRENT_WRITE set for file writes - cds 6/2014
       flags = O_WRONLY | O_CREAT | O_CONCURRENT_WRITE;
       PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT | O_CONCURRENT_WRITE",
                       p_dest->fstype_to_str());
    }

    // give destination the same mode as src, (access-bits only)
    mode_t dest_mode = p_src->mode() & (S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO);
    if (! p_dest->open(flags, dest_mode, offset, length)) {
       if (p_dest->get_errno() == EDQUOT) {
           errsend_fmt(FATAL, "Failed to open file %s for write (%s)\n",
                   p_dest->path(), p_dest->strerror());
       }
       else {
           errsend_fmt(NONFATAL, "Failed to open file %s for write (%s)\n",
                   p_dest->path(), p_dest->strerror());
       }
       p_src->close();
       if (buf)
          free(buf);
       return -1;
    }

    // copy contents from source to destination
    while (completed != length) {
        // .................................................................
        // READ data from source (or generate it synthetically)
        // .................................................................
        //1 MB is too big
        if ((length - completed) < blocksize) {
            blocksize = (length - completed);
        }

        // Wasteful?  If we fail to read blocksize, we'll have a problem
        // anyhow.  And if we succeed, then we'll wipe this all out with
        // the data, anyhow.  [See also memsets in compare_file()]
        //
        //        memset(buf, '\0', blocksize);

           bytes_processed = p_src->read(buf, blocksize, offset+completed);

           PRINT_IO_DEBUG("rank %d: copy_file() Copy of %d bytes complete for file %s\n",
                          rank, bytes_processed, p_dest->path());


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
           //
           // UPDATE: don't print warnings (or count non-fatal errors) for
           // every retry.  If the set of retries fail, register one
           // non-fatal error.  Otherwise, keep quiet.
           // ---------------------------------------------------------------------------

           retry_count = 0;
           while ((bytes_processed != blocksize) && (retry_count < 5)) {
              p_src->close();   // best effort
              if (! p_src->open(O_RDONLY, p_src->mode(), offset+completed, length-completed)) {
                 errsend_fmt(NONFATAL, "(read-RETRY) Failed to open %s for read, off %lu+%lu\n",
                             p_src->path(), offset, completed);
                 if (buf)
                    free(buf);
                 return -1;
              }

              // try again ...
              bytes_processed = p_src->read(buf, blocksize, offset+completed);
              retry_count ++;
           }
           // END of EXPERIMENT

        if (bytes_processed != blocksize) {
           char retry_msg[128];
           retry_msg[0] = 0;
           if (retry_count)
              sprintf(retry_msg, " (retries = %d)", retry_count);

           errsend_fmt(NONFATAL, "%s: Read %ld bytes instead of %zd%s: %s\n",
                       p_src->path(), bytes_processed, blocksize, retry_msg, p_src->strerror());
           err = 1; break;  // return -1
        }
        else if (retry_count) {
           output_fmt(2, "(read-RETRY) success for %s, off %lu+%lu (retries = %d)\n",
                      p_dest->path(), offset, completed, retry_count);
        }



        // .................................................................
        // WRITE data to destination
        // .................................................................

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

        retry_count = 0;
        while ((bytes_processed != blocksize)
               && (retry_count < 5)
               && (completed == 0)) {
           p_dest->close();     // best effort

           if (! p_dest->open(flags, 0600, offset, length)) {
              errsend_fmt(NONFATAL, "(write-RETRY) Failed to open file %s for write, off %lu+%lu (%s)\n",
                          p_dest->path(), offset, completed, p_dest->strerror());

              p_src->close();
              if (buf)
                 free(buf);
              return -1;
           }

           // try again ...
           bytes_processed = p_dest->write(buf, blocksize, offset+completed);
           retry_count ++;
        }
        // END of EXPERIMENT

        if ((bytes_processed == -blocksize)
            && (blocksize != 1) // TBD: how to distinguish this from an error?
            && ((completed + blocksize) == length)) {

           bytes_processed = -bytes_processed; // "deemed success"
           success = 1;        // caller can distinguish actual/deemed success
           // err = 0; break;  // return -1;
        }
        else if (bytes_processed != blocksize) {
           char retry_msg[128];
           retry_msg[0] = 0;
           if (retry_count)
              sprintf(retry_msg, " (retries = %d)", retry_count);

           errsend_fmt(NONFATAL, "Failed %s offs %ld wrote %ld bytes instead of %zd%s: %s\n",
                       p_dest->path(), offset, bytes_processed, blocksize, retry_msg, p_dest->strerror());
           err = 1; break;  // return -1;
        }
        else if (retry_count) {
           output_fmt(2, "(write-RETRY) success for %s, off %lu+%lu (retries = %d)\n",
                      p_dest->path(), offset, completed, retry_count);
        }

        completed += blocksize;
    }


    // .................................................................
    // CLOSE source and destination
    // .................................................................

       if (! p_src->close()) {
          errsend_fmt(NONFATAL, "Failed to close src file: %s (%s)\n",
                      p_src->path(), p_src->strerror());
       }

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

    if (offset == 0 && length == p_src->size()) {
        PRINT_IO_DEBUG("rank %d: copy_file() Updating transfer stats for %s\n",
                       rank, p_dest->path());
        if (update_stats(p_src, p_dest, o)) {
            return -1;
        }
    }

    return success;             // 0: copied, 1: deemed copy
}

int compare_file(path_item*      src_file,
                 path_item*      dest_file,
                 size_t          blocksize,
                 int             meta_data_only,
                 struct options& o) {

   ////    struct stat  dest_st;
   size_t       completed = 0;
   char*        ibuf;
   char*        obuf;
   size_t       bytes_processed;
   char         errormsg[MESSAGESIZE];
   int          rc;
   int          crc;
   off_t offset = (src_file->chkidx * src_file->chksz);
   off_t length = (((offset + src_file->chksz) > src_file->st.st_size)
                   ? (src_file->st.st_size - offset)
                   : src_file->chksz);

   PathPtr p_src( PathFactory::create_shallow(src_file));
   PathPtr p_dest(PathFactory::create_shallow(dest_file));


   // assure dest exists
   if (! p_dest->stat())
      return 2;

   if (samefile(p_src, p_dest, o)) {

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

      if (! p_src->open(O_RDONLY, src_file->st.st_mode, offset, length)) {
         errsend_fmt(NONFATAL, "Failed to open file %s for compare source\n", p_src->path());
         free(ibuf);
         free(obuf);
         return -1;
      }

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

         bytes_processed = p_src->read(ibuf, blocksize, completed+offset);
         if (bytes_processed != blocksize) {
            sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                    src_file->path, bytes_processed, blocksize);
            errsend(NONFATAL, errormsg);
            free(ibuf);
            free(obuf);
            return -1;
         }

         bytes_processed = p_dest->read(obuf, blocksize, completed+offset);
         if (bytes_processed != blocksize) {
            sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                    dest_file->path, bytes_processed, blocksize);
            errsend(NONFATAL, errormsg);
            free(ibuf);
            free(obuf);
            return -1;
         }

         crc = memcmp(ibuf,obuf,blocksize);
         if (crc != 0) {
            completed=length;
            break; // this code never worked prior to this addition, would read till EOF and fail.
         }

         completed += blocksize;
      }

      if (! p_src->close()) {
         sprintf(errormsg, "Failed to close src file: %s", src_file->path);
         errsend(NONFATAL, errormsg);
         free(ibuf);
         free(obuf);
         return -1;
      }

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
int update_stats(PathPtr      p_src,
                 PathPtr      p_dst,
                 struct options& o) {

    int            rc;
    char           errormsg[MESSAGESIZE];
    int            mode;


    // don't touch the destination, unless this is a COPY
    if (o.work_type != COPYWORK)
       return 0;

    // Make a path_item matching <dest_file>, using <src_file>->dest_ftype
    // NOTE: Path::follow() is false, by default
    path_item  dest_copy(p_dst->node());
    dest_copy.ftype = p_src->dest_ftype();
    PathPtr p_dest(PathFactory::create_shallow(&dest_copy));

    // if running as root, always update <dest_file> owner  (without following links)
    // non-root user can also attempt this, by setting "preserve" (with -o)
    if (0 == geteuid() || o.preserve) {
       if (! p_dest->lchown(p_src->st().st_uid, p_src->st().st_gid)) {
          errsend_fmt(NONFATAL, "update_stats -- Failed to chown %s: %s\n",
                      p_dest->path(), p_dest->strerror());
       }
    }

    // ignore symlink destinations
    if (p_src->is_link())
        return 0;


    // perform any final adjustments on destination, before we set atime/mtime
    p_dest->post_process(p_src);

    // update <dest_file> access-permissions
    mode = p_src->mode() & 07777;
    if (! p_dest->chmod(mode)) {
       errsend_fmt(NONFATAL, "update_stats -- Failed to chmod fuse chunked file %s: %s\n",
                   p_dest->path(), p_dest->strerror());
    }

    // update <dest_file> atime and mtime
    struct timespec times[2];

    times[0].tv_sec  = p_src->st().st_atim.tv_sec;
    times[0].tv_nsec = p_src->st().st_atim.tv_nsec;

    times[1].tv_sec  = p_src->st().st_mtim.tv_sec;
    times[1].tv_nsec = p_src->st().st_mtim.tv_nsec;

    if (! p_dest->utimensat(times, AT_SYMLINK_NOFOLLOW)) {
       errsend_fmt(NONFATAL, "update_stats -- Failed to change atime/mtime %s: %s\n",
                   p_dest->path(), p_dest->strerror());
    }
   
    if(!p_src->get_packable()) {
       const char* plus = strrchr((const char*)p_dest->path(), '+');
       if (plus) {
          size_t  p_dest_orig_len = plus - p_dest->path();
          PathPtr p_dest_orig(p_dest->path_truncate(p_dest_orig_len));

          if(! p_dest->rename(p_dest_orig->path())) {
             errsend_fmt(FATAL, "update_stats -- Failed to rename %s "
                         "to original file path %s\n",
                         p_dest->path(), p_dest_orig->path());
          }
          // remove this?  potential deadlock on final chunk, if OUTPUT_PROC is already gone.
          else if (o.verbose >= 1) {
             output_fmt(0, "INFO  DATACOPY Renamed temp-file %s to %s\n",
                        p_dest->path(), p_dest_orig->path());
          }

          p_dest = p_dest_orig;
       }
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
#ifdef MPI_DEBUG
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PRINT_MPI_DEBUG("rank %d: Sending command %d to target rank %d\n", rank, type_cmd, target_rank);
#endif
    if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, target_rank, MPI_COMM_WORLD) != MPI_SUCCESS) {//Tell a rank it's time to begin processing
        fprintf(stderr, "Failed to send command %d to rank %d\n", type_cmd, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
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

void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize) {
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
    dequeue_buf_list(workbuflist, workbuftail, workbufsize);
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

void send_manager_examined_stats(int num_examined_files, size_t num_examined_bytes, int num_examined_dirs, size_t num_finished_bytes) {
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
    if (MPI_Send(&num_finished_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to send num_finished_bytes %zd to rank %d\n", num_finished_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

}

void send_manager_regs_buffer(path_item *buffer, int *buffer_count) {
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, PROCESSCMD, buffer, buffer_count);
}

void send_manager_dirs_buffer(path_item *buffer, int *buffer_count) {
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, DIRCMD, buffer, buffer_count);
}

void send_manager_new_buffer(path_item *buffer, int *buffer_count) {
    //send manager new inputs
    send_path_buffer(MANAGER_PROC, INPUTCMD, buffer, buffer_count);
}

void send_manager_work_done(int ignored) {
    //the worker is finished processing, notify the manager
    send_command(MANAGER_PROC, WORKDONECMD);
}

void send_manager_timing_stats(int tot_stats, int pod_id, int total_blk, size_t timing_stats_buff_size, char* repo, char* timing_stats)
{
   send_command(MANAGER_PROC, STATS);
   char* cursor;
   char* buffer = (char*)malloc(sizeof(int) * 3 + sizeof(size_t) + MARFS_MAX_REPO_SIZE);

   cursor = buffer;

   memcpy(cursor, &tot_stats, sizeof(int));
   cursor += sizeof(int);

   memcpy(cursor, &pod_id, sizeof(int));
   cursor += sizeof(int);

   memcpy(cursor, &total_blk, sizeof(int));
   cursor += sizeof(int);

   memcpy(cursor, &timing_stats_buff_size, sizeof(size_t));
   cursor += sizeof(size_t);

   memcpy(cursor, repo, MARFS_MAX_REPO_SIZE);
   
   //send metadata of timing stats
   if(MPI_Send(buffer, sizeof(int) * 3 + sizeof(size_t) + MARFS_MAX_REPO_SIZE, MPI_CHAR, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS)
      {
         fprintf(stderr, "Failed to send metadata of timing stats to rank %d\n", MANAGER_PROC);
         MPI_Abort(MPI_COMM_WORLD, -1);
      }

   //send timing_stats buffer
   if(MPI_Send(timing_stats, timing_stats_buff_size, MPI_CHAR, MANAGER_PROC, MANAGER_PROC, MPI_COMM_WORLD) != MPI_SUCCESS)
      {
         fprintf(stderr, "Failed to send timing stats buffer to rank %d\n", MANAGER_PROC);
      }

   free(buffer);
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
    else if (log == 2) {
        send_command(OUTPUT_PROC, LOGONLYCMD);
    }

    //send the message
    if (MPI_Send((void*)message, MESSAGESIZE, MPI_CHAR, OUTPUT_PROC, OUTPUT_PROC, MPI_COMM_WORLD) != MPI_SUCCESS) {
        fprintf(stderr, "Failed to message to rank %d\n", OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}


// This allows caller to use inline formatting, without first snprintf() to
// a local errmsg-buffer.  Like so:
//
//    output_fmt(1, "rank %d hello!", rank);
//
void output_fmt(int log, const char* format, ...) {
   char     msg[MESSAGESIZE];
   va_list  args;

   va_start(args, format);
   vsnprintf(msg, MESSAGESIZE, format, args);
   va_end(args);

   // msg[MESSAGESIZE -1] = 0;  /* no need for this */
   write_output(msg, log);
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

void send_worker_readdir(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize) {
    //send a worker a buffer list of paths to stat
    send_buffer_list(target_rank, DIRCMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_copy_path(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize) {
    //send a worker a list buffers with paths to copy
    send_buffer_list(target_rank, COPYCMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_compare_path(int target_rank, work_buf_list  **workbuflist, work_buf_list  **workbuftail, int *workbufsize) {
    //send a worker a list buffers with paths to compare
    send_buffer_list(target_rank, COMPARECMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_exit(int target_rank) {
    //order a rank to exit
    send_command(target_rank, EXITCMD);
}

static void errsend_internal(Lethality fatal, const char* errormsg) {
    write_output(errormsg, 1);

    if (fatal) {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else {
        send_manager_nonfatal_inc();
    }
}

//functions that workers use
void errsend(Lethality fatal, const char *error_text) {
    //send an error message to the outputproc. Die if fatal.
    char errormsg[MESSAGESIZE];

    if (fatal)
       snprintf(errormsg, MESSAGESIZE, "ERROR FATAL: %s\n", error_text);
    else
       snprintf(errormsg, MESSAGESIZE, "ERROR NONFATAL: %s\n", error_text);

    // errormsg[MESSAGESIZE -1] = 0; /* no need for this */
    errsend_internal(fatal, errormsg);
}

// This allows caller to use inline formatting, without first snprintf() to
// a local errmsg-buffer.  Like so:
//
//    errsend_fmt(NONFATAL, "rank %d hello!", rank);
//
void errsend_fmt(Lethality fatal, const char* format, ...) {
   char     errormsg[MESSAGESIZE];
   va_list  args;

   snprintf(errormsg, MESSAGESIZE, "ERROR %sFATAL: ", (fatal ? "" : "NON"));
   size_t offset = strlen(errormsg);

   va_start(args, format);
   vsnprintf(errormsg+offset, MESSAGESIZE-offset, format, args);
   va_end(args);

   // errormsg[MESSAGESIZE -1] = 0;  /* no need for this */
   errsend_internal(fatal, errormsg);
}

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
    int         numchars;
    char        linkname[PATHSIZE_PLUS];

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
       if ( under_mdfs_top(work_node->path) ) {
           return -1;
       }

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

#ifdef GEN_SYNDATA
    // --- is it a Synthetic Data path?
    if (! got_type) {
        if (o.syn_size && isSyndataPath(work_node->path)) { 
           int dlvl;        // directory level, if a directory. Currently ignored.
           if (rc = syndataSetAttr(work_node->path,&st,&dlvl,o.syn_size))
              return -1;    // syndataSetAttr() returns non-zero on failure
           work_node->ftype = SYNDATA;
           got_type = true;
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

    work_node->st = st;
    return 0;
}

// <fs> is actually a SrcDstFSType.  If you have <sys/vfs.h>, then initialize
// <fs> to match the type of <path>.  Otherwise, call it ANYFS.
//
// QUESTION: Are we using fprintf() + MPI_Abort(), instead of errsend_fmt()
//    because of OUTPUT_PROC might not be available, or is this just
//    from before errsend_fmt() was available?

void get_stat_fs_info(const char *path, SrcDstFSType *fs) {

#ifdef HAVE_SYS_VFS_H
    struct stat st;
    struct statfs stfs;
    char errortext[MESSAGESIZE];
    int rc;
    char use_path[PATHSIZE_PLUS];

    strncpy(use_path, path, PATHSIZE_PLUS);
    if (use_path[PATHSIZE_PLUS -1]) {  // strncpy() is unsafe
       fprintf(stderr, "Oversize path '%s'\n", path);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }

    // look at <path>, or, if that fails, look at dirname(<path>)
    PathPtr p(PathFactory::create(use_path));
    if (! p) {
       fprintf(stderr, "PathFactory couldn't interpret path %s\n", use_path);
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else if (! p->stat()) {
       char* use_path_copy = strdup(use_path); // dirname() may alter arg
       strncpy(use_path, dirname(use_path_copy), PATHSIZE_PLUS);
       free(use_path_copy);

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
       else if (p->node().ftype == SYNDATA) {
          *fs = SYNDATAFS;
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


// NOTE: master spending ~50% time in this function.
// all callers provide <start_range>==START_PROC
int get_free_rank(struct worker_proc_status *proc_status, int start_range, int end_range) {
    //given an inclusive range, return the first encountered free rank
    int i;
    for (i = start_range; i <= end_range; i++) {
        if (proc_status[i].inuse == 0) {
            return i;
        }
    }
    return -1;
}

//are all the ranks free?
// return 1 for yes, 0 for no.
int processing_complete(struct worker_proc_status *proc_status, int free_worker_count, int nproc) {
#if 0
    int i;
    int count = 0;
    for (i = 0; i < nproc; i++) {
        if (proc_status[i].inuse == 1)
           return 0;
    }
    return 1;

#else
    if (free_worker_count == (nproc - START_PROC)) {
       int i;
       for (i = 0; i < START_PROC; i++) {
          if (proc_status[i].inuse)
             return 0;
       }
       return 1;
    }
    return 0;

#endif
}

//Queue Function Definitions

// push path onto the tail of the queue
void enqueue_path(path_list **head, path_list **tail, char *path, int *count) {
    path_list *new_node = (path_list*)malloc(sizeof(path_list));
    memset(new_node, 0, sizeof(path_list));
    if (! new_node) {
       errsend_fmt(FATAL, "Failed to allocate %lu bytes for new_node\n", sizeof(path_list));
    }

    strncpy(new_node->data.path, path, PATHSIZE_PLUS);
    if (new_node->data.path[PATHSIZE_PLUS -1]) { // strncpy() is unsafe
       errsend_fmt(FATAL, "enqueue_path: Oversize path '%s'\n", path);
    }

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
    memset(temp_node, 0, sizeof(path_list));
    if (! temp_node) {
       errsend_fmt(FATAL, "Failed to allocate %lu bytes for temp_node\n", sizeof(path_list));
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


void enqueue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize, char *buffer, int buffer_size) {

    work_buf_list *new_buf_item = (work_buf_list*)malloc(sizeof(work_buf_list));
    memset(new_buf_item, 0, sizeof(work_buf_list));
    if (! new_buf_item) {
       fprintf(stderr, "Failed to allocate %lu bytes for new_buf_item\n", sizeof(work_buf_list));
       MPI_Abort(MPI_COMM_WORLD, -1);
    }
    new_buf_item->buf = buffer;
    new_buf_item->size = buffer_size;
    new_buf_item->next = NULL;

    if (*workbufsize < 0) {
        *workbufsize = 0;
    }

    if (*workbuflist == NULL) {
        *workbuflist = new_buf_item;
        *workbuftail = new_buf_item;
        *workbufsize = 1;
        return;
    }

    (*workbuftail)->next = new_buf_item;
    *workbuftail = new_buf_item;
    (*workbufsize)++;
}

void dequeue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize) {
    if (*workbuflist == NULL)
        return;

    if (*workbuftail == *workbuflist)
       *workbuftail = NULL;

    work_buf_list *new_head = (*workbuflist)->next;
    free((*workbuflist)->buf);
    free(*workbuflist);

    *workbuflist = new_head;
    (*workbufsize)--;
}

void delete_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize) {
    while (*workbuflist) {
       dequeue_buf_list(workbuflist, workbuftail, workbufsize);
    }
    *workbufsize = 0;
}

void pack_list(path_list *head, int count, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize) {
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
            enqueue_buf_list(workbuflist, workbuftail, workbufsize, buffer, buffer_size);
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
       enqueue_buf_list(workbuflist, workbuftail, workbufsize, buffer, buffer_size);
    }
}

/**
 * This function tests the metadata of the two nodes
 * to see if they are the same. For files that are chunkable,
 * it looks to see if CTM (chunk transfer metadata) exists for 
 * the source file. If it does, then it assumes that there was 
 * an aborted transfer, and the files are NOT the same!
 *
 * check size, mtime, mode, and owners
 * 
 * NOTE: S3 objects DO NOT HAVE create-time or access-time.  Therefore,
 *    their metadata is initialized with ctime=mtime, and atime=mtime.
 *    Therefore, if you compare a POSIX file having values for ctime,
 *    atime, and mtime that are not all the same, with any S3 object, the
 *    two sets of metadata will *always* differ in these values, even if
 *    you freshly create them and set all these values to match the
 *    original.  Therefore, it might make sense to reconsider this test.
 *
 * @param src        a path_item structure containing
 *           the metadata for the souce file
 * @param dst        a path_item structure containing
 *           the metadata for the destination
 *           file
 * @param o      the PFTOOL global options structure
 *
 * @return 1 (TRUE) if the files are "the same", or 0 otherwise.  If the
 *   command-line includes an option to skip copying files that have
 *   already been done (-n), then the copy will be skipped if files are
 *   "the same".
 */

// TBD: Use Path methods to avoid over-reliance on POSIX same-ness
int samefile(PathPtr p_src, PathPtr p_dst, const struct options& o) {
    const path_item& src = p_src->node();
    const path_item& dst = p_dst->node();

    // compare metadata - check size, mtime, mode, and owners
    // (satisfied conditions -> "same" file)

    if (src.st.st_size == dst.st.st_size
        && (src.st.st_mtime == dst.st.st_mtime || S_ISLNK(src.st.st_mode))
#if 0
        // by removing this we no longer care about file permissions for transfers.
        // Probably the right choice, but revisit if needed
        && (src.st.st_mode == dst.st.st_mode)

        // non-root doesn't chown unless '-o'
        && (((src.st.st_uid == dst.st.st_uid) && (src.st.st_gid == dst.st.st_gid))
            || (geteuid() && !o.preserve))
#else
        // only chown if preserve set
        && (((src.st.st_uid == dst.st.st_uid) && (src.st.st_gid == dst.st.st_gid))
            || !o.preserve)
#endif
        ) {

       // if a chunkable file matches metadata, but has CTM,
       // then files are NOT the same.
       if ((o.work_type == COPYWORK)
           && (src.st.st_size >= o.chunk_at)
           && (hasCTM(dst.path)))
          return 0;

       // class-specific techniques (e.g. MarFS file has RESTART?)
       if (p_dst->incomplete())
          return 0;

       // Files are the "same", as far as MD is concerned.
       return 1;
    }

    return 0;
}

int epoch_to_string(char* str, size_t size, const time_t* time) {
   struct tm tm;

   LOG(LOG_INFO, " epoch_to_str epoch:            %016lx\n", *time);

   // time_t -> struct tm
   if (! localtime_r(time, &tm)) {
      LOG(LOG_ERR, "localtime_r failed: %s\n", strerror(errno));
      return -1;
   }

   size_t strf_size = strftime(str, size, MARFS_DATE_FORMAT, &tm);
   if (! strf_size) {
      LOG(LOG_ERR, "strftime failed even more than usual: %s\n", strerror(errno));
      return -1;
   }

   //   // DEBUGGING
   //   LOG(LOG_INFO, " epoch_2_str to-string (1)      %s\n", str);

   // add DST indicator
   snprintf(str+strf_size, size-strf_size, MARFS_DST_FORMAT, tm.tm_isdst);

   //   // DEBUGGING
   //   LOG(LOG_INFO, " epoch_2_str to-string (2)      %s\n", str);

   return 0;
}



// <p_src>    is the (unaltered) source
// <out_node> is the (unaltered) destination
//
// see comments at check_ctm_match()

int check_temporary(PathPtr p_src, path_item* out_node)
{
   time_t src_mtime = p_src->mtime();
   char   src_mtime_str[DATE_STRING_MAX];
   char   src_to_hash[PATHSIZE_PLUS]; // p_src->path() + mtime string

   epoch_to_string(src_mtime_str, DATE_STRING_MAX, &src_mtime);

   snprintf(src_to_hash, PATHSIZE_PLUS, "%s+%s", p_src->path(), src_mtime_str);
   // src_to_hash[PATHSIZE_PLUS -1] = 0; /* no need for this */

   return check_ctm_match(out_node->path, src_to_hash);
}
