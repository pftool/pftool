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
#include "ctm.h" // hasCTM()
#include "sig.h"
#include "debug.h"

#include <syslog.h>
#include <signal.h>
#include <math.h>

#include <pthread.h> // manager_sig_handler()

// EXITCMD, or ctl-C
volatile int worker_exit = 0;

void usage()
{
    // print usage statement
    printf("********************** PFTOOL USAGE ************************************************************\n");
    printf("\n");
    printf("\npftool: parallel file tool utilities\n");
    printf("1. Walk through directory tree structure and gather statistics on files and\n");
    printf("   directories encountered.\n");
    printf("2. Apply various data moving operations based on the selected options \n");
    printf("\n");
    printf("mpirun -np <totalprocesses> pftool [options]\n");
    printf("For general use please use the helper scripts (pfcp/pfcm/pfls) located in in {install_prefix}/bin/\n");
    printf("\n");
    printf(" Options\n");
    printf(" [-p]         path to start parallel tree walk (required argument)\n");
    printf(" [-c]         destination path for data movement\n");
    printf(" [-j]         unique jobid for the pftool job\n");
    printf(" [-w]         work type: { 0=copy | 1=list | 2=compare}\n");
    printf(" [-i]         process paths in a file list instead of walking the file system\n");
    printf(" [-s]         block size for COPY and COMPARE\n");
    printf(" [-C]         file size to start chunking (for N:1)\n");
    printf(" [-S]         chunk size for COPY\n");
    printf(" [-n]         only operate on file if different (aka 'restart')\n");
    printf(" [-r]         recursive operation down directory tree\n");
    printf(" [-t]         specify file system type of destination file/directory\n");
    printf(" [-l]         turn on logging to syslog\n");
    printf(" [-P]         force destination to be treated as parallel (i.e. assume N:1 support)\n");
    printf(" [-D]         perform block-compare, default: metadata-compare\n");
    printf(" [-o]         attempt to preserve source ownership (user/group) in COPY\n");
    printf(" [-e]         excludes files that match this pattern\n");
    printf(" [-v]         output verbosity [specify multiple times, to increase]\n");
    printf(" [-g]         debugging-level  [specify multiple times, to increase]\n");
    printf(" [-M]         The maximum number of readdir ranks, not limited if not specified (default \"-1\")\n");
    printf(" [-W]         Attempt O_DIRECT data writes if possible\n");
    printf(" [-R]         Attempt O_DIRECT data reads if possible\n");
    printf(" [-h]         print Usage information\n");
    printf("\n");

    printf("      [if configured with --enable-syndata\n");
    printf(" [-X]         specify a synthetic data pattern file or constant. default: none\n");
    printf(" [-x]         synthetic file size. If specified, file(s) will be synthetic data of specified size\n");
    printf(" \n");

    printf("********************** PFTOOL USAGE ************************************************************\n");
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
const char *cmd2str(OpCode cmdidx)
{
    static const char *CMDSTR[] = {
        "EXITCMD", "UPDCHUNKCMD", "BUFFEROUTCMD", "OUTCMD", "LOGCMD", "LOGONLYCMD", "QUEUESIZECMD", "STATCMD", "COMPARECMD", "COPYCMD", "PROCESSCMD", "INPUTCMD", "DIRCMD", "WORKDONECMD", "NONFATALINCCMD", "CHUNKBUSYCMD", "COPYSTATSCMD", "EXAMINEDSTATSCMD", "ADDTIMINGCMD", "SHOWTIMINGCMD"};

    return ((cmdidx > SHOWTIMINGCMD) ? "Invalid Command" : CMDSTR[cmdidx]);
}

// print the mode <aflag> into buffer <buf> in a regular 'pretty' format
char *printmode(mode_t aflag, char *buf)
{

    static int m0[] = {1, S_IREAD >> 0, 'r', '-'};
    static int m1[] = {1, S_IWRITE >> 0, 'w', '-'};
    static int m2[] = {3, S_ISUID | S_IEXEC, 's', S_IEXEC, 'x', S_ISUID, 'S', '-'};

    static int m3[] = {1, S_IREAD >> 3, 'r', '-'};
    static int m4[] = {1, S_IWRITE >> 3, 'w', '-'};
    static int m5[] = {3, S_ISGID | (S_IEXEC >> 3), 's',
                       S_IEXEC >> 3, 'x', S_ISGID, 'S', '-'};
    static int m6[] = {1, S_IREAD >> 6, 'r', '-'};
    static int m7[] = {1, S_IWRITE >> 6, 'w', '-'};
    static int m8[] = {3, S_ISVTX | (S_IEXEC >> 6), 't', S_IEXEC >> 6, 'x', S_ISVTX, 'T', '-'};
    static int *m[] = {m0, m1, m2, m3, m4, m5, m6, m7, m8};

    int i, j, n;
    int *p = (int *)1;
    buf[0] = S_ISREG(aflag) ? '-' : S_ISDIR(aflag) ? 'd'
                                : S_ISLNK(aflag)   ? 'l'
                                : S_ISFIFO(aflag)  ? 'p'
                                : S_ISCHR(aflag)   ? 'c'
                                : S_ISBLK(aflag)   ? 'b'
                                : S_ISSOCK(aflag)  ? 's'
                                                   : '?';

    for (i = 0; i <= 8; i++)
    {
        for (n = m[i][0], j = 1; n > 0; n--, j += 2)
        {
            p = m[i];
            if ((aflag & p[j]) == p[j])
            {
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
int mkpath(char *thePath, mode_t perms)
{
    char *slash = thePath; // point at the current "/" in the path
    struct stat sbuf;      // a buffer to hold stat information
    int save_errno;        // errno from mkdir()

    while (*slash == '/')
        slash++; // burn through any leading "/". Note that if no leading "/",
    // then thePath will be created relative to CWD of process.
    while (slash = strchr(slash, '/'))
    { // start parsing thePath
        *slash = '\0';

        if (stat(thePath, &sbuf))
        { // current path element cannot be stat'd - assume does not exist
            if (mkdir(thePath, perms))
            {                       // problems creating the directory - clean up and return!
                save_errno = errno; // save off errno - in case of error...
                *slash = '/';
                return (save_errno);
            }
        }
        else if (!S_ISDIR(sbuf.st_mode))
        { // element exists but is NOT a directory
            *slash = '/';
            return (ENOTDIR);
        }
        *slash = '/';
        slash++; // increment slash ...
        while (*slash == '/')
            slash++; // burn through any blank path elements
    }                // end mkdir loop

    if (stat(thePath, &sbuf))
    {                                    // last path element cannot be stat'd - assume does not exist
        if (mkdir(thePath, perms))       // problems creating the directory - clean up and return!
            return (save_errno = errno); // save off errno - just to be sure ...
    }
    else if (!S_ISDIR(sbuf.st_mode)) // element exists but is NOT a directory
        return (ENOTDIR);

    return (0);
}

// unused?
// convert up to 28 bytes of <b> to ASCII-hex.
void hex_dump_bytes(char *b, int len, char *outhexbuf)
{
    char smsg[64] = {0};
    char tmsg[3] = {0};
    unsigned char *ptr;
    int start = 0;

    ptr = (unsigned char *)(b + start); /* point to buffer location to start  */
    /* if last frame and more lines are required get number of lines */
    memset(smsg, '\0', 64);

    short str_index;
    short str_max = 28; // 64 - (2 *28) = room for terminal-NULL
    if (len < 28)
        str_max = len;

    for (str_index = 0; str_index < str_max; str_index++)
    {
        sprintf(tmsg, "%02X", ptr[str_index]);
        strncat(smsg, tmsg, 2); // controlled, no overflow
    }
    sprintf(outhexbuf, "%s", smsg);
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
ssize_t write_field(int fd, void *start, size_t len)
{
    size_t n;                     // number of bytes written for a given call to write()
    ssize_t tot = 0;              // total number of bytes written
    char *wstart = (char *)start; // the starting point in the buffer
    size_t wcnt = len;            // the running count of bytes to write

    while (wcnt > 0)
    {
        if (!(n = write(fd, wstart, wcnt))) // if nothing written -> assume error
            return ((ssize_t)-errno);
        tot += n;
        wstart += n; // increment the start address by n
        wcnt -= n;   // decreamnt byte count by n
    }

    return (tot);
}

// remove trailing chars w/out repeated calls to strlen();
// inline
void trim_trailing(int ch, char *path)
{
    if (path)
    {
        for (size_t pos = strlen(path) - 1; ((pos > 0) && (path[pos] == ch)); --pos)
        {
            path[pos] = '\0';
        }
    }
}

// Install path into <item>.
// We assume <base_path> has size at least PATHSIZE_PLUS.
void get_base_path(char *base_path,
                   const path_item *item,
                   int wildcard)
{ // (<wildcard> is boolean)

    char dir_name[PATHSIZE_PLUS] = {0};
    struct stat st;
    int rc;
    char *path = (char *)item->path;

    PathPtr p(PathFactory::create(item));
    if (!p->stat())
    {
        fprintf(stderr, "get_base_path -- Failed to stat path %s\n", path);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    st = p->st();

    char *path_copy = strdup(path); // dirname() may alter arg
    strncpy(dir_name, dirname(path_copy), PATHSIZE_PLUS);
    free(path_copy);

    if ((strncmp(".", dir_name, PATHSIZE_PLUS) == 0) && S_ISDIR(st.st_mode))
    {
        strncpy(base_path, path, PATHSIZE_PLUS);
    }
    else if (S_ISDIR(st.st_mode) && wildcard == 0)
    {
        strncpy(base_path, path, PATHSIZE_PLUS);
    }
    else
    {
        strncpy(base_path, dir_name, PATHSIZE_PLUS);
    }
    trim_trailing('/', base_path);
}

// To the tail of <dest_path>, add '/' if needed, then append the last part
// of the path in <beginning_node> (i.e. what 'basename' would return).
// Put the result into dest_node->path.
void get_dest_path(path_item *dest_node,  // fill this in
                   const char *dest_path, // from command-line arg
                   const path_item *beginning_node,
                   int makedir,
                   int num_paths,
                   struct options &o)
{
    int rc;
    struct stat beg_st;
    struct stat dest_st;
    char temp_path[PATHSIZE_PLUS] = {0};
    char *result = dest_node->path;
    char *path_slice;

    memset(dest_node, 0, sizeof(path_item));   // zero-out header-fields
    strncpy(result, dest_path, PATHSIZE_PLUS); // install dest_path
    if (result[PATHSIZE_PLUS - 1])             // strncpy() is unsafe
        errsend_fmt(FATAL, "Oversize path '%s'\n", dest_path);

    dest_node->ftype = TBD; // we will figure out the file type later

    strncpy(temp_path, beginning_node->path, PATHSIZE_PLUS);
    if (temp_path[PATHSIZE_PLUS - 1])
    { // strncpy() is unsafe
        errsend_fmt(FATAL, "Not enough room to append '%s' + '%s'\n",
                    temp_path, beginning_node->path);
    }

    trim_trailing('/', temp_path);

    //recursion special cases
    if (o.recurse && (strncmp(temp_path, "..", PATHSIZE_PLUS) != 0) && (o.work_type != COMPAREWORK))
    {

        beg_st = beginning_node->st;

        PathPtr d_dest(PathFactory::create(dest_path));
        dest_st = d_dest->st();
        if (d_dest->exists() && S_ISDIR(dest_st.st_mode) && S_ISDIR(beg_st.st_mode) && (num_paths == 1))
        {

            // append '/' to result
            size_t result_len = strlen(result);
            if (result[result_len - 1] != '/')
            {
                strncat(result, "/", PATHSIZE_PLUS - result_len);
                if (result[PATHSIZE_PLUS - 1])
                { // strncat() is unsafe
                    errsend_fmt(FATAL, "Not enough room to append '%s' + '/'\n",
                                result);
                }
            }

            // append tail-end of beginning_node's path
            char *last_slash = strrchr(temp_path, '/');
            if (last_slash)
                path_slice = last_slash + 1;
            else
                path_slice = (char *)temp_path;

            strncat(result, path_slice, PATHSIZE_PLUS - strlen(result) - 1);
            if (result[PATHSIZE_PLUS - 1])
            { // strncat() is unsafe
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

void get_output_path(path_item *out_node, // fill in out_node.path
                     const char *base_path,
                     const path_item *src_node,
                     const path_item *dest_node,
                     struct options &o,
                     int rename_flag)
{

    const char *path_slice;
    int path_slice_duped = 0;

    // start clean
    memset(out_node, 0, sizeof(path_item));

    // marfs may want to know chunksize
    out_node->chksz = dest_node->chksz;
    out_node->chkidx = dest_node->chkidx;

    //remove trailing slash(es)
    // NOTE: both are the same size, and dest_node has already been assured to have
    //       a terminal-NULL in get_dest_path(), so strncpy() okay.
    strncpy(out_node->path, dest_node->path, PATHSIZE_PLUS);

    trim_trailing('/', out_node->path);
    ssize_t remain = PATHSIZE_PLUS - strlen(out_node->path) - 1;

    //path_slice = strstr(src_path, base_path);
    if (o.recurse == 0)
    {
        const char *last_slash = strrchr(src_node->path, '/');
        if (last_slash)
            path_slice = last_slash + 1;
        else
            path_slice = (char *)src_node->path;
    }
    else
    {
        if (strcmp(base_path, ".") == 0)
            path_slice = (char *)src_node->path;
        else
        {
            path_slice = strdup(src_node->path + strlen(base_path) + 1);
            path_slice_duped = 1;
        }
    }

    // assure there is enough room to append path_slice
    size_t slice_len = strlen(path_slice);
    if (slice_len > remain)
    {
        out_node->path[0] = 0;
        return;
    }
    remain -= slice_len;

    if (S_ISDIR(dest_node->st.st_mode))
    {
        strcat(out_node->path, "/");
        strcat(out_node->path, path_slice);
    }
    if (path_slice_duped)
        free((void *)path_slice);
#ifdef TMPFILE
    if ((rename_flag == 1) && (src_node->packable == 0) && strcmp(dest_node->path, "/dev/null"))
    {
        //need to create temporary file name

        // assure there is room
        if (remain < DATE_STRING_MAX + 1)
        {
            out_node->path[0] = 0;
            return;
        }
        remain -= DATE_STRING_MAX + 1;

        strcat(out_node->path, "+");

        // construct temp-file pathname
        time_t mtime = src_node->st.st_mtime;
        char timestamp[DATE_STRING_MAX] = {0};
        epoch_to_string(timestamp, DATE_STRING_MAX, &mtime);
        strcat(out_node->path, timestamp);
    }
#endif
    out_node->path[PATHSIZE_PLUS - 1] = 0;
}

int one_byte_read(const char *path)
{
    int fd, bytes_processed;
    char data;
    int rc = 0;
    char errormsg[MESSAGESIZE] = {0};
    fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        sprintf(errormsg, "Failed to open file %s for read", path);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    bytes_processed = read(fd, &data, 1);
    if (bytes_processed != 1)
    {
        sprintf(errormsg, "%s: Read %d bytes instead of %d", path, bytes_processed, 1);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    rc = close(fd);
    if (rc != 0)
    {
        sprintf(errormsg, "Failed to close file: %s", path);
        errsend(NONFATAL, errormsg);
        return -1;
    }
    return 0;
}

//take a src, dest, offset and length. Copy the file and return >=0 on
//success, -1 on failure.  [0 means copy succeeded, 1 means a "deemed"
//success.]
int copy_file(PathPtr p_src,
              PathPtr p_dest,
              size_t blocksize,
              int rank,
              struct options &o)
{
    //MPI_Status status;
    int rc = 0;
    size_t completed = 0;
    char *buf = NULL;
    char errormsg[MESSAGESIZE] = {0};
    int err = 0; // non-zero -> close src/dest, free buf
    int flags;
    off_t offset = (p_src->node().chkidx * p_src->node().chksz);
    off_t length = (((offset + p_src->node().chksz) > p_src->size())
                        ? (p_src->size() - offset)
                        : p_src->node().chksz);
    ssize_t bytes_processed = 0;

    //symlink
    char link_path[PATHSIZE_PLUS] = {0};
    int numchars;
    int page_size = getpagesize();
    int read_flags = O_RDONLY;
    off_t aligned_read_size = 0;
    off_t aligned_read_offset = 0;
    off_t aligned_read_offset_adjust = 0;
    int write_flags = O_WRONLY | O_CREAT;

    // only write O_DIRECT if requested *and* page-aligned *and* blocksize is aligned
    if (o.direct_write && (((length / page_size) * page_size) == length) && (((blocksize / page_size) * page_size) == blocksize)) {
        write_flags |= O_DIRECT;
    }

    // only read O_DIRECT if requested, handle block size and offset below in read path for alignment
    // read blocksize will be adjusted to be page aligned no matter what in the read loop
    if (o.direct_read) {
        read_flags |= O_DIRECT;
    }

    // If source is a link, create similar link on the destination-side.
    //can't be const for MPI_IO
    //
    // NOTE: The only way this can be a link is if it's on a quasi-POSIX
    //       system.  So we can just readlink(), to get the link-target.
    //       But the destination could be anything, so we need a
    //       Path::symlink() to implement that.
    if (p_src->is_link())
    {

        // <link_path> = name of link-destination
        numchars = p_src->readlink(link_path, PATHSIZE_PLUS);
        if (numchars < 0)
        {
            errsend_fmt(NONFATAL, "Failed to read link %s\n", p_src->path());
            return -1;
        }
        else if (numchars >= PATHSIZE_PLUS)
        {
            errsend_fmt(NONFATAL, "readlink %s, not enough room for '%s'\n", p_src->path());
            return -1;
        }
        link_path[numchars] = '\0';

        ////        rc = symlink(link_path, dest_file->path);
        ////        if (rc < 0)
        if (!p_dest->symlink(link_path) && strcmp(p_dest->class_name().get(), "NULL_Path"))
        {
            errsend_fmt(NONFATAL, "Failed to create symlink %s -> %s\n", p_dest->path(), link_path);
            return -1;
        }

        if (update_stats(p_src, p_dest, o) != 0)
        {
            return -1;
        }
        return 0;
    }

    //a file less than configured I/O size
    if (length < blocksize)
    { // a file < blocksize in size
        blocksize = length;
    }

    // round up to the nearest page size for read size, plus one page in case we have to shift the starting offset
    aligned_read_size = ((ceil((double)blocksize / (double)page_size) + 1) * page_size);
    if (blocksize)
    {
        rc = posix_memalign((void **)&buf, page_size, aligned_read_size * sizeof(char));
        if (!buf || rc)
        {
            errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                        aligned_read_size, p_src->path());
            return -1;
        }

        memset(buf, '\0', aligned_read_size);
    }

    // OPEN source for reading (binary mode)
    if (!p_src->open(read_flags, p_src->mode()) && !p_src->open(read_flags & ~O_DIRECT, p_src->mode()))
    {
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
    if (p_src->size() <= length)
    {
        // no chunking
        PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT",
                        p_dest->fstype_to_str());
    }
    else
    {
        write_flags = write_flags | O_CONCURRENT_WRITE;
        PRINT_MPI_DEBUG("fstype = %s. Setting open flags to O_WRONLY | O_CREAT | O_CONCURRENT_WRITE",
                        p_dest->fstype_to_str());
    }

    // give destination the same mode as src, (access-bits only)
    // always add user write
    mode_t dest_mode = (p_src->mode() & (S_ISUID | S_ISGID | S_IRWXU | S_IRWXG | S_IRWXO)) | S_IWUSR;
    if (!p_dest->open(write_flags, dest_mode, offset, length) &&  !p_dest->open(write_flags & ~O_DIRECT, dest_mode, offset, length))
    {
        if (p_dest->get_errno() == EDQUOT)
        {
            errsend_fmt(FATAL, "Failed to open file %s for write (%s)\n",
                        p_dest->path(), p_dest->strerror());
        }
        else
        {
            errsend_fmt(NONFATAL, "Failed to open file %s for write (%s)\n",
                        p_dest->path(), p_dest->strerror());
        }
        p_src->close();
        if (buf)
            free(buf);
        return -1;
    }

    // copy contents from source to destination
    while (completed != length)
    {
        // .................................................................
        // READ data from source
        // .................................................................
        // remaining file data is smaller than configured I/O size
        if ((length - completed) < blocksize)
        {
            blocksize = (length - completed);
        }

        // round up to the nearest page size for the current I/O
        // align read offset to nearest page
        // track alignement offset so we can jump forward in the read buffer for the write call
        aligned_read_size = ((ceil((double)blocksize / (double)page_size)) * page_size);
        aligned_read_offset = ((floor((double)(offset + completed) / (double)page_size)) * page_size);
        aligned_read_offset_adjust = (offset + completed) - aligned_read_offset;

        // read an extra page if we're shifting the starting offset
        if (aligned_read_offset_adjust)
            aligned_read_size += page_size;


        bytes_processed = p_src->read(buf, aligned_read_size, aligned_read_offset);
        if (aligned_read_offset)
            PRINT_IO_DEBUG("Reading offset: %d instead of %d, %d bytes instead of %d bytes, adjust output by %d bytes\n", 
                           aligned_read_offset, (offset+completed), aligned_read_size, blocksize, aligned_read_offset_adjust);

        // if we didn't overread AND we didn't read to EOF, something is wrong
        if ((bytes_processed < 0) || (bytes_processed != aligned_read_size) && (bytes_processed < (blocksize+aligned_read_offset_adjust)))
        {
            errsend_fmt(NONFATAL, "Failed %s offs %ld read %ld bytes instead of %zd: %s\n",
                        p_src->path(), aligned_read_offset, bytes_processed, blocksize, p_src->strerror());
            err = 1;
            break; // return -1;
        }
        PRINT_IO_DEBUG("rank %d: copy_file() Read of %zd bytes ( offset adjust = %zd ) complete for file %s\n",
                       rank, bytes_processed, aligned_read_offset_adjust, p_dest->path());

        // .................................................................
        // WRITE data to destination
        // .................................................................

        // shift the output buffer to compensate for the alignment on the read
        bytes_processed = p_dest->write(buf+aligned_read_offset_adjust, blocksize, offset + completed);

        if (bytes_processed != blocksize)
        {
            errsend_fmt(NONFATAL, "Failed %s offs %ld wrote %ld bytes instead of %zd: %s\n",
                        p_dest->path(), offset + completed, bytes_processed, blocksize, p_dest->strerror());
            err = 1;
            break; // return -1;
        }
        completed += blocksize;
        PRINT_IO_DEBUG("rank %d: copy_file() Copy of %zd bytes complete for file %s\n",
                       rank, bytes_processed, p_dest->path());
    }

    // .................................................................
    // CLOSE source and destination
    // .................................................................

    if (!p_src->close())
    {
        errsend_fmt(NONFATAL, "Failed to close src file: %s (%s)\n",
                    p_src->path(), p_src->strerror());
    }

    if (!p_dest->close())
    {
        errsend_fmt(NONFATAL, "Failed to close dest file: %s (%s)\n",
                    p_dest->path(), p_dest->strerror());
        err = 1;
    }

    if (buf)
        free(buf);

    // even error-situations have now done clean-up
    if (err)
        return -1;

    if (offset == 0 && length == p_src->size())
    {
        PRINT_IO_DEBUG("rank %d: copy_file() Updating transfer stats for %s\n",
                       rank, p_dest->path());
        if (update_stats(p_src, p_dest, o))
        {
            return -1;
        }
    }

    return 0;
}

int compare_file(path_item *src_file,
                 path_item *dest_file,
                 size_t blocksize,
                 int meta_data_only,
                 struct options &o)
{

    ////    struct stat  dest_st;
    size_t completed = 0;
    char *ibuf;
    char *obuf;
    size_t bytes_processed;
    char errormsg[MESSAGESIZE] = {0};
    int rc;
    int crc;
    int page_size = getpagesize();
    off_t offset = (src_file->chkidx * src_file->chksz);
    off_t length = (((offset + src_file->chksz) > src_file->st.st_size)
                        ? (src_file->st.st_size - offset)
                        : src_file->chksz);

    int read_flags = O_RDONLY;

    // for now just do O_DIRECT only when asked and the page sizes line up
    // can be optimized further like copy_file for ragged edges but probably not worth the complexity
    if (o.direct_read && (((length / page_size) * page_size) == length) && (((blocksize / page_size) * page_size) == blocksize)) {
        read_flags |= O_DIRECT;
    }


    PathPtr p_src(PathFactory::create_shallow(src_file));
    PathPtr p_dest(PathFactory::create_shallow(dest_file));

    // assure dest exists
    if (!p_dest->stat())
        return 2;

    if (samefile(p_src, p_dest, o, -1))
    {

        //metadata compare
        if (meta_data_only)
        {
            return 0;
        }

        // special case for symlinks
        if (p_src->is_link()) {
            // quick check for non-link dest
            if( !p_dest->is_link() ) { return -1; }
            // allocate buffs to link tgt compare
            char src_link_path[PATHSIZE_PLUS] = {0};
            char dest_link_path[PATHSIZE_PLUS] = {0};
            int numchars;
            // actually perform readlink ops
            numchars = p_src->readlink(src_link_path, PATHSIZE_PLUS);
            if (numchars < 0)
            {
                errsend_fmt(NONFATAL, "Failed to read link %s\n", p_src->path());
                return -1;
            }
            else if (numchars >= PATHSIZE_PLUS)
            {
                errsend_fmt(NONFATAL, "readlink %s, not enough room for '%s'\n", p_src->path());
                return -1;
            }
            else if ( p_dest->readlink(dest_link_path, PATHSIZE_PLUS - 1) != numchars ) {
                errsend_fmt(NONFATAL, "symlink target mismatch for '%s' and '%s'\n", p_src->path(), p_dest->path());
                return -1;
            }
            // ensure NULL-terminator
            src_link_path[numchars] = '\0';
            dest_link_path[numchars] = '\0';
            // actually strcmp the targets
            if ( strncmp( src_link_path, dest_link_path, numchars ) ) {
                errsend_fmt(NONFATAL, "symlink target mismatch for '%s' and '%s'\n", p_src->path(), p_dest->path());
                return -1;
            }
            // symlinks match
            return 0;
        }

        //byte compare
        // allocate buffers and open files ...
        rc = posix_memalign((void **)&ibuf, 4096, blocksize * sizeof(char));
        if (!ibuf || rc)
        {
            errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                        blocksize, src_file->path);
            return -1;
        }

        rc = posix_memalign((void **)&obuf, 4096, blocksize * sizeof(char));
        if (!obuf || rc)
        {
            errsend_fmt(NONFATAL, "Failed to allocate %lu bytes for reading %s\n",
                        blocksize, dest_file->path);
            free(ibuf);
            return -1;
        }

        if (!p_src->open(read_flags, src_file->st.st_mode, offset, length) && !p_src->open(read_flags & ~O_DIRECT, src_file->st.st_mode, offset, length))
        {
            errsend_fmt(NONFATAL, "Failed to open file %s for compare source\n", p_src->path());
            free(ibuf);
            free(obuf);
            return -1;
        }

        if (!p_dest->open(read_flags, dest_file->st.st_mode, offset, length) && !p_dest->open(read_flags & ~O_DIRECT, dest_file->st.st_mode, offset, length))
        {
            errsend_fmt(NONFATAL, "Failed to open file %s for compare destination\n", p_dest->path());
            free(ibuf);
            free(obuf);
            return -1;
        }

        //incase someone accidently set an offset+length that exceeds the file bounds
        if ((src_file->st.st_size - offset) < length)
        {
            length = src_file->st.st_size - offset;
        }
        //a file less than blocksize
        if (length < blocksize)
        {
            blocksize = length;
        }
        crc = 0;
        while (completed != length)
        {

            // Wasteful?  If we fail to read blocksize, we'll have a problem
            // anyhow.  And if we succeed, then we'll wipe this all out with
            // the data, anyhow.  [See also memsets in copy_file()]
            //
            //            memset(ibuf, 0, blocksize);
            //            memset(obuf, 0, blocksize);

            //blocksize is too big
            if ((length - completed) < blocksize)
            {
                blocksize = (length - completed);
            }

            bytes_processed = p_src->read(ibuf, blocksize, completed + offset);
            if (bytes_processed != blocksize)
            {
                sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                        src_file->path, bytes_processed, blocksize);
                errsend(NONFATAL, errormsg);
                free(ibuf);
                free(obuf);
                return -1;
            }

            bytes_processed = p_dest->read(obuf, blocksize, completed + offset);
            if (bytes_processed != blocksize)
            {
                sprintf(errormsg, "%s: Read %zd bytes instead of %zd for compare",
                        dest_file->path, bytes_processed, blocksize);
                errsend(NONFATAL, errormsg);
                free(ibuf);
                free(obuf);
                return -1;
            }

            crc = memcmp(ibuf, obuf, blocksize);
            if (crc != 0)
            {
                completed = length;
                break; // this code never worked prior to this addition, would read till EOF and fail.
            }

            completed += blocksize;
        }

        if (!p_src->close())
        {
            sprintf(errormsg, "Failed to close src file: %s", src_file->path);
            errsend(NONFATAL, errormsg);
            free(ibuf);
            free(obuf);
            return -1;
        }

        if (!p_dest->close())
        {
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
int update_stats(PathPtr p_src,
                 PathPtr p_dst,
                 struct options &o)
{

    int rc;
    char errormsg[MESSAGESIZE] = {0};
    int mode;

    // don't touch the destination, unless this is a COPY
    if (o.work_type != COPYWORK)
        return 0;

    // Make a path_item matching <dest_file>, using <src_file>->dest_ftype
    // NOTE: Path::follow() is false, by default
    //path_item dest_copy(p_dst->node());
    //dest_copy.ftype = p_src->dest_ftype();
    //PathPtr p_dest(PathFactory::create_shallow(&dest_copy));
    PathPtr p_dest = p_dst;

    // if running as root, always update <dest_file> owner  (without following links)
    // non-root user can also attempt this, by setting "preserve" (with -o)
    if (0 == geteuid())
    {
        if (!p_dest->lchown(p_src->st().st_uid, p_src->st().st_gid))
        {
            errsend_fmt(NONFATAL, "update_stats -- Failed to chown %s: %s\n",
                        p_dest->path(), p_dest->strerror());
        }
    }
    else if (o.preserve)
    {
        if (!p_dest->lchown(geteuid(), p_src->st().st_gid))
        {
            errsend_fmt(NONFATAL, "update_stats -- Failed to set group ownership %s: %s\n",
                        p_dest->path(), p_dest->strerror());
        }
    }

    // ignore symlink destinations
    if (p_src->is_link())
        return 0;

    // perform any final adjustments on destination, before we set atime/mtime
    if ( p_dest->post_process(p_src) != true ) {
        errsend_fmt(NONFATAL, "Failed to finalize destination file %s: %s\n",
                    p_dest->path(), p_dest->strerror());
        return -1; // DO NOT update any other stats if this step fails
    }

    // update <dest_file> access-permissions
    // always add user write
    mode = (p_src->mode() & 07777) | S_IWUSR;
    if (!p_dest->chmod(mode))
    {
        errsend_fmt(NONFATAL, "update_stats -- Failed to chmod fuse chunked file %s: %s\n",
                    p_dest->path(), p_dest->strerror());
    }

    // update <dest_file> atime and mtime
    struct timespec times[2];

    times[0].tv_sec = p_src->st().st_atim.tv_sec;
    times[0].tv_nsec = p_src->st().st_atim.tv_nsec;

    times[1].tv_sec = p_src->st().st_mtim.tv_sec;
    times[1].tv_nsec = p_src->st().st_mtim.tv_nsec;

    if (!p_dest->utimensat(times, AT_SYMLINK_NOFOLLOW))
    {
        errsend_fmt(NONFATAL, "update_stats -- Failed to change atime/mtime %s: %s\n",
                    p_dest->path(), p_dest->strerror());
    }
#ifdef TMPFILE
    if (!p_src->get_packable() && p_src->st().st_size > o.chunk_at)
    {
        const char *plus_sign = strrchr((const char *)p_dest->path(), '+');
        if (plus_sign)
        {
            size_t p_dest_orig_len = plus_sign - p_dest->path();
            PathPtr p_dest_orig(p_dest->path_truncate(p_dest_orig_len));

            if (!p_dest->rename(p_dest_orig->path()))
            {
                errsend_fmt(FATAL, "update_stats -- Failed to rename %s "
                                   "to original file path %s\n",
                            p_dest->path(), p_dest_orig->path());
            }
            else if (o.verbose >= 1)
            {
                output_fmt(0, "INFO  DATACOPY Renamed temp-file %s to %s\n",
                           p_dest->path(), p_dest_orig->path());
            }

            p_dest = p_dest_orig;
        }
    }
#endif
    return 0;
}

//local functions only
int request_response(int type_cmd)
{
    MPI_Status status;
    int response;
    send_command(MANAGER_PROC, type_cmd, MPI_TAG_NOT_MORE_WORK);
    if (MPI_Recv(&response, 1, MPI_INT, MANAGER_PROC, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS)
    {
        errsend(FATAL, "Failed to receive response\n");
    }
    return response;
}

int request_input_queuesize()
{
    return request_response(QUEUESIZECMD);
}

void send_command(int target_rank, int type_cmd, int mpi_tag)
{
#ifdef MPI_DEBUG
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    PRINT_MPI_DEBUG("rank %d: Sending command %d to target rank %d\n", rank, type_cmd, target_rank);
#endif

    // Send a simple CMD (without args) to a rank
    if (MPI_Send(&type_cmd, 1, MPI_INT, target_rank, mpi_tag, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send command %d to rank %d\n", type_cmd, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_path_buffer(int target_rank, int command, path_item *buffer, int *buffer_count)
{
    int i;
    int position = 0;
    int worksize;
    char *workbuf;
    path_item work_node;
    path_item *work_node_ptr; /* avoid unnecessary copying */

    worksize = *buffer_count * sizeof(path_item);
    workbuf = (char *)malloc(worksize * sizeof(char));
    if (!workbuf)
    {
        fprintf(stderr, "Failed to allocate %lu bytes for workbuf\n", worksize);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    for (i = 0; i < *buffer_count; i++)
    {
        work_node_ptr = &buffer[i];
        MPI_Pack(work_node_ptr, sizeof(path_item), MPI_CHAR, workbuf, worksize, &position, MPI_COMM_WORLD);
    }
    send_command(target_rank, command, MPI_TAG_MORE_WORK);
    if (MPI_Send(buffer_count, 1, MPI_INT, target_rank, MPI_TAG_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send buffer_count %d to rank %d\n", *buffer_count, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(workbuf, worksize, MPI_PACKED, target_rank, MPI_TAG_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send workbuf to rank %d\n", target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    *buffer_count = 0;
    free(workbuf);
}

void send_buffer_list(int target_rank, int command, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    int size = (*workbuflist)->size;
    int worksize = sizeof(path_item) * size;
    send_command(target_rank, command, MPI_TAG_NOT_MORE_WORK);
    if (MPI_Send(&size, 1, MPI_INT, target_rank, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send workbuflist size %d to rank %d\n", size, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send((*workbuflist)->buf, worksize, MPI_PACKED, target_rank, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send workbuflist buf to rank %d\n", target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    dequeue_buf_list(workbuflist, workbuftail, workbufsize);
}

//manager
void send_manager_nonfatal_inc()
{
    send_command(MANAGER_PROC, NONFATALINCCMD, MPI_TAG_NOT_MORE_WORK);
}

void send_manager_chunk_busy()
{
    send_command(MANAGER_PROC, CHUNKBUSYCMD, MPI_TAG_NOT_MORE_WORK);
}

void send_manager_copy_stats(int num_copied_files, size_t num_copied_bytes)
{
    send_command(MANAGER_PROC, COPYSTATSCMD, MPI_TAG_NOT_MORE_WORK);
    //send the # of paths
    if (MPI_Send(&num_copied_files, 1, MPI_INT, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_copied_files %d to rank %d\n", num_copied_files, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    //send the # of paths
    if (MPI_Send(&num_copied_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_copied_byes %zd to rank %d\n", num_copied_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_manager_examined_stats(int num_examined_files, size_t num_examined_bytes, int num_examined_dirs, size_t num_finished_bytes)
{
    send_command(MANAGER_PROC, EXAMINEDSTATSCMD, MPI_TAG_NOT_MORE_WORK);
    //send the # of paths
    if (MPI_Send(&num_examined_files, 1, MPI_INT, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_examined_files %d to rank %d\n", num_examined_files, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_examined_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_examined_bytes %zd to rank %d\n", num_examined_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_examined_dirs, 1, MPI_INT, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_examined_dirs %d to rank %d\n", num_examined_dirs, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(&num_finished_bytes, 1, MPI_DOUBLE, MANAGER_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to send num_finished_bytes %zd to rank %d\n", num_finished_bytes, MANAGER_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_manager_regs_buffer(path_item *buffer, int *buffer_count)
{
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, PROCESSCMD, buffer, buffer_count);
}

void send_manager_dirs_buffer(path_item *buffer, int *buffer_count)
{
    //sends a chunk of regular files to the manager
    send_path_buffer(MANAGER_PROC, DIRCMD, buffer, buffer_count);
}

void send_manager_new_buffer(path_item *buffer, int *buffer_count)
{
    //send manager new inputs
    send_path_buffer(MANAGER_PROC, INPUTCMD, buffer, buffer_count);
}

void send_manager_work_done(int ignored)
{
    //the worker is finished processing, notify the manager
    send_command(MANAGER_PROC, WORKDONECMD, MPI_TAG_NOT_MORE_WORK);
}

//worker
void update_chunk(path_item *buffer, int *buffer_count)
{
    send_path_buffer(ACCUM_PROC, UPDCHUNKCMD, buffer, buffer_count);
}

void write_output(const char *message, int log)
{
    //write a single line using the outputproc
    //set the command type
    if (log == 0)
    {
        send_command(OUTPUT_PROC, OUTCMD, MPI_TAG_NOT_MORE_WORK);
    }
    else if (log == 1)
    {
        send_command(OUTPUT_PROC, LOGCMD, MPI_TAG_NOT_MORE_WORK);
    }
    else if (log == 2)
    {
        send_command(OUTPUT_PROC, LOGONLYCMD, MPI_TAG_NOT_MORE_WORK);
    }

    //send the message
    if (MPI_Send((void *)message, MESSAGESIZE, MPI_CHAR, OUTPUT_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to message to rank %d\n", OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

// This allows caller to use inline formatting, without first snprintf() to
// a local errmsg-buffer.  Like so:
//
//    output_fmt(1, "rank %d hello!", rank);
//
void output_fmt(int log, const char *format, ...)
{
    char msg[MESSAGESIZE] = {0};
    va_list args;

    va_start(args, format);
    vsnprintf(msg, MESSAGESIZE, format, args);
    va_end(args);

    // msg[MESSAGESIZE -1] = 0;  /* no need for this */
    write_output(msg, log);
}

void write_buffer_output(char *buffer, int buffer_size, int buffer_count)
{

    //write a buffer to the output proc
    //set the command type
    send_command(OUTPUT_PROC, BUFFEROUTCMD, MPI_TAG_NOT_MORE_WORK);

    //send the size of the buffer
    if (MPI_Send(&buffer_count, 1, MPI_INT, OUTPUT_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to buffer_count %d to rank %d\n", buffer_count, OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (MPI_Send(buffer, buffer_size, MPI_PACKED, OUTPUT_PROC, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to message to rank %d\n", OUTPUT_PROC);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_worker_queue_count(int target_rank, int queue_count)
{
    if (MPI_Send(&queue_count, 1, MPI_INT, target_rank, MPI_TAG_NOT_MORE_WORK, MPI_COMM_WORLD) != MPI_SUCCESS)
    {
        fprintf(stderr, "Failed to queue_count %d to rank %d\n", queue_count, target_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

void send_worker_readdir(int target_rank, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    //send a worker a buffer list of paths to stat
    send_buffer_list(target_rank, DIRCMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_copy_path(int target_rank, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    //send a worker a list buffers with paths to copy
    send_buffer_list(target_rank, COPYCMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_compare_path(int target_rank, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    //send a worker a list buffers with paths to compare
    send_buffer_list(target_rank, COMPARECMD, workbuflist, workbuftail, workbufsize);
}

void send_worker_exit(int target_rank)
{
    //order a rank to exit
    send_command(target_rank, EXITCMD, MPI_TAG_NOT_MORE_WORK);
}

static void errsend_internal(Lethality fatal, const char *errormsg)
{

    write_output(errormsg, 1);

    if (fatal)
    {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else
    {
        send_manager_nonfatal_inc();
    }
}

//functions that workers use
void errsend(Lethality fatal, const char *error_text)
{
    //send an error message to the outputproc. Die if fatal.
    char errormsg[MESSAGESIZE] = {0};

#ifdef CONDUIT
    // possibly send a CONDUIT message, as well
    snprintf( errormsg, MESSAGESIZE,
              "#CONDUIT-MSG {\"Type\":\"ERROR\", \"Class\":\"%s\", \"Origin\":\"%s\", \"Errno\":%d, \"Message\":\"%s\"}",
              (fatal) ? "FATAL" : "NONFATAL", "Unknown", errno, error_text );
    write_output(errormsg, 1);
#endif

    if (fatal)
        snprintf(errormsg, MESSAGESIZE, "ERROR FATAL: %s\n", error_text);
    else
        snprintf(errormsg, MESSAGESIZE, "ERROR NONFATAL: %s\n", error_text);

    errsend_internal(fatal, errormsg);
}

// This allows caller to use inline formatting, without first snprintf() to
// a local errmsg-buffer.  Like so:
//
//    errsend_fmt(NONFATAL, "rank %d hello!", rank);
//
void errsend_fmt(Lethality fatal, const char *format, ...)
{
    char errormsg[MESSAGESIZE] = {0};
    va_list args;

#ifdef CONDUIT
    // possibly send a CONDUIT message, as well
    va_list tmpargs;
    va_start( tmpargs, format );

    // format message header
    size_t tmpoffset = snprintf( errormsg, MESSAGESIZE,
              "#CONDUIT-MSG {\"Type\":\"ERROR\", \"Class\":\"%s\", \"Origin\":\"%s\", \"Errno\":%d, \"Message\":\"",
              (fatal) ? "FATAL" : "NONFATAL", "Unknown", errno );
    // format proc message
    size_t messagelen = vsnprintf(errormsg + tmpoffset, MESSAGESIZE - tmpoffset, format, tmpargs);
    // check for buffer overflow
    if ( (tmpoffset + messagelen) >= MESSAGESIZE ) {
        // if the message is too long, just omit it
        snprintf( errormsg + tmpoffset, MESSAGESIZE - tmpoffset, "\"}\n" );
    }
    else {
        // remove assumed newline char ( this is why we need an explicit overflow check, above )
        *(errormsg + tmpoffset + messagelen - 1) = '\0';
        // append message end
        snprintf( errormsg + tmpoffset + messagelen - 1, MESSAGESIZE - tmpoffset - messagelen + 1, "\"}\n" );
    }
    write_output(errormsg, 1);
#endif

    snprintf(errormsg, MESSAGESIZE, "ERROR %sFATAL: ", (fatal ? "" : "NON"));
    size_t offset = strlen(errormsg);

    va_start(args, format);
    vsnprintf(errormsg + offset, MESSAGESIZE - offset, format, args);
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
int stat_item(path_item *work_node, struct options &o)
{
    char errmsg[MESSAGESIZE] = {0};
    struct stat st = {0};
    int rc;
    int numchars;
    char linkname[PATHSIZE_PLUS] = {0};

    // defaults
    work_node->ftype = REGULARFILE;
    work_node->dest_ftype = REGULARFILE;

    bool got_type = false;

#ifdef MARFS
    if (!got_type)
    {
        // identify if the path is below the MarFS mountpoint
        rc = MARFS_Path::lstat(work_node->path, &st);
        if (rc == 0  ||  errno != EINVAL)
        {
            // only failure w/ EINVAL indicates an invalid path
            work_node->ftype = MARFSFILE;
            got_type = true;
        }
    }
#endif

#ifdef GEN_SYNDATA
    // --- is it a Synthetic Data path?
    if (!got_type)
    {
        if (o.syn_size && isSyndataPath(work_node->path))
        {
            int dlvl; // directory level, if a directory. Currently ignored.
            if (rc = syndataSetAttr(work_node->path, &st, &dlvl, o.syn_size))
                return -1; // syndataSetAttr() returns non-zero on failure
            work_node->ftype = SYNDATA;
            got_type = true;
        }
    }
#endif

    // --- is it '/dev/null' or '/dev/null/[...]'?
    if (!got_type)
    {
        if ((!strncmp(work_node->path, "/dev/null", 9)))
        {
            if (work_node->path[9] == 0)
            {
                work_node->ftype = NULLFILE;
                got_type = true;

                rc = lstat("/dev/null", &st);
            }
            else if (work_node->path[9] == '/')
            {
                size_t len = strlen(work_node->path);
                if (work_node->path[len - 1] == '/')
                {
                    work_node->ftype = NULLDIR;
                    got_type = true;

                    char *homedir = getenv("HOME");
                    rc = lstat(homedir, &st);
                }
                else
                {
                    work_node->ftype = NULLFILE;
                    got_type = true;

                    rc = lstat("/dev/null", &st);
                }
            }
        }
    }

    // --- is it a POSIX path?
    if (!got_type)
    {
        rc = lstat(work_node->path, &st); // TODO: in posix path it checks to see if it should follow links
        if (rc == 0)
        {
            work_node->ftype = REGULARFILE;
            got_type = true;
        }
        else
            return -1;
    }

    // only update our stat struct if the stat was successful
    if ( rc == 0 )
        work_node->st = st;
    return rc;
}

// <fs> is actually a SrcDstFSType.  If you have <sys/vfs.h>, then initialize
// <fs> to match the type of <path>.  Otherwise, call it ANYFS.
//
// QUESTION: Are we using fprintf() + MPI_Abort(), instead of errsend_fmt()
//    because of OUTPUT_PROC might not be available, or is this just
//    from before errsend_fmt() was available?

void get_stat_fs_info(const char *path, SrcDstFSType *fs)
{

#ifdef HAVE_SYS_VFS_H
    struct stat st;
    struct statfs stfs;
    char errortext[MESSAGESIZE] = {0};
    int rc;
    char use_path[PATHSIZE_PLUS] = {0};

    strncpy(use_path, path, PATHSIZE_PLUS);
    if (use_path[PATHSIZE_PLUS - 1])
    { // strncpy() is unsafe
        fprintf(stderr, "Oversize path '%s'\n", path);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    // look at <path>, or, if that fails, look at dirname(<path>)
    PathPtr p(PathFactory::create(use_path));
    if (!p)
    {
        fprintf(stderr, "PathFactory couldn't interpret path %s\n", use_path);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    else if (!p->stat())
    {
        char *use_path_copy = strdup(use_path); // dirname() may alter arg
        strncpy(use_path, dirname(use_path_copy), PATHSIZE_PLUS);
        free(use_path_copy);

        p = PathFactory::create(use_path);
        if (!p)
        {
            fprintf(stderr, "PathFactory couldn't interpret parent-path %s\n", use_path);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        else if (!p->stat())
        {
            fprintf(stderr, "Failed to stat path %s, or parent %s\n", path, use_path);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }
    st = p->st();

    // if the thing we're looking at isn't link, maybe run statfs() on it.
    if (!S_ISLNK(st.st_mode))
    {
        if ((p->node().ftype == NULLFILE) || (p->node().ftype == NULLDIR))
        {
            *fs = NULLFS;
            return;
        }
        else if (p->node().ftype == SYNDATA)
        {
            *fs = SYNDATAFS;
            return;
        }
        else if (p->node().ftype == PLFSFILE)
        {
            *fs = PLFSFS; // NOTE: less than PARALLEL_DESTFS
            return;
        }
        else if (p->node().ftype == MARFSFILE)
        {
            *fs = MARFSFS;
            return;
        }

        rc = statfs(use_path, &stfs);
        if (rc < 0)
        {
            snprintf(errortext, MESSAGESIZE, "Failed to statfs path %s", path);
            errsend(FATAL, errortext);
        }
        else if (stfs.f_type == GPFS_FILE)
        {
            *fs = GPFSFS;
        }
        else if (stfs.f_type == PANFS_FILE)
        {
            *fs = PANASASFS;
        }
        else
        {
            *fs = ANYFS; // NOTE: less than PARALLEL_DESTFS
        }
    }
    else
    {
        //symlink assumed to be GPFS
        *fs = GPFSFS;
    }

#else
    *fs = ANYFS; // NOTE: less than PARALLEL_DESTFS
#endif
}

// NOTE: master spending ~50% time in this function.
// all callers provide <start_range>==START_PROC
int get_free_rank(struct worker_proc_status *proc_status, int start_range, int end_range)
{
    //given an inclusive range, return the first encountered free rank
    int i;
    for (i = start_range; i <= end_range; i++)
    {
        if (proc_status[i].inuse == 0)
        {
            return i;
        }
    }
    return -1;
}

//are all the ranks free?
// return 1 for yes, 0 for no.
int processing_complete(struct worker_proc_status *proc_status, int free_worker_count, int nproc)
{
    if (free_worker_count == (nproc - START_PROC))
    {
        int i;
        for (i = 0; i < START_PROC; i++)
        {
            if (proc_status[i].inuse)
                return 0;
        }
        return 1;
    }
    return 0;
}

//Queue Function Definitions

// push path onto the tail of the queue
void enqueue_path(path_list **head, path_list **tail, char *path, int *count)
{
    path_list *new_node = (path_list *)malloc(sizeof(path_list));
    memset(new_node, 0, sizeof(path_list));
    if (!new_node)
    {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for new_node\n", sizeof(path_list));
    }

    strncpy(new_node->data.path, path, PATHSIZE_PLUS);
    if (new_node->data.path[PATHSIZE_PLUS - 1])
    { // strncpy() is unsafe
        errsend_fmt(FATAL, "enqueue_path: Oversize path '%s'\n", path);
    }

    new_node->data.start = 1;
    new_node->data.ftype = TBD;
    new_node->next = NULL;
    if (*head == NULL)
    {
        *head = new_node;
        *tail = *head;
    }
    else
    {
        (*tail)->next = new_node;
        *tail = (*tail)->next;
    }
    *count += 1;
}

void print_queue_path(path_list *head)
{
    //print the entire queue
    while (head != NULL)
    {
        printf("%s\n", head->data.path);
        head = head->next;
    }
}

void delete_queue_path(path_list **head, int *count)
{
    //delete the entire queue;
    path_list *temp = *head;
    while (temp)
    {
        *head = (*head)->next;
        free(temp);
        temp = *head;
    }
    *count = 0;
}

// enqueue a node using an existing node (does a new allocate, but allows
// us to pass nodes instead of paths)
void enqueue_node(path_list **head, path_list **tail, path_list *new_node, int *count)
{
    path_list *temp_node = (path_list *)malloc(sizeof(path_list));
    memset(temp_node, 0, sizeof(path_list));
    if (!temp_node)
    {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for temp_node\n", sizeof(path_list));
    }
    temp_node->data = new_node->data;
    temp_node->next = NULL;
    if (*head == NULL)
    {
        *head = temp_node;
        *tail = *head;
    }
    else
    {
        (*tail)->next = temp_node;
        *tail = (*tail)->next;
    }
    *count += 1;
}

void dequeue_node(path_list **head, path_list **tail, int *count)
{
    //remove a path from the front of the queue
    path_list *temp_node = *head;
    if (temp_node == NULL)
    {
        return;
    }
    *head = temp_node->next;
    free(temp_node);
    *count -= 1;
}

void enqueue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize, char *buffer, int buffer_size)
{

    work_buf_list *new_buf_item = (work_buf_list *)malloc(sizeof(work_buf_list));
    memset(new_buf_item, 0, sizeof(work_buf_list));
    if (!new_buf_item)
    {
        fprintf(stderr, "Failed to allocate %lu bytes for new_buf_item\n", sizeof(work_buf_list));
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    new_buf_item->buf = buffer;
    new_buf_item->size = buffer_size;
    new_buf_item->next = NULL;

    if (*workbufsize < 0)
    {
        *workbufsize = 0;
    }

    if (*workbuflist == NULL)
    {
        *workbuflist = new_buf_item;
        *workbuftail = new_buf_item;
        *workbufsize = 1;
        return;
    }

    (*workbuftail)->next = new_buf_item;
    *workbuftail = new_buf_item;
    (*workbufsize)++;
}

void dequeue_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
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

void delete_buf_list(work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    while (*workbuflist)
    {
        dequeue_buf_list(workbuflist, workbuftail, workbufsize);
    }
    *workbufsize = 0;
}

void pack_list(path_list *head, int count, work_buf_list **workbuflist, work_buf_list **workbuftail, int *workbufsize)
{
    int position;
    char *buffer;
    int buffer_size = 0;
    int worksize;
    path_list *iter;

    worksize = MESSAGEBUFFER * sizeof(path_item);
    buffer = (char *)calloc(worksize, sizeof(char));
    if (!buffer)
    {
        fprintf(stderr, "Failed to allocate %lu bytes for buffer\n", sizeof(worksize));
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    position = 0;

    for (iter = head; iter != NULL; iter = iter->next)
    {
        MPI_Pack(&iter->data, sizeof(path_item), MPI_CHAR, buffer, worksize, &position, MPI_COMM_WORLD);
        buffer_size++;
        if (buffer_size % STATBUFFER == 0 || buffer_size % MESSAGEBUFFER == 0)
        {
            enqueue_buf_list(workbuflist, workbuftail, workbufsize, buffer, buffer_size);
            buffer_size = 0;
            buffer = (char *)malloc(worksize);
            if (!buffer)
            {
                fprintf(stderr, "Failed to allocate %lu bytes for buffer-elt\n", sizeof(worksize));
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
            position = 0; // should this be here?
        }
    }
    if (buffer_size != 0)
    {
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
 * In order to avoid a redundant stat of the CTM file, we now have the
 * caller pass that information in through <dst_has_ctm>, because the
 * caller now has that information already, by other means.
 *
 * @param src           path_item structure containing
 *                         the metadata for the souce file
 * @param dst           path_item structure containing
 *                         the metadata for the destination file
 * @param o             the PFTOOL global options structure
 * @param dst_has_ctm   COPYWORK destination has CTM?
 *                      -1=unsure, 0=no, 1=yes
 *
 * @return 1 (TRUE) if the files are "the same", or 0 otherwise.  If the
 *   command-line includes an option to skip copying files that have
 *   already been done (-n), then the copy will be skipped if files are
 *   "the same".
 */

// TBD: Use Path methods to avoid over-reliance on POSIX same-ness
int samefile(PathPtr p_src, PathPtr p_dst, const struct options &o, int dst_has_ctm)
{
    const path_item &src = p_src->node();
    const path_item &dst = p_dst->node();

    // compare metadata - check size, mtime, mode, and owners
    // (satisfied conditions -> "same" file)

    if (src.st.st_size == dst.st.st_size && (src.st.st_mtime == dst.st.st_mtime || S_ISLNK(src.st.st_mode))
        // only chown if preserve set
        && (((src.st.st_uid == dst.st.st_uid) && (src.st.st_gid == dst.st.st_gid)) || !o.preserve)
    )
    {
        // class-specific techniques (e.g. MarFS file has RESTART?)
        if (p_dst->incomplete())
            return 0;

        // Files are the "same", as far as MD is concerned.
        return 1;
    }

    return 0;
}

// stolen from MarFS epoch_to_str(), for convenience
#define fka_MARFS_DATE_FORMAT "%Y%m%d_%H%M%S%z"
#define fka_MARFS_DST_FORMAT "_%d"
int epoch_to_string(char *str, size_t size, const time_t *time)
{
    struct tm tm;

    // time_t -> struct tm
    if (!localtime_r(time, &tm))
    {
        fprintf(stderr, "localtime_r failed: %s\n", strerror(errno));
        return -errno;
    }

    size_t strf_size = strftime(str, size, fka_MARFS_DATE_FORMAT, &tm);
    if (!strf_size)
    {
        fprintf(stderr, "strftime failed even more than usual: %s\n", strerror(errno));
        return -errno;
    }

    // add DST indicator
    snprintf(str + strf_size, size - strf_size, fka_MARFS_DST_FORMAT, tm.tm_isdst);

    return 0;
}

// <p_src>    is the (unaltered) source
// <out_node> is the (unaltered) destination
//
// see comments at check_ctm_match(), in ctm.c

#define DATE_STRING_MAX 64
int check_temporary(PathPtr p_src, path_item *out_node)
{
    time_t src_mtime = p_src->mtime();
    char src_mtime_str[DATE_STRING_MAX] = {0};
    char src_to_hash[PATHSIZE_PLUS] = {0}; // p_src->path() + mtime string

    int rc = epoch_to_string(src_mtime_str, DATE_STRING_MAX, &src_mtime);
    if (rc)
        return rc;

    snprintf(src_to_hash, PATHSIZE_PLUS, "%s+%s", p_src->path(), src_mtime_str);
    src_to_hash[PATHSIZE_PLUS - 1] = 0; /* no need for this */

    return check_ctm_match(src_to_hash, out_node->path);
}
