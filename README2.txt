Operation of pftool

This is an extra README file, written by someone new to the code, so that
my efforts figuring it out can benefit others.  This readme also explains
my efforts to make the code simpler and more flexible, including an
extension from C to C++.


pftool runs as an MPI job requiring at least 4 ranks.  The first three
ranks have specific functions, and the rest are workers.

--- ROLES


Rank 0: manager

     This process collects paths from the workers and assigns work to other
     wrokers that aren't busy, in a round-robin order.

Rank 1: output

     All output to the console is sent to this rank.  This avoids
     interleaved output from parallel prints.

Rank 2: start

     The start rank's only job is to initiate activity by receiving an
     initial request to read the top-level source-directory, provided by
     the user on the command-line.

Rank >= 3:

      All ranks with id above 2 are "workers".  They wait for messages from
      the manager and perform corresponding work.




--- MESSAGES


Each communication, in either direction, between workers and the manager is
actually a series of elements.  The first element is an integer
command-code.  This represents a request from the manager, or a response
from a worker.  For example, the manager uses DIRCMD to request a worker to
read a directory.  (Requests are always made to specific workers.)

The receiver of a command uses the code to select an action, which is
typically implemented by a function in pfutils.c.  The functions for
manager actions are called "send_worker_<something>", and the functions for
worker actions are called "send_manager_<something>".

The command is typically followed by arguments.  For example, after the
DIRCMD, the manager sends a single path, which the worker processes as a
directory to be listed.  [Somewhat confusing because the mgr appears to
send a list.]



--- BUILDING

(a) Having the plfs (2.5) modules loaded prevents configure from working.
However, you can't compile without it.  [Maybe this is only if you use
'--enable-plfs'?]

[UPDATE: I think this is fixed now.  It seems much simpler to use the plfs
module to define PLFS_CFLAGS and PLFS_LDFLAGS, so do that.]

    module use /usr/projects/plfs/rrz/modulefiles
    module load my-openmpi-gnu/1.6.5
    module load my-plfs/2.5     [if you used '--enable-plfs']

    echo PLFS_CFLAGS
    -->  -I/usr/projects/plfs/rrz/plfs/plfs-2.5-install/include

    echo PLFS_LDFLAGS
    -->  -L/usr/projects/plfs/rrz/plfs/plfs-2.5-install/lib -Wl,-rpath=/usr/projects/plfs/rrz/plfs/plfs-2.5-install/lib -Wl,--whole-archive -lplfs -Wl,--no-whole-archive
     


(b) S3 requires the 'aws4c' library (version 0.5.3), plus libxml2.  You can
get aws4c from github.

    git clone https://github.com/jti-lanl/aws4c.git
    cd aws4c
    git checkout lanl     [switch to LANL development branch]
    make

    # You also need to create ~/.awsAuth This file contains entries
    # something like the following, where you would replace "jti" with your
    # moniker, and "xxxx" with your 40(?) char private user-id, given to
    # you by the S3 system administrator

    jti:jti@ccstar.lanl.gov:xxxx


    # If you want to use s3curl.pl, you'll use the same 40-digit key in
    # the ~/.s3curl file.


(c) Once you have installed and built aws4c, you have to communicate where
it is to configure, using environment variables S3_CFLAGS and S3_LDFLAGS.


# DO THIS (supposing you want both PLFS and S3):

    module use /usr/projects/plfs/rrz/modulefiles
    module load my-openmpi-gnu/1.6.5
    module load my-plfs/2.5

    # these paths are where I installed the aws4c lib, plus where libxml2 is
    export S3_CFLAGS="-I/users/jti/projects/ecs_hobo/tools/aws4c -I/usr/include/libxml2"
    export S3_LDFLAGS="-L/users/jti/projects/ecs_hobo/tools/aws4c -L/usr/lib64"

    ./autogen

     ( export CPPFLAGS="${S3_CFLAGS} ${PLFS_CFLAGS}"; \
       export LDFLAGS="${S3_LDFLAGS} ${PLFS_LDFLAGS}"; \
       ./configure --prefix=`pwd`/installed --enable-plfs --enable-s3 )


    # for debugging, you could add this:
    ( ... export CFLAGS=-g; ./configure ... )


    # you can add "V=1" if you want to see the compile lines,
    make  [V=1]

    make install




--- removing unnecessary #ifdefs

There were many places where high-level code had unnecessary
conditional-compilation, based on configuration options.  For example, in
pfutils.c, get_fs_stat_info() includes the following snippet of code:


    if (!S_ISLNK(st.st_mode)) {
        if (statfs(use_path, &stfs) < 0) {
            snprintf(errortext, MESSAGESIZE, "Failed to statfs path %s", path);
            errsend(FATAL, errortext);
        }
        if (stfs.f_type == GPFS_FILE) {
            *fs = GPFSFS;
        }
        else if (stfs.f_type == PANFS_FILE) {
            *fs = PANASASFS;
        }
#ifdef FUSE_CHUNKER
        else if (stfs.f_type == FUSE_SUPER_MAGIC) {
            //fuse file
            *fs = GPFSFS;
        }
#endif
        else {
            *fs = ANYFS;
        }
    }


It's not really necessary to make the FUSE_CHUNKER code conditional.  The
variable FUSE_SUPER_MAGIC could always be defined, and there is no
compelling reason for this code to avoid testing for it.  If you're not
configured to use the FUSE-chunker, it's a wasted test, but, sheesh, it's
more useful to have the code be legible.

DESIGN QUESTION: is it really so illegible like this?  It does allow you to
eliminate everything to do with the FUSE-chunker in one fell-swoop.  That
could be nice if you are testing and want to be able to back everythying
out.

ANSWER: This isn't the woprst example.  Some code is actually much more
complicated and error prone, because it makes heroic attempts to leave old
code untouched, while inserting new special sub-cases into complex tests.
I think it has reached the breaking point.  Time to switch to an
object-oriented method.  When you add a new protocol, you'll be adding
subclasses that override some methods and leave others intact.





--- CONVERSION TO C++

There are several reasons to do this:

(1) The number of interleaved conditional-compilation sections (e.g. #ifdef
    PLFS) are already numerous, but we now want to add a new protocol
    (Amazon S3) that is likely different enough from the existing methods
    that it will cause considerable confusion.

(2) The code is difficult to follow as is.  Some of the conditional
    compilations could be reduced to run-time tests of flags, which execute
    isolated sections of conditionally-compiled code.

(3) For maintainability, (2) is most easily accomplished as a hierarchy of
    classes.

(4) Maintaining custom linked-lists by passing e.g. head, tail, and length
    through a hierarchy of functions is cumbersome.  Its also not easy to
    tell whether a function that processes lists is supposed to take only
    the first one, or the whole thing.  perhaps these things can be cleared
    up by moving to STL data-structures, removing the need for custom
    push/pop/delete/enqueue/dequeue functions.

(5) It seems we can also get rid of the need for multiple lists to hold
    different kinds of paths (e.g. dir_buf_list vs tape_buf_list), because
    the lists of Path objects will know what kind of object they represent,
    and will be respond appropriately to generic method-calls.

(6) The use of lstat() or plfs_stat() at top-level means any new protocol
    must be able to approximately fake POSIX stat semantics (like plfs
    does).  That's unrealistic for S3-based object-filesystems.  Instead,
    all the status stuff should be hidden behind high-level abstractions
    ("is this thing newer than that thing?").  Otherwise, we end up having
    to write a whole POSIX-simulation layer (like plfs_stat()) for every
    new protocol, with more conditional code for each case.


It looks to me like one likely place to start toward modularization is by
moving intellgence into path objects.  These can be cognizant of the types
of systems used for source and destination, and can make appropriate
adjustments.  With N different types of source and destination file-system,
we only need N types of objects, instead of N^2 conditions.

This is also the proper granularity for knowing whether we want to get
meta-data via lstat(), plfs_stat(), or a POST to the bucket-name.  The
paths can know how to do other file-system-specific things that need doing,
like creating new paths, directory-listing, reading, writing, etc.

This is similar to the C-based function-pointer struct used in IOR, but C++
classes buy us better flexibility: if someone needs to add a new method for
their file system, it may be possible to provide a default in the
base-classes, instead of requiring every module to be updated.


--- removing custom linked-lists, etc, in favor of STL data-structures

This reduces the amount of code that has to be maintained (e.g. code to
add/delete/push/pop/etc), and simplifies function-signatures, by allowing
us to pass the data-structure, instead of passing pointers to head, tail,
and count).  Of course, we could just make a struct, and pass that.




---------------------------------------------------------------------------
PROBLEMS:

-- If the source is a symlink, pftool creates a corresponding link at the
   destination.  This is a problem if the destination doesn't support
   symlinks.  For example, no symlinks in S3. We could:

   (a) Follow the links and copy the files underneath to the name of the link
   on the destination.  This means a large file could get copied twice, once
   to the name of the link, and once as the name of the file that was linked.
   It also means that copying A -> B -> A is not the same as A.

   (b) Ignore symlinks on the source

   (c) Provide a command-line option to choose between (a) or (b).

   (d) Create our own custom "link" meta-data (e.g. "this object is actually a
   link to the object named MyFile").  This would allow us to restore
   symlinks, if we later copy the objects to somewhere that supports them. So,
   A -> B -> A would work.  But nobody will be able to follow the links on the
   object-side, unless we provide browsing tools that know about this symlink
   hackery.

   (e) depend on vendors that provide access both as objects and files, and
   create the symlinks on the file-side.  (Not sure if that's even possible.
   I think Scality requires, in some cases, that you can store as objects or
   files, but not both.)





---------------------------------------------------------------------------
TBD:

-- convert Path::_path to STL string.

   This would save a lot of strduping, and C manipulations.  Careful,
   though, this could also add some overhead.  Might save some time, as
   well, because everybody won't have to be repeatedly calling strlen().


