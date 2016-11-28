AC_PREREQ([2.59])

AC_INIT([pftool], [2.1.3], [dsherril@lanl.gov])


# Check If AM_SILENT_RULES Rule Are Supported
# If So, Build Using AM_SILENT_RULES
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([src])

# note: -Wall -Werror are automake options.  they are NOT compiler flags
AM_INIT_AUTOMAKE([-Wall -Werror foreign])


# lets add some options to our CFLAGS variable
AC_ARG_ENABLE(threads, [  --enable-threads]  [Run in threading mode])
AM_CONDITIONAL([THREADS], [test x$enable_threads = xyes])

AC_ARG_ENABLE(tape, [  --enable-tape]  [enable tape support])
AM_CONDITIONAL([TAPE], [test x$enable_tape = xyes])

AC_ARG_ENABLE(fusechunker, [  --enable-fusechunker]  [enable fusechunker support])
AM_CONDITIONAL([FUSECHUNKER], [test x$enable_fusechunker = xyes])

AC_ARG_ENABLE(hpss, [  --enable-hpss]  [enable hpss support])
AM_CONDITIONAL([HPSS], [test x$enable_hpss = xyes])

AC_ARG_ENABLE(syndata, [  --enable-syndata]  [enable synthetic data generation support])
AM_CONDITIONAL([SYNDATA], [test x$enable_syndata = xyes])

AC_ARG_ENABLE(plfs, [  --enable-plfs]  [enable plfs support])
AM_CONDITIONAL([PLFS], [test x$enable_plfs = xyes])

AC_ARG_ENABLE(s3, [  --enable-s3]  [enable S3 support])
AM_CONDITIONAL([S3], [test x$enable_s3 = xyes])

AC_ARG_ENABLE(marfs, [  --enable-marfs]  [enable marfs support])
AM_CONDITIONAL([MARFS], [test x$enable_marfs = xyes])

AC_ARG_ENABLE(allstatic, [  --enable-allstatic]  [enable static linking of all libs])
AM_CONDITIONAL([ALLSTATIC], [test x$enable_allstatic = xyes])

AC_ARG_ENABLE(CTM, [  --enable-ctm]  [choose CTM mode = {xattrs | files | no}, default=files])

AC_CONFIG_HEADER([config.h])



# checks for programs.
# note that we are checking for mpicc first, the next check will verify CC
### AC_PROG_CC([mpicc cc])
AC_PROG_CC([mpic++ mpicxx mpig++ c++ g++])
AC_LANG(C++)
AC_PROG_CXX([mpic++ mpicxx mpig++ c++ g++])


# jti experiments
AC_SUBST([_s3_cflags],  ${S3_CFLAGS})
AC_SUBST([_s3_ldflags], ${S3_LDFLAGS})

# dejager experiments
AC_SUBST([_marfs_cflags],  ${MARFS_CFLAGS})
AC_SUBST([_marfs_ldflags], ${MARFS_LDFLAGS})

AC_SUBST([_plfs_cflags],  ${PLFS_CFLAGS})
AC_SUBST([_plfs_ldflags], ${PLFS_LDFLAGS})



# check for adequate mpi support
AS_IF([test x$enable_threads != xyes],
# [AC_MSG_CHECKING([if $CC can compile MPI applications])])
[AC_MSG_CHECKING([if $CXX can compile MPI applications])])

AS_IF([test x$enable_threads != xyes],
[AC_LINK_IFELSE([
    AC_LANG_PROGRAM(
        [[#include <mpi.h>]],
        [[MPI_Finalize();]])],
    [AC_MSG_RESULT([yes])],
    [AC_MSG_RESULT([no])
     AC_MSG_ERROR([$CXX cannot compile MPI applications. cannot continue.])]
)])

#AS_IF([test x$enable_threads = xyes],[cp -v etc/pftool.threaded.cfg etc/pftool.cfg])
#AS_IF([test x$enable_threads != xyes],[cp -v etc/pftool.mpi.cfg etc/pftool.cfg])

# chunk-transfer metadata can be stored to allow incremental restarts for
# large N:1 destination-files.  use '--enable-CTM=' to specify how (or
# whether) to store this info.
AS_IF([test x$enable_CTM == xxattrs],
      [AC_DEFINE([CTM_MODE], [CTM_PREFER_XATTRS], [prefer to store CTM in xattrs])],

      [test x$enable_CTM == xfiles],
      [AC_DEFINE([CTM_MODE], [CTM_PREFER_FILES], [prefer to store CTM in files])],

      [test x$enable_CTM == xno],
      [AC_DEFINE([CTM_MODE], [CTM_PREFER_NONE], [Do not store CTM])],

      [test x$enable_CTM == x],
      [AC_DEFINE([CTM_MODE], [CTM_PREFER_FILES], [prefer to store CTM in files])],

      [AC_MSG_ERROR([Usage: --enable-CTM=@<:@ {xattrs|files|no| }@:>@ ]) ])


### AM_PROG_CC_C_O
AC_PROG_CXX_C_O
#works with 2.6 and above
#AC_PROG_CC_C99

m4_pattern_allow([AM_PROG_AR])
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])

# checks for libraries.
# none at this point

# checks for header files.
AC_CHECK_HEADERS([sys/vfs.h gpfs.h gpfs_fcntl.h dmapi.h xattr.h])
AS_IF([test x$enable_plfs == xyes],
  [AC_CHECK_HEADERS([plfs.h])]
)
AS_IF([test x$enable_s3 == xyes],
  [AC_CHECK_HEADERS([aws4c.h aws4c_extra.h])]
)
AS_IF([test x$enable_marfs == xyes],
  [AC_CHECK_HEADERS([common.h marfs_ops.h aws4c.h aws4c_extra.h])]
)
AS_IF([test x"$enable_marfs" == xyes],
  [AC_CHECK_LIB([xml2], [xmlSAX2GetEntity])
   AC_CHECK_LIB([curl], [curl_easy_cleanup])
   AC_CHECK_LIB([aws4c_extra], [s3_create_bucket], [], [], [-laws4c])
   AC_CHECK_LIB([aws4c], [s3_enable_Scality_extensions])
   AC_CHECK_LIB([marfs], [mc_put],
                [LIBS = -lmarfs $LIBS -lisal -lne
                 AC_DEFINE(HAVE_LIBMARFS)],
                [AC_CHECK_LIB([marfs], [marfs_open], [],
                              [AC_ERROR([could not find libmarfs])])],
                [-lisal -lne])])

# checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T
AC_TYPE_SIZE_T

# checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strerror strtoul])

AC_CONFIG_FILES([Makefile
                 libs/Makefile
                 libs/tompi/Makefile
                 src/Makefile
                 scripts/Makefile
                 etc/Makefile])

AC_OUTPUT

# let the user know the configuration 

cat << EOF

pftool configuration
#####################
CXX      : $CXX
CXXFLAGS : $CXXFLAGS

CC       : $CC
CFLAGS   : $CFLAGS
LDFLAGS  : $LDFLAGS
LIBS     : $LIBS
CPPFLAGS : $CPPFLAGS
CPP      : $CPP

EOF
