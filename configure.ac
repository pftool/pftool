AC_PREREQ([2.60])

AC_INIT([pftool], [2.0], [agtorre@lanl.gov])

# Check If AM_SILENT_RULES Rule Are Supported
# If So, Build Using AM_SILENT_RULES
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([src])

# note: -Wall -Werror are automake options.  they are NOT compiler flags
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

AM_CONFIG_HEADER([config.h])

# checks for programs.
# note that we are checking for mpicc first, the next check will verify CC
AC_PROG_CC([mpicc cc])

# check for adequate mpi support
AC_MSG_CHECKING([if $CC can compile MPI applications])
AC_LINK_IFELSE([
    AC_LANG_PROGRAM(
        [[#include <mpi.h>]],
        [[MPI_Finalize();]])],
    [AC_MSG_RESULT([yes])],
    [AC_MSG_RESULT([no])
     AC_MSG_ERROR([$CC cannot compile MPI applications. cannot continue.])]
)

AM_PROG_CC_C_O
AC_PROG_CC_C99

# checks for libraries.
# none at this point

# checks for header files.
AC_CHECK_HEADERS([\
signal.h getopt.h stdio.h string.h stdlib.h stat.h statfs.h time.h types.h vfs.h dirent.h libgen.h unistd.h errno.h utime.h gpfs.h gpfs_fcntl.h dmapi.h xattr.h\
])

# checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T
AC_TYPE_SIZE_T

# checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset strerror strtoul])

AC_CONFIG_FILES([Makefile
                 src/Makefile])

AC_OUTPUT

# let the user know the configuration 

cat << EOF

mpimemu configuration
#####################

CC      : $CC
CFLAGS  : $CFLAGS
LDFLAGS : $LDFLAGS
LIBS    : $LIBS
CPPFLAGS: $CPPFLAGS
CPP     : $CPP

EOF
