AC_PREREQ([2.59])

AC_INIT([pftool], [2.1.3], [dsherril@lanl.gov])


# # Check If AM_SILENT_RULES Rule Are Supported
# # If So, Build Using AM_SILENT_RULES
# m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([src])

# note: -Wall -Werror are automake options.  they are NOT compiler flags
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])

# change defaults, instead of "-g -O2"
CXXFLAGS="-g"
CFLAGS="$CFLAGS -g"
CPPFLAGS="$CFLAGS"
# pftool, as a setuid program, will need to have any custom library path explicitly built into the binary ( NO user env )
if test x"$prefix" != xNONE; then LDFLAGS="$LDFLAGS -Wl,-rpath=$prefix/lib"; fi

# lets add some options to our CFLAGS variable
# AC_ARG_ENABLE(threads,
#    AS_HELP_STRING([  --enable-threads], [Run in threading mode]))
# AM_CONDITIONAL([THREADS], [test x$enable_threads = xyes])

AC_ARG_ENABLE(syndata,
   AS_HELP_STRING([  --enable-syndata], [enable synthetic data generation support]))
AM_CONDITIONAL([SYNDATA], [test x$enable_syndata = xyes])

MARFS_ENABLED="DISABLED"
AC_ARG_ENABLE(marfs,
   AS_HELP_STRING([  --enable-marfs], [enable marfs support]))
AM_CONDITIONAL([MARFS], [test x$enable_marfs = xyes]  &&  MARFS_ENABLED="ENABLED")

AC_ARG_ENABLE(gprof,
   AS_HELP_STRING([  --enable-gprof], [enable run-time profiling with gprof]))
AM_CONDITIONAL([GPROF], [test x$enable_gprof = xyes])

AC_ARG_ENABLE(allstatic,
   AS_HELP_STRING([  --enable-allstatic], [enable static linking of all libs]))
AM_CONDITIONAL([ALLSTATIC], [test x$enable_allstatic = xyes])

AC_ARG_ENABLE(CTM,
   AS_HELP_STRING([  --enable-ctm], [choose CTM mode = {xattrs | files | no}, default=files]))

AC_ARG_ENABLE(debug,
   AS_HELP_STRING([  --enable-debug], [build with -O0, instead of -O3]))
AM_CONDITIONAL([DEBUG], [test "x$enable_debug" = xyes])

CONDUIT_ENABLED="DISABLED"
AC_ARG_ENABLE(conduit,
   AS_HELP_STRING([  --enable-conduit], [Enable extra Conduit message output, disable tempfile logic, disable persistent CTM]))
AM_CONDITIONAL([CONDUIT], [test x$enable_conduit = xyes]  &&  CONDUIT_ENABLED="ENABLED")

TMPFILE_ENABLED="DISABLED"
AC_ARG_ENABLE([tmpfile],
              AS_HELP_STRING( [--disable-tmpfile], [Disable output to temporary file during parallel write @{:@ CONDUIT support forces this off @:}@] ) )
AM_CONDITIONAL([TMPFILE], [( test "x$enable_tmpfile" = "xyes"  ||  test "x$enable_tmpfile" = "x" )  &&  test "x$CONDUIT_ENABLED" = "xDISABLED"]  &&  TMPFILE_ENABLED="ENABLED")

RESTART_ENABLED="DISABLED"
AC_ARG_ENABLE([restart],
              AS_HELP_STRING( [--disable-restart], [Disable restartability of parallel writes and output of CTM info @{:@ CONDUIT support forces this off @:}@] ) )
AM_CONDITIONAL([RESTART], [( test "x$enable_restart" = "xyes"  ||  test "x$enable_restart" = "x" )  &&  test "x$CONDUIT_ENABLED" = "xDISABLED"]  &&  RESTART_ENABLED="ENABLED")

AC_CONFIG_HEADER([config.h])


# checks for programs.
# note that we are checking for mpicc first, the next check will verify CC
### AC_PROG_CC([mpicc cc])
AC_PROG_CC([mpic++ mpicxx mpig++ c++ g++])
AC_LANG(C++)
AC_PROG_CXX([mpic++ mpicxx mpig++ c++ g++])


# dejager experiments
AC_SUBST([_marfs_cflags],  ${MARFS_CFLAGS})
AC_SUBST([_marfs_ldflags], ${MARFS_LDFLAGS})

# define the MARFS config path
AM_COND_IF([MARFS],[
   AS_IF([test "x$MARFS_CONFIG_PATH" != x],
      [AC_DEFINE_UNQUOTED([MARFS_CONFIG_PATH], ["$MARFS_CONFIG_PATH"], [Path to the MarFS Config File])],
      [AC_MSG_ERROR([MARFS_CONFIG_PATH is not defined!])])
   ])

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


# checks for header files.
AC_CHECK_HEADERS([sys/vfs.h gpfs.h gpfs_fcntl.h dmapi.h xattr.h])
AS_IF([test x$enable_marfs == xyes],
  [AC_CHECK_HEADERS([marfs.h])]
)

# # check for adequate mpi support
# AS_IF([test x$enable_threads != xyes],
# [AC_MSG_CHECKING([if $CXX can compile MPI applications])])
#
# AS_IF([test x$enable_threads != xyes],
# [AC_LINK_IFELSE([
#     AC_LANG_PROGRAM(
#         [[#include <mpi.h>]],
#         [[MPI_Finalize();]])],
#     [AC_MSG_RESULT([yes])],
#     [AC_MSG_RESULT([no])
#      AC_MSG_ERROR([$CXX cannot compile MPI applications. cannot continue.])]
# )])

# AS_IF([test x"$enable_threads" != xyes], [
AC_CHECK_LIB([mpi], [MPI_Abort],
             [],
             [AC_MSG_ERROR([$CXX cannot compile MPI applications. Cannot continue.])] )
# ])
AC_CHECK_HEADERS([mpi.h])


AC_CHECK_LIB([crypto], [EVP_MD_CTX_new], [], [
   AC_DEFINE([USE_DEPRECATED_CRYPTO], [1], [use deprecated libcrypto interface])
   AC_CHECK_LIB([crypto], [MD5_Init], [], [AC_ERROR([could not find libcrypto])])
])

AC_CHECK_LIB([rt], [timer_settime])


# checks for libraries.
# NOTE: each test *prepends* its library to LIBS, so test for the
#       depended-on libs before the libs that depend on them.
AS_IF([test x"$enable_marfs" == xyes],
   [AC_CHECK_LIB([marfs], [marfs_init], [], [AC_ERROR([could not find libmarfs])])])


# checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T
AC_TYPE_SIZE_T

# checks for library functions.
AC_CHECK_FUNCS([memset strerror strtoul])

# AC_FUNC_MALLOC
AC_CHECK_FUNCS([malloc])


AC_CONFIG_FILES([Makefile
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

PREFIX   : $prefix

MARFS Support   : $MARFS_ENABLED
CONDUIT Support : $CONDUIT_ENABLED
Tempfile Output : $TMPFILE_ENABLED
Restartability  : $RESTART_ENABLED

EOF
