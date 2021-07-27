AC_PREREQ([2.59])

AC_INIT([pftool], [2.1.3], [dsherril@lanl.gov])


# # Check If AM_SILENT_RULES Rule Are Supported
# # If So, Build Using AM_SILENT_RULES
# m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([src])

# note: -Wall -Werror are automake options.  they are NOT compiler flags
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])


# lets add some options to our CFLAGS variable
# AC_ARG_ENABLE(threads,
#    AS_HELP_STRING([  --enable-threads], [Run in threading mode]))
# AM_CONDITIONAL([THREADS], [test x$enable_threads = xyes])

AC_ARG_ENABLE(syndata,
   AS_HELP_STRING([  --enable-syndata], [enable synthetic data generation support]))
AM_CONDITIONAL([SYNDATA], [test x$enable_syndata = xyes])

AC_ARG_ENABLE(s3,
   AS_HELP_STRING([  --enable-s3], [enable S3 support]))
AM_CONDITIONAL([S3], [test x$enable_s3 = xyes])

AC_ARG_ENABLE(marfs,
   AS_HELP_STRING([  --enable-marfs], [enable marfs support]))
AM_CONDITIONAL([MARFS], [test x$enable_marfs = xyes])

AC_ARG_ENABLE(old_marfs,
   AS_HELP_STRING([  --enable-old_marfs], [enable old_marfs support]))
AM_CONDITIONAL([OLD_MARFS], [test x$enable_old_marfs = xyes])

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

AC_CONFIG_HEADER([config.h])



# change defaults, instead of "-g -O2"
: ${CFLAGS="-g "} ${CXXFLAGS="-g"}

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
AS_IF([test x$enable_s3 == xyes],
  [AC_CHECK_HEADERS([aws4c.h aws4c_extra.h])]
)
AS_IF([test x$enable_marfs == xyes],
  [AC_CHECK_HEADERS([marfs.h])]
)
AS_IF([test x$enable_old_marfs == xyes],
  [AC_CHECK_HEADERS([common.h marfs_ops.h aws4c.h aws4c_extra.h])]
)
AS_IF([test x$enable_s3 == xyes  ||  test x$enable_old_marfs == xyes], [
  PKG_CHECK_MODULES(AWSXML, libxml-2.0)
  AC_SUBST(AWSXML_CFLAGS)
  AC_SUBST(AWSCML_LIBS)
  ]
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

AC_CHECK_LIB([crypto], [MD5_Final])
AC_CHECK_LIB([rt], [timer_settime])

# checks for libraries.
# NOTE: each test *prepends* its library to LIBS, so test for the
#       depended-on libs before the libs that depend on them.
AS_IF([test x"$enable_marfs" == xyes],
   [AC_CHECK_LIB([xml2], [xmlSAX2GetEntity])
   AC_CHECK_LIB([isal], [ec_encode_data])
   AC_CHECK_LIB([marfs], [marfs_init])
  ])

AS_IF([test x"$enable_old_marfs" == xyes],
  [AC_CHECK_LIB([crypto], [MD5_Init])
   AC_CHECK_LIB([xml2], [xmlSAX2GetEntity])
   AC_CHECK_LIB([curl], [curl_easy_cleanup])
   AC_CHECK_LIB([aws4c], [s3_enable_Scality_extensions])
   AC_CHECK_LIB([aws4c_extra], [s3_create_bucket])
   AC_CHECK_LIB([pthread], [pthread_tryjoin_np] )
   AC_CHECK_LIB([rdmacm], [rdma_getaddrinfo] )
   AC_CHECK_LIB([ft],   [fast_timer_inits])
   AC_CHECK_LIB([rdma_sockets], [skt_open])
   AC_CHECK_LIB([isal], [ec_encode_data])
   AC_CHECK_LIB([rt],   [timer_settime])
   AC_CHECK_LIB([dl],   [dlclose])
   AC_CHECK_LIB([ne],   [ne_write])
   AC_CHECK_LIB([dl], [dlclose])
   AC_CHECK_LIB([marfs], [mc_put], [],
                [AC_CHECK_LIB([marfs], [marfs_open], [],
                              [AC_ERROR([could not find libmarfs])])])
  ])

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

EOF
