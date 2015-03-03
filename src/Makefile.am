bin_PROGRAMS = $(top_builddir)/bin/pftool

if SYNDATA
__top_builddir__bin_pftool_SOURCES = \
debug.h \
hashtbl.c hashtbl.h \
str.c str.h \
syndata.c syndata.h \
pfutils.c pfutils.h \
pftool.cpp pftool.h \
Path.cpp Path.h

syndata_cflags=-DGEN_SYNDATA

else
__top_builddir__bin_pftool_SOURCES = \
debug.h \
hashtbl.c hashtbl.h \
str.c str.h \
pfutils.c pfutils.h \
pftool.cpp pftool.h \
Path.cpp Path.h

endif


if THREADS
threads_cflags=-DTHREADS_ONLY -I$(top_srcdir)/libs/tompi/include
threads_ldflags=-L$(top_srcdir)/libs/tompi/ -lmpi -lpthread
endif

if TAPE
tape_cflags = -DTAPE -O -DGPFS_LINUX
tape_ldflags=-lgpfs -ldmapi
endif

if FUSECHUNKER
fusechunker_cflags=-DFUSE_CHUNKER
endif

if PLFS
plfs_cflags=$(_plfs_cflags) -DPLFS
plfs_ldflags=$(_plfs_ldflags) -lplfs
endif

if S3
s3_cflags=$(_s3_cflags) -DS3
s3_ldflags=$(_s3_ldflags) -laws4c -laws4c_extra -lcurl -lxml2
endif

if ALLSTATIC
allstatic_ldflags=-all-static
endif



__top_builddir__bin_pftool_CFLAGS   = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)
__top_builddir__bin_pftool_CXXFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)

__top_builddir__bin_pftool_LDFLAGS  = $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) $(s3_ldflags) $(allstatic_ldflags)



# AM_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)
# AM_CXXFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)

# AM_LDFLAGS = $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) $(s3_ldflags) $(allstatic_ldflags)
