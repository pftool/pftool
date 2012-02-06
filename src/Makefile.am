bin_PROGRAMS = $(top_builddir)/bin/pftool

__top_builddir__bin_pftool_SOURCES = \
debug.h \
hashtbl.c hashtbl.h \
pfutils.c pfutils.h \
pftool.c pftool.h 

if THREADS
threads_cflags=-DTHREADS_ONLY -I$(top_srcdir)/libs/tompi/include
threads_ldflags=-L$(top_srcdir)/libs/tompi/ -lmpi -lpthread
endif

if TAPE
tape_flags = -DTAPE -O -DGPFS_LINUX
tape_ldflags=-lgpfs -ldmapi
endif

if FUSECHUNKER
fusechunker_cflags=-DFUSE_CHUNKER
endif

if PLFS
plfs_cflags=-DPLFS
plfs_ldflags=-lplfs
endif

__top_builddir__bin_pftool_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags)
__top_builddir__bin_pftool_LDFLAGS = $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) 
