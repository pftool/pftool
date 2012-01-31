bin_PROGRAMS = pftool

pftool_SOURCES = \
hashtbl.c hashtbl.h \
pfutils.c pfutils.h \
pftool.c pfutils.h 

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

pftool_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags)
pftool_LDFLAGS = $(threads_ldflags) $(tape_ldflags)
