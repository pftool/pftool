bin_PROGRAMS = $(top_builddir)/bin/pftool

if SYNDATA
__top_builddir__bin_pftool_SOURCES = \
debug.h \
cta.c ctf.c ctm.c ctm.h \
hashtbl.c hashtbl.h hashdataCTM.c hashdataCTM.h\
str.c str.h \
syndata.c syndata.h \
pfutils.c pfutils.h \
pftool.c pftool.h 

syndata_cflags=-DGEN_SYNDATA
else
__top_builddir__bin_pftool_SOURCES = \
debug.h \
cta.c ctf.c ctm.c ctm.h \
hashtbl.c hashtbl.h hashdataCTM.c hashdataCTM.h\
str.c str.h \
pfutils.c pfutils.h \
pftool.c pftool.h 
endif

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
plfs_cflags=${PLFS_CFLAGS} -DPLFS
plfs_ldflags=${PLFS_LDFLAGS} -lplfs
endif

if ALLSTATIC
allstatic_ldflags=-all-static
endif

supportlib_ldflags=-lssl

__top_builddir__bin_pftool_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags)
__top_builddir__bin_pftool_LDFLAGS = ${supportlib_ldflags} $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) $(allstatic_ldflags)
