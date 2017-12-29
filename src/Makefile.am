
bin_PROGRAMS = $(top_builddir)/bin/pftool

__top_builddir__bin_pftool_SOURCES = \
debug.h \
cta.c ctf.c ctm.c ctm.h \
hashtbl.c hashtbl.h hashdataCTM.c hashdataCTM.h\
str.c str.h \
pfutils.cpp pfutils.h \
pftool.cpp pftool.h \
Path.cpp Path.h

if SYNDATA
__top_builddir__bin_pftool_SOURCES += syndata.c syndata.h Path-syndata.h
syndata_cflags=-DGEN_SYNDATA
endif

if S3
s3_cflags=$(_s3_cflags) -DS3
s3_ldflags=$(_s3_ldflags) -laws4c -laws4c_extra -lcurl -lxml2
endif

if MARFS
marfs_cflags=-DMARFS
marfs_ldflags=-lisal
install-exec-local:
	chmod u+s $(bindir)/pftool
	chmod g-s $(bindir)/pftool
endif

if ALLSTATIC
allstatic_ldflags=-all-static
endif

supportlib_ldflags=-lssl


__top_builddir__bin_pftool_CFLAGS   = $(syndata_cflags) $(marfs_cflags) $(s3_cflags)
__top_builddir__bin_pftool_CXXFLAGS = $(syndata_cflags) $(marfs_cflags) $(s3_cflags)
__top_builddir__bin_pftool_LDFLAGS  = ${supportlib_ldflags} $(s3_ldflags) $(marfs_ldflags) $(allstatic_ldflags)
