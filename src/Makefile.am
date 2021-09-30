generic_cflags  =
generic_ldflags =

if DEBUG
  generic_cflags  += -O0
  generic_ldflags += -O0
else
  generic_cflags  += -O3
  generic_ldflags += -O3
endif


if GPROF
  generic_cflags  += -pg
  generic_ldflags += -pg
endif


bin_PROGRAMS = $(top_builddir)/bin/pftool

__top_builddir__bin_pftool_SOURCES = \
  debug.h \
  cta.c ctf.c ctm.c ctm.h \
  hashtbl.c hashtbl.h hashdataCTM.c hashdataCTM.h\
  str.c str.h \
  sig.c sig.h \
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
  __top_builddir__bin_pftool_SOURCES += marfs.c marfs.h
  marfs_cflags=-DMARFS
  marfs_ldflags=-lisal
endif

if OLD_MARFS
  marfs_cflags=-DOLD_MARFS
  marfs_ldflags=-lisal
install-exec-local:
	chmod u+s $(bindir)/pftool
	chmod g-s $(bindir)/pftool
endif

if ALLSTATIC
  allstatic_ldflags=-all-static
endif

supportlib_ldflags=-lssl


__top_builddir__bin_pftool_CFLAGS   = $(generic_cflags) $(syndata_cflags) $(marfs_cflags) $(s3_cflags) @AWSXML_CFLAGS@

__top_builddir__bin_pftool_CXXFLAGS = $(generic_cflags) $(syndata_cflags) $(marfs_cflags) $(s3_cflags) @AWSXML_CFLAGS@

__top_builddir__bin_pftool_LDFLAGS  = $(generic_ldflags) $(supportlib_ldflags) $(s3_ldflags) $(marfs_ldflags) $(allstatic_ldflags) @AWSXML_LIBS@



# AM_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)
# AM_CXXFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags) $(s3_cflags)

# AM_LDFLAGS = $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) $(s3_ldflags) $(allstatic_ldflags)
