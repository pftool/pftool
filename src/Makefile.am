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

feature_enable_cflags =
if MARFS
  feature_enable_cflags += -DMARFS
install-exec-local:
	chmod u+s $(bindir)/pftool
endif
if CONDUIT
  feature_enable_cflags += -DCONDUIT
endif
if TMPFILE
  feature_enable_cflags += -DTMPFILE
endif
if RESTART
  feature_enable_cflags += -DRESTART
endif

if ALLSTATIC
  allstatic_ldflags=-all-static
endif

supportlib_ldflags=-lssl


__top_builddir__bin_pftool_CFLAGS   = $(generic_cflags) $(syndata_cflags) $(feature_enable_cflags)

__top_builddir__bin_pftool_CXXFLAGS = $(generic_cflags) $(syndata_cflags) $(feature_enable_cflags)

__top_builddir__bin_pftool_LDFLAGS  = $(generic_ldflags) $(supportlib_ldflags) $(marfs_ldflags) $(allstatic_ldflags)



# AM_CFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags)
# AM_CXXFLAGS = $(threads_cflags) $(tape_cflags) $(fusechunker_cflags) $(plfs_cflags) $(syndata_cflags)

# AM_LDFLAGS = $(threads_ldflags) $(tape_ldflags) $(plfs_ldflags) $(allstatic_ldflags)
