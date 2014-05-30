if THREADS
EXTRA_DIST = pftool.threaded.cfg
else
EXTRA_DIST = pftool.mpi.cfg
endif

sysconfdir = $(prefix)/etc
sysconf_DATA = pftool.cfg 

CLEANFILES = pftool.cfg

pftool.cfg: $(srcdir)/$(EXTRA_DIST)
	cp -p $(EXTRA_DIST) $@
