EXTRA_DIST = pftool.mpi.cfg

sysconfdir = $(prefix)/etc
sysconf_DATA = pftool.cfg 

CLEANFILES = pftool.cfg

pftool.cfg: $(srcdir)/$(EXTRA_DIST)
	cp -p $(EXTRA_DIST) $@
