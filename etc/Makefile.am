EXTRA_DIST = pftool.mpi.cfg

sysconfdir = $(prefix)/etc
sysconf_DATA = example-pftool.cfg

CLEANFILES = example-pftool.cfg

example-pftool.cfg: $(srcdir)/$(EXTRA_DIST)
	cp -p $(EXTRA_DIST) $@
