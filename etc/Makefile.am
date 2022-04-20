EXTRA_DIST = pftool.mpi.cfg

sysconfdir = $(prefix)/etc
sysconf_DATA = pftool.cfg.example 

CLEANFILES = pftool.cfg.example

pftool.cfg.example: $(srcdir)/$(EXTRA_DIST)
	cp -p $(EXTRA_DIST) $@
