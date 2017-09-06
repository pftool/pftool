EXTRA_DIST = AUTHORS README COPYRIGHT autogen etc

CLEANFILES = core*

if THREADS
    SUBDIRS = libs src scripts etc
    ACLOCAL_AMFLAGS = -I m4
else
    SUBDIRS = src scripts etc
endif

