BASE = ..
include ../Makefile.inc

all: threadspec.h

# If you change the following dependency line, you probably want to change the
# corresponding one in src/Makefile.
threadspec.h: gthread.h ../Makefile.inc mkthreadspec
	./mkthreadspec "$(CPP)"

clean:

distclean:
