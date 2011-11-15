BASE = ..
include $(BASE)/Makefile.inc

LIB = libmpi.a

PROFILE = 

all: ../include/threadspec.h check_profile subdirs lib

../include/threadspec.h: ../include/gthread.h ../include/mkthreadspec ../Makefile.inc
	cd ../include; make

check_profile:
	./chk_profile $(PROFILE)
	echo $(PROFILE) > .last-profile

subdirs: dcoll dcomm dcontext derror dgroup dmisc dpt2pt dtopo dtypes
dcoll:
	cd coll; make
dcomm:
	cd comm; make
dcontext:
	cd context; make
derror:
	cd error; make
dgroup:
	cd group; make
dmisc:
	cd misc; make
dpt2pt:
	cd pt2pt; make
dtopo:
	cd topo; make
dtypes:
	cd types; make

proto:
	./makeproto

lib:
	rm -f $(LIB)
	ar cr $(LIB) */*.o
	-ranlib $(LIB)

pmpi: profile
profile:
	make LIB=libpmpi.a PROFILE=-DPROFILE all

clean:
	rm -f */*.o
distclean: clean
	rm -f *~ */*~ libmpi.a libpmpi.a .last-profile

check:
	find . -name '*.c' -print | sed 's/\.c$$/.o/' | sort > .expected
	find . -name '*.o' -print | sort | diff .expected - | cat
	rm -f .expected
