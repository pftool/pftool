# Top-level makefile for TOMPI (see the README file), to avoid cd'ing
# everywhere.

all:
	cd src; make
profile:
	cd src; make profile

clean:
	cd src; make clean
	cd mpicc; make clean

distclean:
	rm -f *~ include/*~
	cd src; make distclean
	cd mpicc; make distclean
