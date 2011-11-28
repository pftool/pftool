all: pftool pftool-threaded pftool-mpi

pftool-mpi: 
	cd src; make clean; make -f Makefile.mpi

pftool-threaded: 
	cd src; make clean; make -f Makefile.threaded

pftool: 
	cd src; make clean; make -f Makefile

clean: 
	cd src; make clean
	

