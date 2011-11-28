all: pftool pftool-mpi

tompi:
	cd ../tompi; make

pftool: 
	cd src; make clean; make

pftool-mpi: 
	cd src; make clean; make -f Makefile.mpi

clean: 
	cd src; make clean
	

