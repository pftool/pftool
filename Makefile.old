all:  pftool #pftool-mpi


pftool: 
	cd src; make clean; make

tompi:
	cd ../tompi; make

pftool-mpi: 
	cd src; make clean; make -f Makefile.mpi

clean: 
	cd src; make clean
	
cleanall: 
	- cd src; make cleanall
	- cd src; make -f Makefile.mpi cleanall
	- cd ../tompi; make clean
