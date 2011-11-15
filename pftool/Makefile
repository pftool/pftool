CC					          =	gcc
CFLAGS			          =	-Wall -D_GNU_SOURCE -g

MPI_ROOT 		          = /gpfstmnt/usr/local/openmpi
MPICC				          =	$(MPI_ROOT)/bin/mpicc
MPICFLAGS		          =	$(MPI_COMPILE_FLAGS) -g -I$(MPI_ROOT)/include
MPICLIBS		          =	-lmpi -L$(MPI_ROOT)/lib

THREADCLIBS           = -L../tompi/src -lmpi -lpthread
IFLAGS                = -I../tompi/include

DLIB 				          = -lgpfs -ldmapi
GPFS_TYPE 	          = GPFS_LINUX 
DCFLAGS 		          = -O -D$(GPFS_TYPE)
#use generic fs if you want to build anywhere
GENERIC_FS_FLAGS      = -DDISABLE_TAPE -DDISABLE_PANFS $(THREADS_ONLY_FLAGS)
THREADS_ONLY_FLAGS    = -DTHREADS_ONLY

#pftool dmapilookup
all: pftool pftool-thread #pftool-generic




pftool-generic: pftool-generic.o pfutils-generic.o hashtbl-generic.o
	$(CC) $(CFLAGS) $(IFLAGS) $(DLIB) pftool-generic.o pfutils-generic.o hashtbl-generic.o -o pftool-generic $(THREADCLIBS)

pftool-generic.o: pftool.c pftool.h pfutils-generic.o debug.h
	$(CC) $(CFLAGS) $(GENERIC_FS_FLAGS) $(IFLAGS) -c pftool.c -o pftool-generic.o

pfutils-generic.o: pfutils.c pfutils.h debug.h
	$(CC) $(CFLAGS) $(DCFLAGS) $(GENERIC_FS_FLAGS) $(IFLAGS) -c pfutils.c -o pfutils-generic.o

hashtbl-generic.o: hashtbl.c hashtbl.h
	$(CC) $(CFLAGS) -c hashtbl.c -o hashtbl-generic.o



pftool-thread: pftool-thread.o pfutils-thread.o hashtbl-thread.o
	$(CC) $(CFLAGS) $(IFLAGS) $(DLIB) pftool-thread.o pfutils-thread.o hashtbl-thread.o -o pftool-thread $(THREADCLIBS)

pftool-thread.o: pftool.c pftool.h pfutils-thread.o debug.h
	$(CC) $(CFLAGS) $(THREADS_ONLY_FLAGS) $(IFLAGS) -c pftool.c -o pftool-thread.o

pfutils-thread.o: pfutils.c pfutils.h debug.h
	$(CC) $(CFLAGS) $(DCFLAGS) $(THREADS_ONLY_FLAGS) $(IFLAGS) -c pfutils.c -o pfutils-thread.o

hashtbl-thread.o: hashtbl.c hashtbl.h
	$(CC) $(CFLAGS) -c hashtbl.c -o hashtbl-thread.o



pftool: pftool.o pfutils.o hashtbl.o
	$(MPICC) $(CFLAGS) $(MPICLIBS) $(DLIB)  pftool.o pfutils.o hashtbl.o -o pftool

pftool.o: pftool.c pftool.h pfutils.o
	$(MPICC) $(CFLAGS) -c pftool.c

pfutils.o: pfutils.c pfutils.h
	$(MPICC) $(CFLAGS) $(DCFLAGS) -c pfutils.c

recall_api.o: recall_api.c recall_api.h
	$(MPICC) $(CFLAGS) $(DCFLAGS) -c recall_api.c

hashtbl.o: hashtbl.c hashtbl.h
	$(MPICC) $(CFLAGS) -c hashtbl.c

dmapilookup: dmapilookup.o pfutils.o
	$(MPICC) $(CFLAGS) $(DLIB) dmapilookup.o pfutils.o -o dmapilookup

dmapilookup.o: dmapilookup.c 
	$(MPICC) $(CFLAGS) $(DCFLAGS) -c dmapilookup.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f *.pyc

cleanall:	clean
	- /bin/rm -f pftool pftool-thread pftool-generic dmapilookup
