MPI_ROOT 		= /gpfstmnt/usr/local/openmpi
CC					=	gcc
CFLAGS			=	-Wall -D_GNU_SOURCE -g
CLIBS				=

MPICC				=	$(MPI_ROOT)/bin/mpicc
MPICFLAGS		=	$(MPI_COMPILE_FLAGS) -g -I$(MPI_ROOT)/include
MPICLIBS		=	-lmpi -L$(MPI_ROOT)/lib

DLIB 				= -lgpfs -ldmapi
GPFS_TYPE 	= GPFS_LINUX 
DCFLAGS 		= -O -D$(GPFS_TYPE)

all: pftool 

pftool: pftool.o pfutils.o recall_api.o
	$(MPICC) $(CFLAGS) $(MPICLIBS) $(DLIB) pftool.o pfutils.o recall_api.o -o pftool


pftool.o: pftool.c pftool.h pfutils.o
	$(MPICC) $(CFLAGS) $(MYSQINCS) -c pftool.c

pfutils.o: pfutils.c pfutils.h
	$(MPICC) $(CFLAGS) $(DCFLAGS) -c pfutils.c

recall_api.o: recall_api.c recall_api.h
	$(MPICC) $(CFLAGS) $(DCFLAGS) -c recall_api.c



clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f *.pyc

cleanall:	clean
	- /bin/rm -f pftool
