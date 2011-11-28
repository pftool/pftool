CC					          =	gcc
CFLAGS			          =	-Wall -D_GNU_SOURCE -g

THREADCLIBS           = -L../../tompi/src -lmpi -lpthread
IFLAGS                = -I../../tompi/include

DLIB 				          = -lgpfs -ldmapi
GPFS_TYPE 	          = GPFS_LINUX 
DCFLAGS 		          = -O -D$(GPFS_TYPE)
#use generic fs if you want to build anywhere
GENERIC_FS_FLAGS      = -DDISABLE_TAPE -DDISABLE_PANFS $(THREADS_ONLY_FLAGS)
THREADS_ONLY_FLAGS    = -DTHREADS_ONLY

all: pftool


pftool: pftool.o pfutils.o hashtbl.o
	$(CC) $(CFLAGS) $(IFLAGS) $(DLIB) pftool.o pfutils.o hashtbl.o -o pftool $(THREADCLIBS)
	mv pftool ../bin/generic

pftool.o: pftool.c pftool.h pfutils.o debug.h
	$(CC) $(CFLAGS) $(GENERIC_FS_FLAGS) $(IFLAGS) -c pftool.c -o pftool.o

pfutils.o: pfutils.c pfutils.h debug.h
	$(CC) $(CFLAGS) $(DCFLAGS) $(GENERIC_FS_FLAGS) $(IFLAGS) -c pfutils.c -o pfutils.o

hashtbl.o: hashtbl.c hashtbl.h
	$(CC) $(CFLAGS) -c hashtbl.c -o hashtbl.o

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x

