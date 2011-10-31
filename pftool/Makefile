CC					=	gcc
CFLAGS			=	-D_GNU_SOURCE -g 
IFLAGS      = -I../include
CLIBS				=

MPICLIBS		=	-L../src -lmpi -lpthread

DCFLAGS 		= -O 
GENERIC_FS_FLAGS     = -DDISABLE_TAPE -DDISABLE_PANFS -DTHREADS_ONLY

all: pftool 

pftool: pftool.o pfutils.o hashtbl.o
	$(CC) $(CFLAGS) $(IFLAGS) pftool.o pfutils.o hashtbl.o -o pftool $(MPICLIBS)

pftool.o: pftool.c pftool.h pfutils.o debug.h
	$(CC) $(CFLAGS) $(GENERIC_FS_FLAGS) $(IFLAGS) -c pftool.c

pfutils.o: pfutils.c pfutils.h debug.h
	$(CC) $(CFLAGS) $(GENERIC_FS_FLAGS) $(DCFLAGS) $(IFLAGS) -c pfutils.c

hashtbl.o: hashtbl.c hashtbl.h
	$(CC) $(CFLAGS) $(IFLAGS) -c hashtbl.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f *.pyc

cleanall:	clean
	- /bin/rm -f pftool
