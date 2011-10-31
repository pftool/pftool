BASE = ..
include ../Makefile.inc

# GNU flex is HIGHLY RECOMMENDED.  Segmentation faults have been encountered
# with other lex's, namely the one in OSF1 V3.2 on DEC alphas.  Note that you
# must use the flex -l compatibility mode, or add the %array directive to the
# top section of lexer.l.
LEX = flex -l

all: g2tsd mpicc #g2tsd_lib.o

g2tsd: lexer.o g2tsd.o hash.o
	$(CC) -o g2tsd g2tsd.o hash.o lexer.o
lexer.c: lexer.l
	$(LEX) lexer.l
	mv lex.yy.c lexer.c

mpicc: mpicc.in ../Makefile.inc mkmpicc
	mkmpicc

clean:
	rm -f *.o

distclean: clean
	rm -f *~ g2tsd
