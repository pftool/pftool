# This file defines most system-dependent values.  You will also have to modify
# include/threading.h.  Makefiles including this file must define BASE to point
# to the directory containing this file (e.g., "..").

# How to link the threads library.  If you change this value, you will also
# have to modify include/threading.h.
 LIBTHREADS = -lpthread         # For pthreads
# LIBTHREADS = -lthread          # For Solaris threads
#LIBTHREADS = -lcthreads        # For Cthreads
#LIBTHREADS = -lRtt             # For RT threads

# Optimization and debugging flags.  I have had *major* problems with high
# levels of optimization on AIX, so it is not recommended.  TOMPI is pretty
# fast to compile, so you're welcome to try it once you get it running.
# Let me know what success/failure you have (on any operating system).
#OPTFLAGS = -fast               # Solaris optimization (works)
 OPTFLAGS = -g                  # Debugging mode

# The C compiler.  Also the linker for example programs if you don't change
# LINK below.  IMPORTANT NOTE: Most operating systems require a special
# compiler or flag to use threads (to link re-entrant libc, use re-entrant
# errno, etc.).  Be sure to get this right.
#CC = cc -D_REENTRANT           # Possibly required for Solaris threads
#CC = cc_r                      # Required for AIX threads
 CC = gcc
#CC = cc

# How to run the C preprocessor.  Do *not* run cpp directly, since this won't
# pass the appropriate -D's.
CPP = $(CC) -E

# If your include files (e.g., for the threads package) are in a funny place,
# you can add -Ipath to the end of this definition.
# If not, you can leave this as is.
INCLUDES = -I$(BASE)/include

# You shouldn't have to change this.
CFLAGS = $(OPTFLAGS) $(INCLUDES) $(PROFILE)

# Change the $(CC) if you want to use a different linker.  See the important
# note for CC above.
LINK = $(CC) $(CFLAGS)

# -------------- No need to change anything below this point. --------------

# How to link the generated MPI library.
LIBMPI = -L$(BASE)/src -lmpi $(LIBTHREADS)
LIBPMPI = -L$(BASE)/src -lpmpi $(LIBTHREADS)

# What file to depend on to recompile when the MPI library changes.
LIBDEP = $(BASE)/src/libmpi.a
PLIBDEP = $(BASE)/src/libpmpi.a
