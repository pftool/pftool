#include "pftool.h"
#include "Path.h"

// definitions of static vector-members for Pool templated-classes
template <typename T>
std::vector<T *> Pool<T>::_pool;

// defns for static PathFactory members
uint8_t PathFactory::_flags = 0;
struct options *PathFactory::_opts = NULL;
pid_t PathFactory::_pid = 0; // for PLFS
int PathFactory::_rank = 1;
int PathFactory::_n_ranks = 1;

// NOTE: New path might not be of the same subclass as us.  For example, we
//    could be descending into a PLFS volume.
//
// NOTE: We return a new Path object to the changed path, leaving the
//    original intact.  (a) it's possible the new Path is of a different
//    sub-class, and (b) we avoid the potential to leave various fields
//    inconsistent.  (We could figure out which ones need to be reset and
//    do that, but we're taking the simpler approach, for now.)
//
// TBD: If we give our subclass to the factory, it could run tests relevant
//    to our type, first, in order to reduce the number of checks, which
//    might have a better chance of succeeding.  This would reduce the
//    overhead for constructing from a raw pathname.

PathPtr
Path::path_append(char *suffix) const
{

   char new_path[PATHSIZE_PLUS];
   size_t len = strlen(_item->path);

   strncpy(new_path, _item->path, PATHSIZE_PLUS);
   strncpy(new_path + len, suffix, PATHSIZE_PLUS - len);

   if (new_path[PATHSIZE_PLUS - 1])
      return PathPtr(); // return NULL, for overflow

   return PathFactory::create(new_path);
}

// remove a suffix.  If <size> is negative, it is size of suffix to remove.
// Otherwise, it is the size of the prefix to keep.
//
// NOTE: We return a new Path object to the changed path, leaving the
//    original intact.  (a) it's possible the new Path is of a different
//    sub-class, and (b) we avoid the potential to leave various fields
//    inconsistent.  (We could figure out which ones need to be reset and
//    do that, but we're taking the simpler approach, for now.)

PathPtr
Path::path_truncate(ssize_t size) const
{

   char new_path[PATHSIZE_PLUS];

   size_t new_len = size;
   if (size < 0)
      new_len = strlen(_item->path) - size;

   if (new_len >= PATHSIZE_PLUS)
      return PathPtr(); // return NULL, for overflow/underflow

   strncpy(new_path, _item->path, new_len);
   new_path[new_len] = 0;

   return PathFactory::create(new_path);
}

#ifdef MARFS
marfs_fhandle marfsCreateStream;
marfs_fhandle marfsSourceReadStream;
marfs_fhandle marfsDestReadStream;
marfs_ctxt marfsctxt;
char marfs_ctag_set;

int initialize_marfs_context( void ) {
   marfs_ctag_set = 0;
   marfsCreateStream = NULL;
   marfsSourceReadStream = NULL;
   marfsDestReadStream = NULL;
   // NOTE -- as pftool is NOT multi-threaded, we are allowing libmarfs itself to handle erasure locking
   marfsctxt = marfs_init( MARFS_CONFIG_PATH, MARFS_BATCH, NULL );
   if ( marfsctxt == NULL ) {
      return -1;
   }
   return 0;
}
#endif
