#include "pftool.h"
#include "Path.h"

#if defined(MARFS)
#include "aws4c_extra.h"        // XML-parsing tools
#endif


// definitions of static vector-members for Pool templated-classes
template<typename T> std::vector<T*> Pool<T>::_pool;


// defns for static PathFactory members
uint8_t          PathFactory::_flags   = 0;
struct options*  PathFactory::_opts    = NULL;
pid_t            PathFactory::_pid     = 0;       // for PLFS
int              PathFactory::_rank    = 1;
int              PathFactory::_n_ranks = 1;


// NOTE: New path might not be same subclass as us.  For example, we could
// be descending into a PLFS volume.
//
// TBD: If we give our subclass to the factory, it could run tests relevant
//      to our type, first, in order to reduce the number of checks, which
//      might have a better chance of succeeding.  This would reduce the
//      overhead for constructing from a raw pathname.
//
PathPtr
Path::append(char* suffix) const {

   char  new_path[PATHSIZE_PLUS];
   
   size_t len = strlen(_item->path);
   strncpy(new_path,      _item->path, PATHSIZE_PLUS);
   strncpy(new_path +len, suffix,      PATHSIZE_PLUS -len);

   return PathFactory::create(new_path);
}



#ifdef MARFS

/**
 * used by the marfs_readdir_wrapper() to read only one item using marfs_readdir
 *
 * @param buf The buffer to copy the dirent into
 * @param name The name of the directory to be moved into the dirent named buff
 * @param stbuf Not used
 * @param off Not Used
 * @return 1 This tells marfs_readdir to only copy one thing
 */
int marfs_readdir_filler(void *buf, const char *name, const struct stat *stbuf, off_t off) {
   marfs_dirp_t* dir = (marfs_dirp_t*) buf;

   // /usr/include/bits/dirent.h shows that the size of this is 256 but I do not know how long this will be true
   strncpy(dir->name, name, PATH_MAX);
   dir->valid = 1; // this will allow us to see if the buffer has been filled

   /* we only want to copy once */
   return 1;
}

/**
 * A wrapper function around marfs_readdir that causes it to only return one directoy at a time
 * This allows it to be used in a posix manner
 *
 * @param dir The dirent to fill
 * @param path The path to read from
 * @param ffi The information about the directoy to be used by marfs_readdir
 * @return 0 on EOF, <0 on error, >0 on success
 */
int marfs_readdir_wrapper(marfs_dirp_t* dir, const char* path, MarFS_DirHandle* ffi) {
   int rc;
   dir->valid = 0; // this will allow us to see if the buffer has been filled

   rc = marfs_readdir(path, dir, marfs_readdir_filler, 0, ffi);
   if(0 == rc) {
      if(1 != dir->valid) {
         return 0;
      } else {
         return 1;
      }
   } else {
      return rc;
   }
}

bool packedFhInitialized = false;
bool packedFhInUse = false;
MarFS_FileHandle packedFh;

std::vector<path_item> packedPaths;

#endif

