#ifndef __PATH_H
#define __PATH_H

// ---------------------------------------------------------------------------
// Paths should have subclasses to provide file-system-specific methods to
// do all of the following:
//
//   -- stat  (e.g. exists?  is-a-directory?  size?)
//   -- read into buf
//   -- write buf
//
//   -- manipulate paths
//      o  find "base" path (e.g. the name of the containing directory)
//      o  append  (e.g. create the full path for a file in a directory)
//      o  translate an input-path to the corresponding output-path
//
//
// User figures out what type of system is involved and builds subclass of
// the appropriate type.  Subclasses just implement a specific protocol.
//
// In order to minimize changes to the working pftool code-base, I'm trying
// to layer this interface on top of the old code.  The old code (in C)
// passes blocks of path_item structs around, and uses MPI pack/unpack to
// write/read them from the buffers.  I don't want to require a
// dynamic-allocation of a new C++ object for every path that used to be
// accessed in this way.  In other words, I want to be able to convert a
// path_item to one of the new C++ classes, without dynamic-allocation of a
// new object.
//
// If we wanted to require that no Path subclasses can have private members,
// we could just cast path_items to classes, but that requirement would make
// things very awkward.
//
// Instead, we have a PathFactory that uses Pools of previously allocated
// objects which can be reused.  The factory provides "smart"-pointers
// (std::tr1::shared_ptr<>).  These automatically return their managed
// objects to the pool, when the shared pointer goes out of scope, or is
// re-assigned.  We typically go through blocks of path_items sequentially,
// so the pools will probably never hold more than one or two objects in
// them.  So, you can think of the factory as "cheap".
//
// However, this means the objects must have constructors that allow the
// pools to allocate new objects with whatever minimal initialization is
// available at the time the factory is called.  For now, the factory only
// uses no-op constructors for new objects.  All initializations in the
// factory are done via assignment operators.  Therefore, Path subclasses
// can ignore constructors and concentrate on supporting
// assignment-operators that are needed by the create() methods in the
// factory.  Do your local subclass-initializations, then just call
// Path::operator=(...) with the appropriate arguments.
//
// TBD:
//
// -- Declare user-defined MPI types for all the subclasses, commit them at
//    initialization-time.  No need for pack/unpack.  Each block of
//    path_items is assumed to contain a single type of object.  Beginning
//    of each block has a code that indicates the type to use for all
//    objects in the block.  (Or, we just read the path_item::ftype of the
//    first object in the block.)  Factory then selects an appropriate type
//    to use in an MPI_recv, and returns a pointer to the received thing.
//
// -- Accomodate schemes we are discussing for doing bulk updates of
//    metadata.
//
// ---------------------------------------------------------------------------

#include "pfutils.h"

#include <assert.h>
#include <stdint.h>
#include <stdarg.h> // va_list, va_start(), va_arg(), va_end()
#include <string.h> // strerror()

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h> // POSIX directories

#include <cxxabi.h> // name-demangling
// #include <typeinfo>             // typeid()

#include <iostream>
#include <string>
#include <queue>
#include <vector>
#include <tr1/memory>

#ifdef MARFS
#include <marfs.h>
#endif

// --- fwd-decls

#define SharedPtr std::tr1::shared_ptr

class Path;
class PathFactory;

typedef SharedPtr<path_item> PathItemPtr;
typedef SharedPtr<Path> PathPtr;
//typedef SharedPtr<char>        CharPtr;

// ---------------------------------------------------------------------------
// No-op shared-ptr
//
// This can be useful e.g. for supplying a shared-pointer, when the
// pointed-to data is actually static.  For example:
//
//   path_item    my_path_item;
//   PathItemPtr  item(NoOpSharedPtr<path_item>(&my_path_item));
//
// When the shared_ptr deallocates the pointed-to path_item, the path_item
// won't actually be deleted.
//
// ---------------------------------------------------------------------------

// no-op deleter-function, used by NoOpSharedPtr
template <typename T>
void no_op(T *ptr)
{
}

template <typename T>
class NoOpSharedPtr : public std::tr1::shared_ptr<T>
{
public:
   typedef std::tr1::shared_ptr<T> BaseType;

   NoOpSharedPtr(T *ptr)
       : std::tr1::shared_ptr<T>(ptr, no_op<T>)
   { // construct with no-op deleter
   }
   operator BaseType &() { return *this; } // cast to std::tr1::shared_ptr<T>
};

// ---------------------------------------------------------------------------
// MallocSharedPtr
//
// This is a shared ptr to wrap around things that are allocated with
// malloc(), instead of new.  Thus, the deleter should call free().
// ---------------------------------------------------------------------------

template <typename T>
class MallocSharedPtr : public std::tr1::shared_ptr<T>
{
public:
   typedef std::tr1::shared_ptr<T> BaseType;

   MallocSharedPtr(T *ptr)
       : std::tr1::shared_ptr<T>(ptr, free)
   {
   }
   operator BaseType &() { return *this; } // cast to std::tr1::shared_ptr<T>
};

typedef MallocSharedPtr<char> CharPtr;

// ---------------------------------------------------------------------------
// POOL
//
// Pool<T> manages a vector of a specific class of objects (e.g. type
// PLFSPath, or PpOSIXPath).  The get() method returns objects from the pool
// (if there are any), or else newly-minted objects of the proper type.  We
// actually give you a shared_ptr to the object.  When your shared-ptr goes
// out of scope (or you call shared_ptr<T>::reset()), then the object will
// automatically be returned to the proper Pool.
//
// This is also used to allow Path objects to use dynamically-allocated
// path_item members, without having a lot of allocations.  Instead they
// can just get one from a Pool<path_item>.
// ---------------------------------------------------------------------------

template <typename T>
class Pool
{
public:
   // put an object of type T back into the pool.
   // This is the "deleter" method used by the shared_ptrs returned from get().
   static void put(T *t)
   {
      //      std::cout << "Pool<T>::put(" << t << ")" << std::endl;
      // t->~T();                  // explicit destructor-call to do clean-up
      *t = T(); // reset via operator=()
      _pool.push_back(t);
   }

   // if the pool is not empty, extract an item, otherwise create one
   static SharedPtr<T> get()
   {
      //      std::cout << "Pool<T>::get() -- pool size = " << _pool.size() << std::endl;
      if (_pool.size())
      {
         T *t = _pool.back();
         _pool.pop_back();
         //         std::cout << "Pool<T>::get() -- "
         //                   << "old = " << t << std::endl;
         return SharedPtr<T>(t, put); // use put(), as shared_ptr deleter-fn
      }
      else
      {
         T *t2 = new T();
         //         std::cout << "Pool<T>::get() -- new = " << t2 << std::endl;
         return SharedPtr<T>(t2, put); // use put(), as shared_ptr deleter-fn
      }
   }

protected:
   // per-class vectors hold the pool objects
   static std::vector<T *> _pool;
};

// ---------------------------------------------------------------------------
// PATH
//
// Abstract base-class, to be implemented by file-system-specific
// subclasses.
//
// PathBuffers are created via MPI_Pack'ing many paths together.  These are
// then sent from workers to manager, and vice versa.  The recipient needs
// to be able to determine the type of objects in a PathBuffer, and then to
// MPI_Unpack data to create objects of that type.  Approaches:
//
// (a) receiver reads a code from the beginning of a PathBuffer, and then
//     uses that to create objects (via a switch-stmt or a factory).  There
//     is a constructor that is implemented by all sub-classes, and
//     receiver calls this.
//
//     CON: what if a future subclass needs something not provided in this
//          generic constructor.
//
// (b) objects provide their own pack/unpack methods.  This could still use
//     a type-code at the front of the buffer, which is used to
//
//     CON: In the simplest impl, each object requires a leading code to be
//          embedded in the buffer, so that the generic factory can be
//          stateless.
//
// (c) each agent (manager or worker) could just know (e.g. from an initial
//     broadcast) the type of all objects it will receive.
//
//
// NOTE: All default-constructors (and all other constructors) should
//       probably be private.  This forces people to go through the
//       PathFactory, where we can assure that the proper subclass is
//       created, and can manage Pools, to avoid excessive
//       dynamic-allocation.
//
//       This means all subclasses should have Pool<T> as a friend.
//
//       PathFactory is friend of the base Path class, to get access to
//       factory_update(), which calls factory_update_list(), which is
//       implemented privately by any subclass that wants/needs updates at
//       "construction"-time, from the factory, to suplement the no-arg,
//       default copy-constructor.  (See e.g. PLFS_Path).
//
//
// --- PATH LIFE-CYCLE:
//
// It is important to understand the interactions between PathFactory,
// Pool<T>, and the Path classes.  pftool uses the PathFactory wherever
// Path classes are needed.  In order to eliminate the cost of
// dynamic-allocation, the factory uses Pool<T> everywhere, (where T is
// some Path subclass).  Thus, the factory reuses Path objects.
//
// The pool returns PathPtrs, and the underlying Path objects are
// initialized in the factory.  When these PathPtrs go out of scope in
// pftool, the associated object is returned to its pool. (The PathPtr
// "deleter" calls Pool<T>put(), which returns the object to the pool.)
//
// When objects are returned to their pool, they are not destructed
// (because there doesn't seem to be a way for a template to call the
// constructor of an existing object, when we want to pull it from the pool
// again).  Instead, the pool assigns a default-constructed object on top
// of it.  This assignment invokes Path::operator=(), which in turn calls
// subclass cleanup methods, via Path::close_all(), which some subclasses
// also override.
//
// The result is that Path-instance resources (e.g. open
// metadata-filehandles, etc) are cleaned up (e.g. closed) when a Path
// subclass object is returned to its respective Pool, via the following
// somewhat-obscure mechanism:
//
//    Pool<T>::put() -> Path::operator=(Path&) -> ... Path::close_all()
//
// ---------------------------------------------------------------------------

#define NO_IMPL(METHOD) unimplemented(__FILE__, __LINE__, (#METHOD))
#define NO_IMPL_STATIC(METHOD, CLASS) unimplemented_static(__FILE__, __LINE__, (#METHOD), (#CLASS))


class Path
{
protected:
   friend class PathFactory;

   // We're assuming all sub-classes can more-or-less fake a stat() call
   // (If they can't, then they can just ignore path_item::st)
   PathItemPtr _item;

   typedef uint16_t FlagType;
   FlagType _flags;

   static const FlagType FACTORY_DEFAULT = 0x0001; // default-constructed
   static const FlagType DID_STAT = 0x0002;        // already ran stat
   static const FlagType STAT_OK = 0x0004;         // stat-data is usable
   static const FlagType FOLLOW = 0x0008;          // e.g. use stat() instead of lstat()
   static const FlagType IS_OPEN = 0x0010;         // open() succeeded, not closed
   static const FlagType IS_OPEN_DIR = 0x0020;     // opendir() succeeded, not closed

   int _rc; // TBD: let user query failures, after the fact
   int _errno;

   //   //   // A number that is the same for all objects of this [sub-]class.
   //   //   // Oops.  No.  type_info::hash_code can also return the same value for
   //   //   // objects of different types.
   //   //   virtual size_t type_code() const { return typeid(*this).hash_code; }
   //   virtual size_t type_code() {
   //      static size_t   type_code = 0;
   //      SharedPtr<char> name = class_name(); // find sub-class name
   //      char*           code_ptr = (char*)&type_code;
   //      char*           name_ptr = name->get();
   //      strncpy(code_ptr, name_ptr, sizeof(type_code));
   //      return type_code;
   //   }

   // This could be useful as a way to allow methods to be optionally
   // defined in some configurations, without requiring #ifdefs around the
   // method signatures, etc.  For instance, methods that are only needed
   // for tape-access could be given signatures in this way, and run-time
   // would throw an error in cases where the configuration doesn't provide
   // an implementation.
   void unimplemented(const char *fname, int line_number,
                      const char *method_name) const
   {
      unimplemented_static(fname, line_number, method_name, this->class_name().get(), this);
   }

   static void unimplemented_static(const char *fname, int line_number,
                                    const char *method_name, const char *class_name,
                                    const Path *p)
   {
      std::cout << "file " << fname << ":" << line_number
                << " -- class '" << class_name << "'"
                << " " << p
                << " does not implement method '" << method_name << "'."
                << std::endl;
      std::cout << "It may be that this is supported with different configure options." << std::endl;
      MPI_Abort(MPI_COMM_WORLD, -1);
   }

   // Factory uses default constructors, then calls specific subclass
   // versions of factory_install().  We want the constructors to be
   // private, so only the factory (our friend) will do creations.  Each
   // subclass might have unique args, so factory_install() is not virtual.
   //
   // This is also the default constructor!
   //
   // Subclasses would fill in path_item.ftype statically, and could look-up
   // dest_ftype from the options struct.
   //
   // maybe someone calls stat to figure sub-class type, then gives it to us.
   // If you do give us a stat struct, we'll assume the stat call succeeded.
   Path()
       : _item(Pool<path_item>::get()), // get recycled path_item from the pool
         _flags(FACTORY_DEFAULT),       // i.e. path might be null
         _rc(0),
         _errno(0)
   {
      //      // not needed.  FACTORY_DEFAULT tells us <_item> is crap
      //      static const size_t path_offset = offsetof(path_item, path);
      //      memset((void*)_item.get(), path_offset +1, 0); // 1st char of path, too

      // constructor should only ever be called via subclass constructors,
      // which should only ever be called by the PathFactory. That implies
      // that PF already knows our ftype, and will be updating our _item
      // appropriately.  This is just here to help identify cases where
      // something didn't get done, same as FACTORY_DEFAULT.
      _item->ftype = TBD;
   }

   // Adopt a provided shared_ptr<path_item>.
   // This would allow a shallow copy. For example:
   //    path_item  item;
   //    Path       path(NoOpSharedPtr<path_item>(&item));
   // Or:
   //    PathPtr    path_ptr(PathFactory::create(NoOpSharedPtr<path_item>(&item)));
   //
   Path(const PathItemPtr &item)
       : _item(item),
         _flags(0),
         _rc(0),
         _errno(0)
   {
      if (item->path[0] &&
          (item->st.st_ino || item->st.st_mode || item->st.st_ctime))
         did_stat(true);
   }

   // This is how PathFactory initializes Path objects.
   //
   // If <item>.st has non-null fields, we infer that it is valid.  This
   // allows us to make a no-copy Path object corresponding to a path_item
   // sent over the wire, without having to run stat() again, to figure out
   // what it is.
   //
   virtual Path &operator=(const PathItemPtr &item)
   {
      // Make sure we're not replacing _item with an item that
      // should be owned by a different Path subclass
      if ((_item->ftype > TBD) &&
          (_item->ftype != item->ftype))
      {
         errsend_fmt(FATAL, "Attempt to change ftype during assignment '%s' = '%s'\n",
                     _item->path, item->path);
         return *this;
      }

      install_path_item(item);
      return *this;
   }

   virtual Path &operator=(const Path &path)
   {
      install_path_item(path._item);

      _flags = path._flags;
      _rc = path._rc;
      _errno = path._errno;

      return *this;
   }

   void install_path_item(const PathItemPtr &item)
   {

      // NOTE: this calls close_all()
      //       subclasses should override close_all(), to handle reset
      //       by Pool<T>, inside Factory.
      path_change_pre(); // subclasses may want to be informed

      _item = item; // returns old _item to Pool<path_item>
      _flags = 0;   // not a FACTORY_DEFAULT, if we were before
      _rc = 0;
      _errno = 0;

      // if it already has stat info, we'll take it for granted
      if (item->path[0] &&
          (item->st.st_ino || item->st.st_mode || item->st.st_ctime))
         did_stat(true);

      path_change_post(); // subclasses may want to be informed
   }

   // underlying path was renamed
   // stat-info is no longer valid
   void reset_path_item()
   {
      memset(&_item->st, 0, sizeof(struct stat));
      FlagType keeper_flags = (_flags & FOLLOW);

      install_path_item(_item); // orderly reset
      _flags |= keeper_flags;
   }

   // see factory_install_list()
   void factory_install(int count, ...)
   {
      va_list list;
      va_start(list, count);

      factory_install_list(count, list);

      va_end(list);
   }

   // easy generic way for subclasses to have a private method, with custom
   // signiture, accessible by the factory, without having to explicitly
   // make the factory a friend of every subclass.  NO_IMPL() informs us at
   // compile time, if the factory thinks a subclass wants initialization,
   // but subclass forgot to implement it.  If you get such a message, your
   // subclass needed to implement this method, and pull out the
   // subclass-specific arguments provided by the factory.
   virtual void factory_install_list(int count, va_list list)
   {
      NO_IMPL(factory_install_list);
   }

   // *** IMPORTANT: because we use Pools, objects are re-used without
   //     being destructed.  That means they must override this method, to
   //     perform any state-clearing operations, when an old object gotten
   //     from the Pool is about to be reused.
   //
   //     NOTE: don't forget to pass the call along to parent classes.

   virtual void close_all()
   {
      if (_flags & IS_OPEN)
         close();
      else if (_flags & IS_OPEN_DIR)
         closedir();
   }

   // Whenever the path changes, various local state may become invalid.
   // (e.g when someone turns on FOLLOW, the old lstat() results are
   // no-longer correct.)  Subclasses can intercept this to do local work,
   // in those cases.  Saves them having to override op=()
   virtual void path_change_pre()
   {
      close_all(); // operator=() depends on this
      _flags &= ~(DID_STAT | STAT_OK);
   }

   virtual void path_change_post() {}

   // this is called whenever there's a chance we might not have done a
   // stat.  If we already did a stat(), then this is a no-op.  Otherwise,
   // defer to subclass-specific impl of stat().
   bool do_stat(bool err_on_failure = true)
   {
      if (!(_flags & DID_STAT))
      {
         _flags &= ~(STAT_OK);

         bool success = do_stat_internal(); // defined by subclasses
         did_stat(success);

         // maybe flag success
         if (!success)
         {
#if 0
            // The stat member of <out_node>, in worker_comparelist() or
            // worker_copylist(), may have crap from earlier iterations.
            // This can fool Path::Path(const PathItemPtr&) or
            // Path::operator=() into thinking we've done a successful
            // stat, unless we wipe it here.
            //
            // COMMENTED OUT: Maybe better than this is to make sure that
            // pftool never calls with an uninitialized stat struct.  We did
            // that by fixing get_output_path() to zero out path_item->st.
            //
            memset(&_item->st, 0, sizeof(struct stat));
#endif
            if (err_on_failure)
            {
               errsend_fmt(NONFATAL, "Failed to stat path %s\n", _item->path);
            }
         }
      }

      return (_flags & STAT_OK);
   }

   // return true for success, false for failure.
   virtual bool do_stat_internal() = 0;

   void did_stat(bool stat_succeeded = true)
   {
      _flags &= ~(FACTORY_DEFAULT);
      _flags |= (DID_STAT);
      if (stat_succeeded)
      {
         _flags |= STAT_OK;

         //         _item->offset = 0;
         //         _item->length = _item->st.st_size;
      }
   }

   virtual void set(FlagType flag) { _flags |= flag; }
   virtual void unset(FlagType flag) { _flags &= ~(flag); }

public:
   // for updating multiple chunks at once.
   typedef struct
   {
      size_t index; // index of this chunk
      size_t size;  // data written to chunk
   } ChunkInfo;
   typedef std::vector<ChunkInfo> ChunkInfoVec;
   typedef std::vector<ChunkInfo>::iterator ChunkInfoVecIt;

   virtual ~Path()
   {
      close_all(); // Wrong.  close() is abstract in Path.
   }

   // demangled name of any subclass
   //
   // NOTE: abi::__cxa_demangle() returns a dynamically-allocated string,
   //       so we use a smart-pointer to make things easier on the caller.
   //
   // NOTE: This is allocated with malloc(), so we use a
   //       MallocSharedPtr<char> to invoke free(), rather than delete, at
   //       clean-up time.
   //
   virtual CharPtr class_name() const
   {
      int status;
      char *name = abi::__cxa_demangle(typeid(*this).name(), 0, 0, &status);
      return CharPtr(name);
   }

   //   // some command-line options might need adjustment, or rejection, by
   //   // some subclasses.  Make changes, if you want.  Return false if you
   //   // reject these options.  [This will be called on the destination
   //   // path only!]
   //   virtual bool adjust_options(struct options& o) {
   //      return true;
   //   }

   // opportunity to adjust the chunk-size that pftool is going to use,
   // with a given destination file, having size <file_size>.
   // Return negative for errors.
   virtual ssize_t chunksize(size_t file_size, size_t desired_chunk_size)
   {
      return desired_chunk_size;
   }

   virtual ssize_t chunk_at(size_t default_chunk_size)
   {
      return default_chunk_size;
   }

   // This replaces the obsolete approach of comparing inode-numbers.  That
   // doesn't work with object-storage, where there are no inodes, and all
   // objects have st.st_ino==0.  So instead, we'll assume that two objects
   // of different subclasses are by default never the same object.
   // Subclasses should override this, to compare specifically against
   // another member of the same subclass (or other subclasses they know
   // how to compare against).
   //
   // NOTE: Objects cant be const; they might have to call stat(), etc.
   //
   //   virtual bool operator==(Path& p) { return identical(p); }
   virtual bool identical(Path *p) { return false; }                 // same item, same FS
   virtual bool identical(PathPtr &p) { return identical(p.get()); } // same item, same FS
   //   virtual bool equivalent(Path& p) { return false; } // same size, perms, etc

   // allow subclasses to extend comparisons in pftool's samefile()
   virtual bool incomplete() { return false; }

   // Create a new Path with our path-name plus <suffix>.  Local object is
   // unchanged.  Result (via the factory) might be a different subclass
   // (e.g. descending into PLFS directory).  Let PathFactory sort it out,
   // using stat_item(), etc.
   virtual PathPtr path_append(char *suffix) const;
   virtual PathPtr path_truncate(ssize_t size) const;

   // don't let outsider change path without calling path_change()
   const char *const path() const { return (char *)_item->path; }

   // don't let outsider change stat without unsetting DID_STAT
   const struct stat &st()
   {
      do_stat();
      return _item->st;
   }

   // don't let outsider change path_item
   // const path_item&   item() const { return *_item; }
   const path_item &node() const { return *_item; }

   // As near as I can tell, dest_ftype is used as a local copy of the
   // ftype of the destination, in order to make this knowledge available
   // in places where the top-level dest-node is not available, such as
   // update_stats(), and copy_file().  It's assigned in stat_item(), and
   // then defaults are overridden in process_stat_buffer(), it seems.
   // This value has no influence on the which Path subclass should own a
   // given path_item (unlike ftype), so we allow it to be set and changed
   // as needed.
   FileType dest_ftype() const { return _item->dest_ftype; }
   void dest_ftype(FileType t) { _item->dest_ftype = t; }

   // ON SECOND THOUGHT:
   // virtual FileType ftype_for_destination() { REGULARFILE; } // subclasses do what they want

   FileType ftype() const { return _item->ftype; }

   // if you just want to know whether stat succeeded call this
   virtual bool stat() { return do_stat(false); }
   virtual bool exists() { return do_stat(false); } // just !ENOENT?

   // These are all stat-related, chosen to allow interpretation in the
   // context of non-POSIX sub-classes.  We assume all subclasses can
   // "fake" a struct stat.
   ///
   // CAREFUL: These also return false if the file doesn't exist!
   virtual bool is_link()
   {
      do_stat();
      return S_ISLNK(_item->st.st_mode);
   }
   virtual bool is_dir()
   {
      do_stat();
      return S_ISDIR(_item->st.st_mode);
   }

   virtual time_t ctime()
   {
      do_stat();
      return _item->st.st_ctime;
   }
   virtual time_t mtime()
   {
      do_stat();
      return _item->st.st_mtime;
   }
   virtual mode_t mode()
   {
      do_stat();
      return _item->st.st_mode;
   }
   virtual size_t size()
   {
      do_stat();
      return _item->st.st_size;
   }

   virtual bool is_open() { return (_flags & (IS_OPEN | IS_OPEN_DIR)); }

   // where applicable, stat() calls should see through symlinks.
   virtual void follow()
   {
      path_change_pre();
      _flags |= FOLLOW;
   }

   // try to adapt these POSIX calls
   virtual bool lchown(uid_t owner, gid_t group) = 0;
   virtual bool chmod(mode_t mode) = 0;
   virtual bool utime(const struct utimbuf *ut) = 0;
   virtual bool utimensat(const struct timespec times[2], int flags) = 0;

   // These are per-class qualities, that pftool may want to know
   virtual bool supports_n_to_1() const = 0; // can support N:1, via chunks?

   // This allows MARFS_Path to select the proper repo, based on total
   // file-size, so individual chunk-mover tasks can get info from the
   // xattrs.  It is called single-threaded from pftool, before copying
   // to a file.
   virtual bool pre_process(PathPtr src) { return true; } // default is no-op

   // Allow subclasses to maintain per-chunk state.  (e.g. MarFS can update
   // MD chunk-info).  This is only called during chunked COPY tasks (not
   // during chunked COMPARE tasks).
   virtual bool chunks_complete(ChunkInfoVec &vec) { return true; } // default is no-op

   // perform any class-specific initializations, after pftool copy has
   // finished.  For example, MARFS_Path can truncate to size It is called
   // single-threaded from pftool, when after all parallel activity is
   // done.
   virtual bool post_process(PathPtr src) { return true; } // default is no-op

   // when subclass operations fail (e.g. mkdir(), they save errno (or
   // whatever), and return false.  Caller can then come back and get the
   // corresponding error-string from here.
   virtual const char *const strerror() { return ::strerror(_errno); }
   virtual int get_errno() { return _errno; }
   virtual int get_rc() { return _rc; }

   virtual int set_error(int rc, int err_no)
   {
      _rc = rc;
      _errno = err_no;
      return 0;
   }

   // like POSIX access().  Return true if accessible in given mode
   virtual bool access(int mode) = 0;

   // like POSIX faccessat(). We assume path is never relative, so no <dirfd>
   virtual bool faccessat(int mode, int flags) = 0;

   // open/close do not return file-descriptors, like POSIX open/close do.
   // You don't need those.  You just open a Path, then read from it, then
   // close it.  The boolean tells you whether you succeeded.  (Consider
   // that, for some Path sub-classes, there is never a "file descriptor"
   // they could return.)  Later, we'll give access to the error-status
   // which we are keeping track of.
   //
   virtual bool open(int flags, mode_t mode) = 0; // non-POSIX will have to interpret
   virtual bool close() = 0;

   // This allows MARFS_Path to support N-to-1, for pftool.
   virtual bool open(int flags, mode_t mode, size_t offset, size_t length)
   {
      return open(flags, mode); // default is to ignore <offset> and <length>
   }

   // read/write to/from caller's buffer
   virtual ssize_t read(char *buf, size_t count, off_t offset) = 0;  // e.g. pread()
   virtual ssize_t write(char *buf, size_t count, off_t offset) = 0; // e.g. pwrite()

   // get the realpath of the path
   virtual char *realpath(char *resolved_path) = 0;

   // return false only to indicate errors (e.g. not end-of-data for readdir())
   // At EOF, readdir() returns true, but sets path[0] == 0.
   virtual bool opendir() = 0;
   virtual bool closedir() = 0;
   virtual bool readdir(char *path, size_t size) = 0;
   virtual bool mkdir(mode_t mode) = 0;

   // delete the file/object
   virtual bool remove() = 0;
   virtual bool unlink() = 0;

   virtual ssize_t readlink(char *buf, size_t bufsiz)
   {
      _errno = 0;
      return -1;
   }
   virtual bool symlink(const char *link_name)
   {
      _errno = 0;
      return false;
   }

   //all additional functions needed for renaming, creating temp files, etc
   virtual int check_packable(size_t length) { return 0; }
   virtual int get_packable() { return _item->packable; }

   // managing time-stamps (for temporary dest-pathname)
   virtual char *get_timestamp() { return _item->timestamp; }

   // In the case of Path::op=(), and Path::install_path_item(), the path
   // being installed is compatible with the specific Path sub-class,
   // because everything is being done by the PathFactory, which has
   // already created the proper Path sub-class.  But in the case of a
   // rename, we can't assume that <new_path> is consistent with what we
   // were.
   //
   // Two possible approaches: (a) we remain as the old path; we do the
   // rename, but we still represent the old path.  Thus, our path_item
   // doesn't change.  However, in the event of success, we should probably
   // invalidate our stat-info, etc.  (b) caller thinks we are now the new
   // path.  That can't work without some fanciness that is probably not
   // worth implmenting.  So, well go with (a).
   virtual bool rename(const char *new_path) = 0;

   // // unused (and undefined?).  pftool uses the function-version in pfutils.cpp
   // PathPtr         get_output_path(path_item src_node, path_item dest_node, struct options o);

   // fstype is apparently only used to distinguish panfs from everything else.
   static FSType parse_fstype(const char *token) { return (strcmp(token, "panfs") ? UNKNOWN_FS : PAN_FS); }
   const char *fstype_to_str() { return ((_item->fstype == PAN_FS) ? "panfs" : "unknown"); }
};

typedef std::queue<PathPtr> PathQueue;

typedef std::vector<PathPtr> PathVec;
typedef std::vector<PathPtr>::iterator PathVecIt;
typedef std::vector<PathPtr>::const_iterator PathVecConstIt;

// ---------------------------------------------------------------------------
// SYNTHETIC DATA SOURCE
//
// This is a source-side way to create artificial data. Synthetic data
// doesn't actually come from a file.  (see syndata.*)
//
// This is mainly used in copy_file().
// ---------------------------------------------------------------------------

#ifdef GEN_SYNDATA
#include "Path-syndata.h"
#endif

// ---------------------------------------------------------------------------
// POSIX
//
// Because the POSIX mindset is so deeply ingrained in pftool, the base
// Path class has a lot of "POSIX" type interfaces, like the notion that
// path metadata can be represented in a stat struct, or that there are
// symlinks, and directories, etc.
//
// The meaning of the POSIX sub-class is really just that we use specific
// library calls (e.g. lstat()) to do the work.  One thing we do add herre
// is the notion that files need to be opened before access, and closed
// after.
//
// By default, we do not follow symlinks.  Call Path::follow() if you want
// following.
// ---------------------------------------------------------------------------

class POSIX_Path : public Path
{
protected:
   friend class Pool<POSIX_Path>;

   int _fd;    // after open()
   DIR *_dirp; // after opendir()

   // FUSE_CHUNKER seems to be the only one that uses stat() instead of lstat()
   virtual bool do_stat_internal()
   {
      _errno = 0;

      // run appropriate POSIX stat function
      if (_flags & FOLLOW)
         _rc = ::stat(_item->path, &_item->st);
      else
         _rc = lstat(_item->path, &_item->st);

      if (_rc)
      {
         _errno = errno;
         return false;
      }

      // couldn't we just look at S_ISLNK(_item->st.st_mode), when we want to know?
      _item->ftype = REGULARFILE;

      return true;
   }

   // private.  Use PathFactory to create paths.
   POSIX_Path()
       : Path(),
         _fd(0),
         _dirp(NULL)
   {
   }

public:
   virtual ~POSIX_Path()
   {
      close_all(); // see Path::operator=()
   }

   //   virtual bool operator==(POSIX_Path& p) { return (st().st_ino == p.st().st_ino); }
   virtual bool identical(Path *p)
   {
      POSIX_Path *p2 = dynamic_cast<POSIX_Path *>(p);
      return (p2 &&
              p2->exists() &&
              (st().st_ino == p2->st().st_ino));
   }

   virtual bool supports_n_to_1() const
   {
      return false;
   }

   //   virtual int    mpi_pack() { NO_IMPL(mpi_pack); } // TBD

   virtual const char *const strerror()
   {
      return ::strerror(_errno);
   }

   virtual bool lchown(uid_t owner, gid_t group)
   {
      if (_rc = ::lchown(path(), owner, group))
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool chmod(mode_t mode)
   {
      if (_rc = ::chmod(path(), mode))
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   // WARNING: This follows links.  Reimplement with lutimes().  Meanwhile, use utimensat()
   virtual bool utime(const struct utimbuf *ut)
   {
      if (_rc = ::utime(path(), ut))
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool utimensat(const struct timespec times[2], int flags)
   {
      if (_rc = ::utimensat(AT_FDCWD, path(), times, flags))
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }

   virtual bool access(int mode)
   {
      if (_rc = ::access(path(), mode))
         _errno = errno;
      return (_rc == 0);
   }
   // path must not be relative
   virtual bool faccessat(int mode, int flags)
   {
      if (_rc = ::faccessat(-1, path(), mode, flags))
         _errno = errno;
      return (_rc == 0);
   }

   // see comments at Path::rename()
   virtual bool rename(const char *new_path)
   {
      if (_rc = ::rename(_item->path, new_path))
         _errno = errno;
      else
         reset_path_item();

      return (_rc == 0);
   }

   // see comments at Path::open()
   // NOTE: We don't protect user from calling open when already open
   virtual bool open(int flags, mode_t mode)
   {
      _fd = ::open((char *)_item->path, flags, mode);
      if (_fd < 0)
      {
         _rc = _fd;
         _errno = errno;
         return false; // return _fd;
      }
      set(IS_OPEN);
      return true; // return _fd;
   }
   virtual bool opendir()
   {
      do_stat_internal(); // this fixes a Lustre issue when server access upcall isn't set
      _dirp = ::opendir(_item->path);
      if (!bool(_dirp))
      {
         _errno = errno;
         return false; // return _rc;
      }
      set(IS_OPEN_DIR);
      return true;
   }

   // NOTE: We don't protect user from calling close when already closed
   virtual bool close()
   {
      _rc = ::close(_fd);
      if (_rc < 0)
      {
         _errno = errno;
         return false; // return _rc;
      }
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN);
      return true; // return _fd;
   }
   virtual bool closedir()
   {
      _rc = ::closedir(_dirp);
      if (_rc < 0)
      {
         _errno = errno;
         return false;
      }
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN_DIR);
      return true;
   }

   virtual char *realpath(char *resolved_path)
   {
      // GRANSOM -- I've edited this behavior to avoid performing a realpath() of the full target,
      //            and instead only realpath the parent of the target file/dir.  This is to allow 
      //            pftool to properly ( in my opinion ) copy symlink sources, recreating the link 
      //            at the destination path rather than copying the target of the link to that path.
      // duplicate the path, so we can modify
      char* duppath = strdup( _item->path );
      if ( duppath == NULL ) {
         _errno = errno;
         return NULL;
      }
      // parse over the path, identifying transition from parent path to FS entry
      char* childref = NULL;
      char* pathparse = duppath;
      while ( pathparse  &&  *pathparse != '\0' ) {
         if ( *pathparse == '/' ) {
            while ( *pathparse == '/' ) {
               pathparse++;
            }
            if ( *pathparse != '\0' ) {
               childref = pathparse; // note start of next path component
            }
         }
	 if ( *pathparse != '\0' )
            pathparse++;
      }
      // prepare to generate the final path
      char *ret = NULL;
      // check for empty path or root path degenerate case
      if ( childref == NULL ) {
         // just realpath the full thing
         ret = ::realpath(_item->path, resolved_path);
         if (NULL == ret)
         {
            _errno = errno;
         }
      }
      else {
         // check if we have a parent path
         if ( childref - 1 != duppath ) {
            // trim our duplicated path, leaving only the parent path at the head
            *(childref - 1) = '\0';
            // only realpath the parent path
            ret = ::realpath(duppath, resolved_path);
         }
         else {
            // no parent path here implies a path of "/<tgt>" format
            ret = ::realpath("/", resolved_path);
         }
         if ( ret == NULL ) {
            free( duppath );
            _errno = errno;
            return NULL;
         }
         size_t rppathlen = strlen( ret );
         if ( *(ret + rppathlen - 1) == '/' ) { rppathlen--; } // overwrite any trailing '/', if present
         // we know the allocated str is PATH_MAX bytes, so we can safely append
         snprintf( ret + rppathlen, PATH_MAX - rppathlen, "/%s", childref );
      }
      free( duppath );
      //printf( "REALPATH: %s\n  RET: %s\n", _item->path, ret );
      return ret;
   }

   virtual ssize_t
   read(char *buf, size_t count, off_t offset)
   {
      ssize_t bytes = pread(_fd, buf, count, offset);
      if (bytes == (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }

   virtual bool readdir(char *path, size_t size)
   {
      errno = 0;
      if (size)
         path[0] = 0;

      struct dirent *d = ::readdir(_dirp);
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      if (d != NULL)
      {
         strncpy(path, d->d_name, size);
      }
      else
      {
         _errno = errno;
         return bool(_errno == 0);
      }
      return true;
   }

   virtual ssize_t write(char *buf, size_t count, off_t offset)
   {
      ssize_t bytes = pwrite(_fd, buf, count, offset);
      if (bytes == (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }
   virtual bool mkdir(mode_t mode)
   {
      if (_rc = ::mkdir(_item->path, mode))
      {
         _errno = errno;
      }
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }

   virtual bool remove()
   {
      return unlink();
   }
   virtual bool unlink()
   {
      if (_rc = ::unlink(_item->path))
         _errno = errno;
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }

   // WARNING: this behaves like POSIX readlink(), not writing final '\0'
   virtual ssize_t readlink(char *buf, size_t bufsiz)
   {
      ssize_t count = ::readlink(_item->path, buf, bufsiz);
      if (-1 == count)
      {
         _rc = -1; // we need an _rc_ssize
         _errno = errno;
      }
      return count;
   }

   virtual bool symlink(const char *link_name)
   {
      if (_rc = ::symlink(link_name, _item->path))
      {
         _errno = errno;
      }
      unset(DID_STAT); // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
};

// ---------------------------------------------------------------------------
// NULL file/dir
//
// This is being introduced to serve a simple purpose.  We want to give the
// appearance of a sort of "/dev/null directory tree", to allow pftool to
// show read BW in the case where the only available destination
// file-system is slow (GPFS), and there is insufficient tempfs space to
// allow that to be used instead.  In this case, this NULL class treats all
// writes (and mkdir, etc) as no-ops, so pftool should only be constrained
// by read BW.
//
// TBD: It looks like you could also use this in the opposite case, as a
// sort of super-fast "/dev/zero directory tree", for reads.  But that's
// harder, because you don't actually have a file-tree here.  What we could
// do is wrap an existing directory tree, return stats, and readdir(), etc,
// from that tree, and have zero-cost reads.
// ---------------------------------------------------------------------------

class NULL_Path : public Path
{
protected:
   friend class Pool<NULL_Path>;

   bool _is_dir;

   // should we figure out _is_dir here?  [Impossible]
   // virtual void path_change_post() { }

   // pftool is going to expect a real stat struct (becuase we haven't
   // converted it to use Path::is_dir(), etc, everywhere).  We'll stat
   // either /dev/null, or /dev, depending on whether pftool thinks this is
   // a directory or not.  The only reason it would think this is a
   // directory is if it just called NULL_Path::mkdir() on it.
   virtual bool do_stat_internal()
   {
      _errno = 0;

      // run appropriate POSIX stat function
      if (_is_dir)
         _rc = lstat("/dev", &_item->st);
      else
         _rc = lstat("/dev/null", &_item->st);

      if (_rc)
      {
         _errno = errno;
         return false;
      }

      // couldn't we just look at S_ISLNK(_item->st.st_mode), when we want to know?
      if (_is_dir)
         _item->ftype = NULLDIR;
      else
         _item->ftype = NULLFILE;

      return true;
   }

   // private.  Use PathFactory to create paths.
   NULL_Path()
       : Path(),
         _is_dir(0)
   {
   }

public:
   virtual ~NULL_Path()
   {
      close_all(); // see Path::operator=()
   }

   //   virtual bool operator==(NULL_Path& p) { return (st().st_ino == p.st().st_ino); }
   virtual bool identical(Path *p)
   {
      NULL_Path *p2 = dynamic_cast<NULL_Path *>(p);
      return (p2 &&
              p2->exists() &&
              (st().st_ino == p2->st().st_ino));
   }

   virtual bool supports_n_to_1() const
   {
      return false;
   }

   // pftool uses the blocksize of the destination.  Match
   // MARFS_File::blocksize() [given that our MarFS repos are typically
   // configured with blocksize=1GiB], so that reading from marfs to
   // /dev/null/ will pull from MarFS with the same blocksize used for
   // writing to MarFS.  This is a decent setup for benchmarks, where
   // reading with bs=10G has poor load-balancing for small-ish files.
   virtual ssize_t chunksize(size_t file_size, size_t desired_chunk_size)
   {
      return desired_chunk_size; // NULL path doesn't actually care about chunk size, so return the default
   }

   //   virtual int    mpi_pack() { NO_IMPL(mpi_pack); } // TBD

   virtual const char *const strerror()
   {
      return ::strerror(_errno);
   }

   virtual bool lchown(uid_t owner, gid_t group)
   {
      return true;
   }
   virtual bool chmod(mode_t mode)
   {
      return true;
   }
   virtual bool utime(const struct utimbuf *ut)
   {
      return true;
   }
   virtual bool utimensat(const struct timespec times[2], int flags)
   {
      return true;
   }
   virtual bool access(int mode)
   {
      return (mode & R_OK);
   }
   virtual bool faccessat(int mode, int flags)
   {
      return (mode & R_OK);
   }

   virtual bool open(int flags, mode_t mode)
   {
      return true;
   }
   virtual bool opendir()
   {
      return true;
   }

   virtual bool close()
   {
      return true;
   }
   virtual bool closedir()
   {
      return true;
   }

   virtual char *realpath(char *resolved_path)
   {
      if (NULL == resolved_path)
      {
         resolved_path = (char *)malloc(strlen(_item->path) + 1);
         if (NULL == resolved_path)
         {
            _errno = errno;
            return NULL;
         }
      }

      strcpy(resolved_path, _item->path);
      return resolved_path;
   }

   virtual ssize_t read(char *buf, size_t count, off_t offset)
   {
      return count;
   }
   virtual bool readdir(char *path, size_t size)
   {
      if (size)
         path[0] = 0;
      return true;
   }

   virtual ssize_t write(char *buf, size_t count, off_t offset)
   {
      return count;
   }
   virtual bool mkdir(mode_t mode)
   {
      _is_dir = true;
      return true;
   }

   virtual bool remove()
   {
      return true;
   }
   virtual bool unlink()
   {
      return true;
   }

   virtual bool symlink(const char *link_name)
   {
      return false;
   }

   virtual bool rename(const char *new_path)
   {
      return true;
   }
};

// ---------------------------------------------------------------------------
// MARFS
//
// ---------------------------------------------------------------------------

#ifdef MARFS

#include <linux/limits.h>


extern marfs_fhandle marfsCreateStream;       // stream for 'packing' created files on a MarFS dest
extern marfs_fhandle marfsSourceReadStream;   // stream for reading from a MarFS source
extern marfs_fhandle marfsDestReadStream;     // stream for reading from a MarFS dest ( i.e. pfcm )
extern marfs_ctxt    marfsctxt;
extern char          marfs_ctag_set;

int initialize_marfs_context( void );


class MARFS_Path : public Path
{
protected:
   friend class Pool<MARFS_Path>;

   marfs_fhandle fh;
   marfs_dhandle dh;

   bool _parallel;
   bool _packed;

   off_t _offset;

   virtual bool do_stat_internal()
   {
      _errno = 0;

      _rc = marfs_stat(marfsctxt, path(), &_item->st, AT_SYMLINK_NOFOLLOW);

      if (_rc)
      {
         _errno = errno;
         return false;
      }
      _item->ftype = MARFSFILE;

      return true;
   }

   MARFS_Path()
       : Path(),
         fh(NULL),
         dh(NULL),
         _parallel(false),
         _packed(false),
         _offset(0)
   {

      unset(DID_STAT);
      unset(IS_OPEN_DIR);
      unset(IS_OPEN);
   }

public:
   virtual const char *const strerror()
   {
      return ::strerror(_errno);
   }

   virtual ~MARFS_Path()
   {
      close_all(); // see Path::operator=()
   }

   virtual void close_all()
   {
      Path::close_all();

      if (fh  &&  fh != marfsCreateStream  &&  fh != marfsSourceReadStream  &&  fh != marfsDestReadStream)
      {
         marfs_release(fh);
         fh = NULL;
      }

      if (dh)
      {
         marfs_closedir(dh);
      }
   }

   // closes the underlying fh stream for packed files
   static bool close_packedfh()
   {
      //printf("rank %d close_fh calling subp\n", MARFS_Path::_rank);
      bool retval = true;
      if (marfsCreateStream)
      {
         if ( marfs_close(marfsCreateStream) ) {
            retval = false;
         }
         marfsCreateStream = NULL;
      }
      if ( marfsSourceReadStream ) {
         if ( marfs_release(marfsSourceReadStream) ) {
            retval = false;
         }
         marfsSourceReadStream = NULL;
      }
      if ( marfsDestReadStream ) {
         if ( marfs_release(marfsDestReadStream) ) {
            retval = false;
         }
         marfsDestReadStream = NULL;
      }

      return retval;
   }

   virtual ssize_t chunksize(size_t file_size, size_t desired_chunk_size)
   {
      off_t offset;
      size_t size = 0;

      // possibly open a new marfs_fhandle
      char release = 0;
      if ( fh == NULL ) {
         fh = marfs_open( marfsctxt, NULL, path(), O_WRONLY );
         if ( fh == NULL ) {
            _errno = errno;
            _rc = -1;
            return 0;
         }
         release = 1;
      }
      if (marfs_chunkbounds(fh, 0, &offset, &size))
      {
         _rc = -1;
         _errno = errno;
         return 0;
      }
      if ( release ) {
         if (  marfs_release(fh) )
            if ( _rc == 0 ) { _errno = errno; _rc = -1; }
         fh = NULL;
      }

      return (size) ? size : desired_chunk_size; // if we got a real chunksize of zero, just return the default instead
   }

   // Two MarFS files are "the exact same file" if they have the same
   // inode, in the same namespace-shard, of the same namespace.  For now,
   // we're just going to say it depends on whether they have identical
   // inodes.
   virtual bool identical(Path *p)
   {
      MARFS_Path *p2 = dynamic_cast<MARFS_Path *>(p);
      return (p2 &&
              p2->exists() &&
              (st().st_ino == p2->st().st_ino));
   }

   virtual bool incomplete()
   {
      return false;
   }

   // pftool calls this when it knows the total size of the source-file
   // that is going to be copied to a MarFS destination (i.e. to us), and
   // knows the file is going to be treated as N:1.  The individual opens
   // and writes may use smaller sizes (because we now support N:1 writes).
   // This is only called once per destination, and is called before any
   // other writes to the file.
   virtual bool pre_process(PathPtr src)
   {
      marfs_fhandle handle = marfs_creat(marfsctxt, NULL, path(), 0600);
      if (handle == NULL)
      {
         _rc = -1;
         _errno = errno;
         return false;
      }

      if ( (_rc = marfs_extend(handle, src->st().st_size)) )
      {
         _errno = errno;
         marfs_release(handle); // don't leak our handle reference
         return false;
      }

      if ( (_rc = marfs_release(handle)) )
      {
         _errno = errno;
         return false;
      }

      return true;
   }

   // If opened N:1, this is our chance to reconcile things that parallel
   // writers couldn't do without locking. This is an opportunity to do
   // single-threaded reconciliation of all these details, after close().
   virtual bool post_process(PathPtr src)
   {
      _rc = 0; // assume success
      if ( !(_packed) ) {
         marfs_fhandle handle = marfs_open(marfsctxt, NULL, path(), O_WRONLY);
         if (handle == NULL)
         {
            _rc = -1;
            _errno = errno;
            return false;
         }

         if (_rc = marfs_close(handle))
         {
            _errno = errno;
            return false;
         }
      }

      return true;
   }

   virtual bool supports_n_to_1() const
   {
      return true;
   }

   virtual bool lchown(uid_t owner, gid_t group)
   {
      if (_rc = marfs_chown(marfsctxt, path(), owner, group, AT_SYMLINK_NOFOLLOW))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool chmod(mode_t mode)
   {
      if (_rc = marfs_chmod(marfsctxt, path(), mode, 0))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool utime(const struct utimbuf *ut)
   {
      struct timespec times[2];
      times[0] = {.tv_sec = ut->actime, .tv_nsec = 0};
      times[1] = {.tv_sec = ut->modtime, .tv_nsec = 0};

      if (_rc = utimensat(times, 0))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool utimensat(const struct timespec times[2], int flags)
   {

      // possibly open a new marfs_fhandle
      char release = 0;
      if ( fh == NULL ) {
         fh = marfs_open( marfsctxt, NULL, path(), O_WRONLY );
         if ( fh == NULL ) {
            _errno = errno;
            _rc = -1;
            return false;
         }
         release = 1;
      }
      if (_rc = marfs_futimens(fh, times))
      {
         _errno = errno;
      }
      if ( release ) {
         if (  marfs_release(fh) )
            if ( _rc == 0 ) { _errno = errno; _rc = -1; }
         fh = NULL;
      }

      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool access(int mode)
   {
      if (_rc = marfs_access(marfsctxt, path(), mode, 0))
      {
         _errno = errno;
      }
      //unset(DID_STAT); #gransom edit, unsure why this is here
      return (_rc == 0);
   }

   // path must not be relative
   virtual bool faccessat(int mode, int flags)
   {
      if (_rc = marfs_access(marfsctxt, path(), mode, flags))
      {
         _errno = errno;
      }
      //unset(DID_STAT); #gransom edit, unsure why this is here
      return (_rc == 0);
   }

   // This is what allows N:1 writes (i.e. concurrent writers to the same
   // MarFS file).  Caller takes responsibility to assure that all writes
   // will be at object-boundaries.  The offset is the logical-offset in
   // the user's data-stream (e.g. not accounting for recovery-info).
   // Thus, caller must know the "logical chunksize", which we provide via
   // chunksize().
   //
   // marfs_close() can not properly synchronize the xattrs, without some
   // kind of locking, which we don't want to impose.  Instead, caller can
   // reconcile xattrs, MD file-size, etc, by calling
   // e.g. MARFS_Path::post_process(), which is called single-threaded,
   // after individual writers have all called marfs_close()
   virtual bool open(int flags, mode_t mode, size_t offset, size_t length)
   {
      return open(flags, mode);
   }

   virtual bool open(int flags, mode_t mode)
   {
      if (flags & O_CONCURRENT_WRITE)
      {
         // should only ever have O_CONCURRENT_WRITE and O_WRONLY
         fh = marfs_open(marfsctxt, NULL, path(), O_WRONLY);
         _parallel = true;
         _packed = false;
      }
      else if (flags & O_CREAT && !exists())
      {
         // should only ever have O_CREAT and O_WRONLY
         fh = marfs_creat(marfsctxt, marfsCreateStream, path(), mode);
         if ( fh )
            marfsCreateStream = fh;
         _packed = true;
         _parallel = false;
      }
      else if (flags & O_WRONLY)
      {
         /* I'm not sure if this case shouldn't be lumped in with the previous
         case. I'm adding this as a separate case to preserve the POSIX-like
         functionality (O_CREAT is ignored if the file already exists). Maybe
         this should disappear and the !exists() condition should be removed
         above.*/
         fh = marfs_open(marfsctxt, NULL, path(), O_WRONLY);
         _parallel = true;
         _packed = false;
      }
      else
      {
         if (flags & O_SOURCE_PATH) {
            fh = marfs_open(marfsctxt, marfsSourceReadStream, path(), O_RDONLY);
            if ( fh )
               marfsSourceReadStream = fh;
         }
         else if (flags & O_DEST_PATH) {
            fh = marfs_open(marfsctxt, marfsDestReadStream, path(), O_RDONLY);
            if ( fh )
               marfsDestReadStream = fh;
         }
         else {
            fh = marfs_open(marfsctxt, NULL, path(), O_RDONLY);
         }
         _parallel = false;
         _packed = false;
      }

      if (!fh)
      {
         _rc = -1;
         _errno = errno;
         if ( errno == EBADFD ) {
            // our stream has been unrecoverably broken
            if ( _packed ) {
               // abandon the create stream
               marfs_release( marfsCreateStream );
               marfsCreateStream = NULL;
            }
            else if ( _parallel = false ) {
               // should only be true for a read stream
               if ( flags & O_SOURCE_PATH ) {
                  // abandon our source read stream
                  marfs_release( marfsSourceReadStream );
                  marfsSourceReadStream = NULL;
               }
               else if ( flags & O_DEST_PATH ) {
                  // abandon our dest read stream
                  marfs_release( marfsDestReadStream );
                  marfsDestReadStream = NULL;
               }
            }
            // any fallthrough from the above cases should be for 'fresh' streams, which can be ignored
            //    ( those created from a NULL ref, rather than an existing stream )
         }
         _parallel = false;
         _packed = false;
         return false;
      }
      _offset = 0;
      set(IS_OPEN);
      return true;
   }

   virtual bool opendir()
   {
      dh = marfs_opendir(marfsctxt, path());
      if (!dh)
      {
         _rc = -1;
         _errno = errno;
         return false;
      }
      set(IS_OPEN_DIR);
      return true;
   }

   virtual bool close()
   {
      if ( fh != marfsCreateStream  &&  fh != marfsSourceReadStream  &&  fh != marfsDestReadStream ) {
         _rc = marfs_release(fh);
         fh = NULL;
         if ( _rc )
         {
            _errno = errno;
            return false;
         }
         fh = NULL;
         _parallel = false;
         _packed = false;
      }
      unset(DID_STAT);
      unset(IS_OPEN);
      return true;
   }

   virtual int check_packable(size_t length)
   {
      return 1;
   }

   // see comments at Path::rename()
   virtual bool rename(const char *new_path)
   {
      if (_rc = marfs_rename(marfsctxt, path(), new_path))
      {
         _errno = errno;
      }
      else
      {
         reset_path_item();
      }
      return (_rc == 0);
   }

   virtual bool closedir()
   {
      if (_rc = marfs_closedir(dh))
      {
         _errno = errno;
         return false;
      }
      dh = NULL;
      unset(DID_STAT);
      unset(IS_OPEN_DIR);
      return true;
   }

   virtual char *realpath(char *resolved_path)
   {
      if (NULL == resolved_path)
      {
         resolved_path = (char *)malloc(strlen(_item->path) + 1);
         if (NULL == resolved_path)
         {
            _rc = -1;
            _errno = errno;
            return NULL;
         }
      }

      strcpy(resolved_path, _item->path);
      return resolved_path;
   }

   virtual ssize_t read(char *buf, size_t count, off_t offset)
   {
      if (_offset != offset) {
         off_t newoffset = marfs_seek(fh, offset, SEEK_SET);
         if ( newoffset != offset )
         {
            // even if offset is unexpected, record the resulting value if it makes any sense at all
            if ( newoffset >= 0 ) { _offset = newoffset; }
            _errno = errno;
            if ( errno == EBADFD ) {
               // our stream has been unrecoverably broken
               if ( fh == marfsCreateStream ) { marfsCreateStream = NULL; }
               if ( fh == marfsSourceReadStream ) { marfsSourceReadStream = NULL; }
               if ( fh == marfsDestReadStream ) { marfsDestReadStream = NULL; }
               marfs_release(fh);
               fh = NULL;
            }
            return false;
         }
         _offset = offset;
      }

      ssize_t bytes = marfs_read(fh, buf, count);
      if (bytes == (ssize_t)-1)
      {
         _errno = errno;
         return false;
      }
      _offset += bytes;
      unset(DID_STAT);
      return bytes;
   }

   // TBD: See opendir()
   virtual bool readdir(char *path, size_t size)
   {
      errno = 0;
      if (size)
      {
         path[0] = 0;
      }
      struct dirent *d = marfs_readdir(dh);
      unset(DID_STAT);
      if (d != NULL)
      {
         strncpy(path, d->d_name, size);
      }
      else
      {
         _errno = errno;
         return bool(_errno == 0);
      }
      return true;
   }

   virtual ssize_t write(char *buf, size_t count, off_t offset)
   {
      if (_offset != offset) {
         off_t newoffset = marfs_seek(fh, offset, SEEK_SET);
         if ( newoffset != offset )
         {
            // even if offset is unexpected, record the resulting value if it makes any sense at all
            if ( newoffset >= 0 ) { _offset = newoffset; }
            _errno = errno;
            if ( errno == EBADFD ) {
               // our stream has been unrecoverably broken
               if ( fh == marfsCreateStream ) { marfsCreateStream = NULL; }
               if ( fh == marfsSourceReadStream ) { marfsSourceReadStream = NULL; }
               if ( fh == marfsDestReadStream ) { marfsDestReadStream = NULL; }
               marfs_release(fh);
               fh = NULL;
            }
            return -1;
         }
         _offset = offset;
      }

      ssize_t bytes = marfs_write(fh, buf, count);
      if (bytes != count)
      {
         _errno = errno;
         if ( errno == EBADFD ) {
            // our stream has been unrecoverably broken
            if ( fh == marfsCreateStream ) { marfsCreateStream = NULL; }
            if ( fh == marfsSourceReadStream ) { marfsSourceReadStream = NULL; }
            if ( fh == marfsDestReadStream ) { marfsDestReadStream = NULL; }
            marfs_release(fh);
            fh = NULL;
         }
      }
      if ( bytes > 0 ) { _offset += bytes; }
      unset(DID_STAT);
      return bytes;
   }

   virtual bool mkdir(mode_t mode)
   {
      if (_rc = marfs_mkdir(marfsctxt, path(), mode))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool unlink()
   {
      if (_rc = marfs_unlink(marfsctxt, path()))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   virtual bool remove()
   {
      return unlink();
   }

   // marfs_readlink(), unlike POSIX readlink(), does currently add final '\0'
   virtual ssize_t readlink(char *buf, size_t bufsiz)
   {
      ssize_t count = marfs_readlink(marfsctxt, path(), buf, bufsiz);
      if (-1 == count)
      {
         _rc = -1; // we need an _rc_ssize
         _errno = errno;
      }
      return count;
   }

   virtual bool symlink(const char *link_name)
   {
      if (_rc = marfs_symlink(marfsctxt, link_name, path()))
      {
         _errno = errno;
      }
      unset(DID_STAT);
      return (_rc == 0);
   }

   static int lstat(char *path, struct stat *buf)
   {
      return marfs_stat(marfsctxt, path, buf, AT_SYMLINK_NOFOLLOW);
   }
};

#endif

// ---------------------------------------------------------------------------
// PathFactory
//
// If you give us a raw path-name, we'll parse it, and run a series of
// attempts to guess what it is, then return an appropriate Path sub-class.
//
// What if you just have a path_item (e.g. because the user gave you that),
// instead of a Path object?  You don't know what type of object should be
// created.  This static factory object can create you an appropriate Path
// object.  Use it like this:
//
//     PathPtr path = PathFactory::create(&my_path_item);
//
// Better yet:
//
//     PathPtr path(PathFactory::create(&my_path_item));
//
// ---------------------------------------------------------------------------

// template<typename T>  class PathPtr; // fwd decl

class PathFactory
{
protected:
   static uint8_t _flags;
   static const uint8_t INIT = 0x01;

   static struct options *_opts;
   static pid_t _pid;   // for PLFS
   static int _rank;    // for PLFS, MARFS
   static int _n_ranks; // MARFS (maybe)

public:
   // pid and rank are needed for PLFS.  Because the main program always
   // has them, we expect main to call this once, after rank and PID are
   // known, before constructing any paths.  Shouldn't be a hardship for
   // non-PLFS builds.
   //
   // NOTE: Do not call this more than once, because we reset the flags
   // here.
   static void initialize(struct options *opts,
                          int rank,
                          int n_ranks,
                          const char *src_path,
                          const char *dest_path)
   {
      _flags = 0;
      _opts = opts;
      _pid = getpid();
      _rank = rank;
      _n_ranks = n_ranks;
      _flags |= INIT;
   }

   // construct appropriate subclass from raw path.  We will have to guess
   // at the type, using stat_item().
   static PathPtr create(const char *path_name)
   {
      PathItemPtr item(Pool<path_item>::get());
      memset(item.get(), 0, sizeof(path_item));
      strncpy(item->path, path_name, PATHSIZE_PLUS - 1);
      item->ftype = TBD; // invoke stat_item() to determine type

      return create_shallow(item);
   }

   // --- these 2 methods assume the path_item either (a) has ftype
   //     correctly initialized (e.g. via stat_item()), or, (b) has ftype
   //     set to NONE/TBD.

   // deep copy
   static PathPtr create(const path_item *item)
   {
      PathItemPtr item_ptr(Pool<path_item>::get());
      *item_ptr = *item; // deep copy;

      return create_shallow(item_ptr);
   }
   // shallow copy  (changes to p->_item will affect caller's item)
   static PathPtr create_shallow(path_item *item)
   {
      NoOpSharedPtr<path_item> no_delete(item);
      PathItemPtr item_ptr(no_delete);

      return create_shallow(item_ptr);
   }

   // --- these 2 methods assume the path_item either (a) has ftype
   //     correctly initialized (e.g. via stat_item()), or, (b) has ftype
   //     set to TBD or NONE.

   // deep copy
   static PathPtr create(const PathItemPtr &item)
   {
      return create(item.get());
   }
   // shallow copy.
   static PathPtr create_shallow(const PathItemPtr &item)
   {
#ifdef MARFS
      if ( !(marfs_ctag_set) ) {
         size_t ctaglen = 13 + strlen( _opts->jid ) + snprintf( NULL, 0, "%d", _rank ); // pretty lame way to calc len
         char* ctagstr = (char*)malloc( sizeof(char) * ctaglen );
         if ( ctagstr ) {
            snprintf( ctagstr, ctaglen, "Pftool-Rank%d-%s", _rank, _opts->jid );
            marfs_setctag( marfsctxt, ctagstr );
            free( ctagstr );
         }
         marfs_ctag_set = 1;
      }

#endif
      PathPtr p;

      if (!item)
      {
         PRINT_MPI_DEBUG("PathFactory::create(PathItemPtr&) -- null-pointer\n");
         return PathPtr();
      }

      switch (item->ftype)
      {
      case NONE:
      case TBD:
      {
         int rc = stat_item(item.get(), *_opts); // initialize item->ftype
         int errno_save = errno;
         p = create_shallow(item); // recurse
         p->did_stat(rc == 0);     // avoid future repeats of failed stat
         if (rc)
            p->set_error(rc, errno_save);
         return p;
      }

      case NULLFILE:
      case NULLDIR:
         p = Pool<NULL_Path>::get();
         break;

      case REGULARFILE:
         p = Pool<POSIX_Path>::get();
         break;

#ifdef MARFS
      case MARFSFILE:
         p = Pool<MARFS_Path>::get();
         break;
#endif

#if GEN_SYNDATA
      case SYNDATA:
         p = Pool<SyntheticDataSource>::get();
         p->factory_install(1, _opts, _rank, -1);
         break;
#endif

      default:
         PRINT_MPI_DEBUG("PathFactory::create(PathItemPtr&) -- unknown type\n", (unsigned)item->ftype);
         return p;
      }

      *p = item; // shallow copy of caller's item
      return p;
   }

   // deep copy
   //
   // NOTE: This is a deep copy only of the path_item in <path>.  Probably
   // doesn't make sense to copy (deep or shallow) all the members into the
   // new instance.  (For example, what if the flags say it is already
   // open?)  PathFactory assignment, coupled with Path::op=, and default
   // constructors, etc, should take care of setting up a new object so
   // that (e.g.) it doesn't have to be stat'ed again, if the original had
   // been stat'ed, and has the right flags, etc.
   PathPtr create(Path &path)
   {

      if (!path._item.get())
      { // impossible?
         errsend(FATAL, "PathFactory::create(Path&) -- no path_item!\n");
         return PathPtr(); // for the compiler
      }

#if 0
      // Use the ftype in path->item to create appropriate subclass.
      // New object has a deep copy of <path>'s _item.
      PathPtr      path2(create(path._item.get())); // deep copy of path->_item

      PathItemPtr  item2(path2->_item);              // save the deep copy
      *path2 = path;                                // shallow copy of everything

      path2->_item = item2;     // replace the deep-copied _item
      return path2;
#else
      return create(path._item.get()); // deep copy of path_item, only
#endif
   }

   // shallow copy of <path>'s _item, meaning we point to the same path_item
   PathPtr create_shallow(Path &path)
   {
      if (!path._item.get())
      { // impossible?
         errsend(FATAL, "PathFactory::create_shallow(Path&) -- no path_item!\n");
         return PathPtr(); // for the compiler
      }
      return create_shallow(path._item.get());
   }

#if 0
   // UNDER CONSTRUCTION
   static void pack() { }
#endif

#if 0
   // unpack into path_item, then copy into Path
   static PathPtr unpack(char* buff, size_t size, int position, MPI_Comm comm=MPI_COMM_WORLD) {
      path_item work_node;
      MPI_Unpack(buff, size, &position, work_node, sizeof(path_item), MPI_CHAR, comm);
      return create(work_node);
   }
#else
   // avoid copying from unpacked path_item into constructed Path's path_item.
   static PathPtr unpack(char *buff, size_t size, int position, MPI_Comm comm = MPI_COMM_WORLD)
   {
      PathItemPtr work_node_ptr(Pool<path_item>::get());
      MPI_Unpack(buff, size, &position, work_node_ptr.get(), sizeof(path_item), MPI_CHAR, comm);
      return create(work_node_ptr);
   }
#endif
};

#undef NO_IMPL
#undef NO_IMPL_STATIC

#endif // __PATH_H
