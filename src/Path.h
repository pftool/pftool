#ifndef  __PATH_H
#define  __PATH_H

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
#include <stdarg.h>             // va_list, va_start(), va_arg(), va_end()
#include <string.h>             // strerror()

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>             // POSIX directories

#include <cxxabi.h>             // name-demangling
// #include <typeinfo>             // typeid()

#include <string>
#include <queue>
#include <vector>
#include <tr1/memory>

#ifdef S3
#  include<aws4c.h>             // must have version >= 5.2 !
#endif

#ifdef MARFS
#  include<aws4c.h>             // must have version >= 5.2 !
#  include <common.h>
#  include <marfs_fuse.h>
#endif

// --- fwd-decls

#define SharedPtr  std::tr1::shared_ptr

class Path;
class PathFactory;

typedef SharedPtr<path_item>   PathItemPtr;
typedef SharedPtr<Path>        PathPtr;
typedef SharedPtr<char>        CharPtr;



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
void no_op(T* ptr) {
}

template <typename T>
class NoOpSharedPtr : public std::tr1::shared_ptr<T> {
public:
  typedef std::tr1::shared_ptr<T>			BaseType;

  NoOpSharedPtr(T* ptr)
    : std::tr1::shared_ptr<T>(ptr, no_op<T>) { // construct with no-op deleter
  }
  operator BaseType& ()  { return *this; } // cast to std::tr1::shared_ptr<T>
};




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


template<typename T>
class Pool {
public:

   // put an object of type T back into the pool.
   // This is the "deleter" method used by the share_ptrs returned from get().
   static void put(T* t) {
      //      std::cout << "Pool<T>::put(" << t << ")" << std::endl;
      _pool.push_back(t);
   }

   // if the pool is not empty, extract an item, otherwise create one
   static SharedPtr<T> get(bool init_w_default=false) {
      // std::cout << "Pool<T>::get() -- pool size = " << _pool.size() << std::endl;
      if (_pool.size()) {
         T* t = _pool.back();
         _pool.pop_back();
         /// if (init_w_default)
         *t = T();                     // initialize to defaults (always)
         //         std::cout << "Pool<T>::get() -- old = " << t
         //                   << (init_w_default ? "  init" : "") << std::endl;
         return SharedPtr<T>(t, put);     // use put(), as shared_ptr deleter-fn
      }
      else {
         T* t2 = new T();
         //         std::cout << "Pool<T>::get() -- new = " << t2 << std::endl;
         return SharedPtr<T>(t2, put); // use put(), as shared_ptr deleter-fn
      }
   }

protected:

   // per-class vectors hold the pool objects
   static std::vector<T*>  _pool;
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
// ---------------------------------------------------------------------------

#define NO_IMPL(METHOD) unimplemented(__FILE__, __LINE__, (#METHOD))
#define NO_IMPL_STATIC(METHOD,CLASS) unimplemented_static(__FILE__, __LINE__, (#METHOD), (#CLASS))




class Path {
protected:

   friend class PathFactory;

   // We're assuming all sub-classes can more-or-less fake a stat() call
   // (If they can't, then they can just ignore path_item::st)
   PathItemPtr      _item;


   typedef uint16_t  FlagType;
   FlagType         _flags;

   static const FlagType   FACTORY_DEFAULT = 0x0001; // default-constructed
   static const FlagType   DID_STAT        = 0x0002; // already ran stat
   static const FlagType   STAT_OK         = 0x0004; // stat-data is usable
   static const FlagType   FOLLOW          = 0x0008; // e.g. use stat() instead of lstat()
   static const FlagType   IS_OPEN         = 0x0010; // open() succeeded, not closed
   static const FlagType   IS_OPEN_DIR     = 0x0020; // opendir() succeeded, not closed


   int              _rc;        // TBD: let user query failures, after the fact
   int              _errno;


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


   // This could be useful as a way to allow methods to be optinally
   // defined in some configurations, without requiring #ifdefs around the
   // method signatures, etc.  For instance, methods that are only needed
   // for tape-access could be given signatures in this way, and run-time
   // would throw an error in cases where the configuration doesn't provide
   // an implementation.
   void unimplemented(const char* fname, int line_number,
                      const char* method_name) const {
      unimplemented_static(fname, line_number, method_name, this->class_name().get());
   }


   static void unimplemented_static(const char* fname, int line_number,
                                    const char* method_name, const char* class_name) {
      std::cout << "file " << fname << ":" << line_number
                << " -- class '" << class_name << "'"
                << " does not implement method '" << method_name << "'."
                << std::endl;
      std::cout << "It may be that this is supported with different configure options." << std::endl;
      MPI_Abort(MPI_COMM_WORLD, -1);
   }



   // Factory uses default constructors, then calls specific subclass
   // versions of factory_install().  We want the constructors to be private
   // so, only the factory (our friend) will do creations.  Each subclass
   // might have unique args, so factory_install() is not virtual.
   //
   // This is also the default constructor!
   //
   // Subclasses would fill in path_item.ftype statically, and could look-up
   // dest_ftype from the options struct.
   //
   // maybe someone calls stat to figure sub-class type, then gives it to us.
   // If you do give us a stat struct, we'll assume the stat call succeeded.
   Path()
      : _item(Pool<path_item>::get(true)), // get recycled path_item from the pool
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
   Path(const PathItemPtr& item)
      : _item(item),
        _flags(0),
        _rc(0),
        _errno(0)
   {
      if (item->path[0] &&
          (item->st.st_ino || item->st.st_mode || item->st.st_uid || item->st.st_ctime))
         did_stat(true);
   }


   // This is how PathFactory initializes Path objects.
   //
   // If <item>.st has non-null fields, we infer that it is valid.  This
   // allows us to make a no-copy Path object corresponding to a path_item
   // sent over the wire, without having to run stat() again, to figure out
   // what it is.
   virtual Path& operator=(const PathItemPtr& item) {

      if ((_item->ftype > TBD) &&
          (_item->ftype != item->ftype)) {
         errsend_fmt(FATAL, "Attempt to change ftype during assignment '%s' = '%s'\n",
                     _item->path, item->path);
         return *this;
      }

      path_change_pre();        // subclasses may want to be informed

      _item  = item;            // returns old _item to Pool<path_item>
      _flags = 0;               // not a FACTORY_DEFAULT, if we were before
      _rc    = 0;
      _errno = 0;

      if (item->path[0] &&
          (item->st.st_ino || item->st.st_mode || item->st.st_uid || item->st.st_ctime))
         did_stat(true);

      path_change_post();       // subclasses may want to be informed

      return *this;
   }


   // *** IMPORTANT: because we use Pools, objects are re-used without
   //     being destructed.  That means they must override this method, to
   //     perform any state-clearing operations, when an old object gotten
   //     from the Pool is about to be reused.
   //
   //     NOTE: don't forget to pass the call along to parent classes.
   //
   ///   virtual reset_all() {
   ///      _item = PathItemPtr();
   ///   }


   virtual void close_all() {
      if (_flags & IS_OPEN)
         close();
      else if (_flags & IS_OPEN_DIR)
         closedir();
   }

   // Whenever the path changes, various local state may become invalid.
   // (e.g when someone turns on FOLLOW, the old lstat() results are
   // no-longer correct.)  Subclasses can intercept this to do local work,
   // in those cases.  Saves them having to override op=()
   virtual void path_change_pre() {
      close_all();
      _flags &= ~(DID_STAT | STAT_OK);
   }

   virtual void path_change_post() { }


   // this is called whenever there's a chance we might not have done a
   // stat.  If we already did a stat(), then this is a no-op.  Otherwise,
   // defer to subclass-specific impl of stat().
   bool   do_stat(bool err_on_failure=true) {
      if (! (_flags & DID_STAT)) {
         _flags &= ~(STAT_OK);

         bool success = do_stat_internal(); // defined by subclasses
         did_stat(success);

         // maybe flag success
         if (! success) {
            if (err_on_failure) {
               errsend_fmt(NONFATAL, "Failed to stat path %s", _item->path);
            }
         }
      }

      return (_flags & STAT_OK);
   }

   // return true for success, false for failure.
   virtual bool do_stat_internal() = 0;

   void did_stat(bool stat_succeeded = true) {
      _flags &= ~(FACTORY_DEFAULT);
      _flags |= (DID_STAT);
      if (stat_succeeded) {
         _flags |= STAT_OK;

         _item->offset = 0;
         _item->length = _item->st.st_size;
      }
   }


   void factory_install(int count, ...) {
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
   virtual void factory_install_list(int count, va_list list) {
      NO_IMPL(factory_install_list);
   }

   virtual void    set  (FlagType flag)  { _flags |= flag; }
   virtual void    unset(FlagType flag)  { _flags &= ~(flag); }


public:

   virtual ~Path() {
      // close_all();   // Wrong.  close() is abstract in Path.
   }

   // demangled name of any subclass
   // NOTE: abi::__cxa_demangle() returns a dynamically-allocated string,
   //       so we use a smart-pointer to make things easier on the caller.
   virtual CharPtr  class_name() const {
      int status;
      char* name = abi::__cxa_demangle(typeid(*this).name(), 0, 0, &status);
      return CharPtr(name);
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
   virtual bool identical(Path& p)     { return false; } // same item, same FS
   virtual bool identical(PathPtr& p)  { return identical(*p); } // same item, same FS
   //   virtual bool equivalent(Path& p) { return false; } // same size, perms, etc



   // Create a new Path with our path-name plus <suffix>.  Local object is
   // unchanged.  Result (via the factory) might be a different subclass
   // (e.g. descending into PLFS directory).  Let PathFactory sort it out,
   // using stat_item(), etc.
   virtual PathPtr append(char* suffix) const;



   // don't let outsider change path without calling path_change()
   const char* const  path() const { return (char*)_item->path; }

   // don't let outsider change stat without unsetting DID_STAT
   const struct stat& st() { do_stat(); return _item->st; }

   // don't let outsider change path_item
   // const path_item&   item() const { return *_item; }
   const path_item&   node() const { return *_item; }

   // As near as I can tell, dest_ftype is used as a local copy of the
   // ftype of the destination, in order to make this knowledge available
   // in places where the top-level dest-node is not available, such as
   // update_stats(), and copy_file().  It's assigned in stat_item(), and
   // then defaults are overridden in process_stat_buffer(), it seems.
   // This value has no influence on the which Path subclass should own a
   // given path_item (unlike ftype), so we allow it to be set and changed
   // as needed.
   FileType     dest_ftype() const { return _item->dest_ftype; }
   void         dest_ftype(FileType t) { _item->dest_ftype = t; }

   // ON SECOND THOUGHT:
   // virtual FileType ftype_for_destination() { REGULARFILE; } // subclasses do what they want


   // if you just want to know whether stat succeeded call this
   virtual bool    stat()     { return do_stat(false); }
   virtual bool    exists()   { return do_stat(false); } // just !ENOENT?

   // These are all stat-related, chosen to allow interpretation in the
   // context of non-POSIX sub-classes.  We assume all subclasses can
   // "fake" a struct stat.
   ///
   // CAREFUL: These also return false if the file doesn't exist!
   virtual bool    is_link()  { do_stat(); return S_ISLNK(_item->st.st_mode); }
   virtual bool    is_dir()   { do_stat(); return S_ISDIR(_item->st.st_mode); }

   virtual time_t  ctime()    { do_stat(); return _item->st.st_ctime; }
   virtual time_t  mtime()    { do_stat(); return _item->st.st_mtime; }
   virtual size_t  size()     { do_stat(); return _item->st.st_size; }

   virtual bool    is_open()  {            return  (_flags & (IS_OPEN | IS_OPEN_DIR)); }

   // where applicable, stat() calls should see through symlinks.
   virtual void    follow()  {
      path_change_pre();
      _flags |= FOLLOW;
   }

   // try to adapt these POSIX calls
   virtual bool    chown(uid_t owner, gid_t group)  = 0;
   virtual bool    chmod(mode_t mode)               = 0;
   virtual bool    utime(const struct utimbuf* ut)  = 0;


   // These are per-class qualities, that pftool may want to know
   virtual bool    supports_n_to_1() const   = 0; // can support N:1, via chunks?


   // when subclass operations fail (e.g. mkdir(), they save errno (or
   // whatever), and return false.  Caller can then come back and get the
   // corresponding error-string from here.
   virtual const char* const strerror()     { NO_IMPL(strerror); }


   // open/close do not return file-descriptors, like POSIX open/close do.
   // You don't need those.  You just open a Path, then read from it, then
   // close it.  The boolean tells you whether you succeeded.  (Consider
   // that, for some Path sub-classes, there is never a "file descriptor"
   // they could return.)  Later, we'll give access to the error-status
   // which we are keeping track of.
   // 
   virtual bool    open(int flags, mode_t mode)   = 0; // non-POSIX will have to interpret
   virtual bool    close()                        = 0;

   // read/write to/from caller's buffer
   virtual ssize_t read( char* buf, size_t count, off_t offset)   = 0; // e.g. pread()
   virtual ssize_t write(char* buf, size_t count, off_t offset)   = 0; // e.g. pwrite()

   // return false only to indicate errors (e.g. not end-of-data for readdir())
   // At EOF, readdir() returns true, but sets path[0] == 0.
   virtual bool    opendir()              = 0;
   virtual bool    closedir()             = 0;
   virtual bool    readdir(char* path, size_t size)  = 0;
   virtual bool    mkdir(mode_t mode)     = 0;

   // delete the file/object
   virtual bool    remove()                       = 0;
   virtual bool    unlink()                       = 0;

   virtual bool    symlink(const char* link_name)  { _errno=0; return false; }


#if 0
   // pftool uses intricate comparisons of members of the struct st, after
   // an lstat().  This won't translate well to obj-storage systems. For
   // example, S3 doesn't have a "creation-date".  Therefore, it is
   // impossible to compare the creation-date of a POSIX file with an
   // object from an S3-based object-filesystem.  Instead, you'll have to
   // make do with modification-dates.  (see compare_date()).
   virtual bool    metadata_equal(Path*) { NO_IMPL(compare_access); }

   // copy into local meta-data from argument-Path
   virtual void    copy_metadata(Path* example) = 0; // FKA update_stats()

   // return {-1, 0, 1} for <this> being {earlier, same, later} than <Path>.
   virtual int     compare_date(Path*) { NO_IMPL(compare_access); }
#endif


   PathPtr         get_output_path(path_item src_node, path_item dest_node, struct options o);

   // fstype is apparently only used to distinguish panfs from everything else.
   static FSType   parse_fstype(const char* token) { return (strcmp(token, "panfs") ? UNKNOWN_FS : PAN_FS); }
   const char*     fstype_to_str()                 { return ((_item->fstype == PAN_FS) ? "panfs" : "unknown"); }
   
};



typedef std::queue<PathPtr>                     PathQueue;

typedef std::vector<PathPtr>                    PathVec;
typedef std::vector<PathPtr>::iterator          PathVecIt;
typedef std::vector<PathPtr>::const_iterator    PathVecConstIt;



// ---------------------------------------------------------------------------
// SYNTHETIC DATA SOURCE
//
// This is a source-side way to create artificial data. Synthetic data
// doesn't actually come from a file.  (see syndata.*)
//
// This is mainly used in copy_file().
// ---------------------------------------------------------------------------

#ifdef GEN_SYNDATA
class SyntheticDataSource : public Path {
protected:

   friend class Pool<SyntheticDataSource>;

   struct options*   _o;
   int               _rank;
   SyndataBufPtr     _synbuf;
   

   virtual bool do_stat_internal() {
      if (_o->syn_size) {
         // We are generating synthetic data, and NOT copying data in
         // file. Need to muck with the file size
         _item->st.st_size = _o->syn_size;
      }
      return true;
   }

   SyntheticDataSource()
      : Path(),
        _o(NULL),
        _rank(-1),
        _synbuf(NULL) {

      strncpy(_item->path, "SyntheticDataSource", PATHSIZE_PLUS);
   }

   // we expect args from the Factory:
   //
   // (0) struct options *  [options]
   // (1) int               [rank]
   //
   virtual void factory_install_list(int count, va_list list) {
      _o    = va_arg(list, struct options *);
      _rank = va_arg(list, int);
   }

public:

   virtual ~SyntheticDataSource() {
      close_all();
   }

   virtual bool    supports_n_to_1() const  { return false; }

   // TBD: assure we are only being opened for READ
   virtual bool    open(int flags, mode_t mode) {
      _synbuf = syndataCreateBufferWithSize(((o.syn_pattern[0]) ? o.syn_pattern : NULL),
                                            ((o.syn_size >= 0)  ? o.syn_size : -rank));
      if (! synbuf) {
         errsend_fmt(FATAL, "Rank %d: Failed to allocate synthetic-data buffer\n", rank);
         unset(IS_OPEN);
      }
      else
         set(IS_OPEN);

      return is_open();
   }
   virtual bool    close() {
      syndataDestroyBuffer(_synbuf);
      unset(IS_OPEN);
      return true;
   }



   // read/write to/from caller's buffer
   virtual ssize_t read(char* buf, size_t count, off_t offset) {
      int rc = syndataFill(_synbuf, buf, count);
      if (rc) {
         errsend_fmt(NONFATAL, "Failed to copy from synthetic data buffer. err = %d", rc);
         return 0;
      }
      return count;
   }


   virtual ssize_t write(char* buf, size_t count, off_t offset) {
      NO_IMPL(read);            // we are only a source, not a sink
   }





   virtual bool    opendir()             { NO_IMPL(opendir); }
   virtual bool    closedir()            { NO_IMPL(closedir); }
   virtual bool    readdir(char* path, size_t) { NO_IMPL(readdir); }
   virtual bool    mkdir(mode_t mode)    { NO_IMPL(mkdir); }

   virtual bool    remove()              { return true; }
   virtual bool    unlink()              { return true; }
};

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

class POSIX_Path: public Path {
protected:

   friend class Pool<POSIX_Path>;

   int            _fd;          // after open()
   DIR*           _dirp;        // after opendir()

   // FUSE_CHUNKER seems to be the only one that uses stat() instead of lstat()
   virtual bool do_stat_internal() {
      _errno = 0;

      // run appropriate POSIX stat function
      if (_flags & FOLLOW)
         _rc = ::stat(_item->path, &_item->st);
      else
         _rc = lstat(_item->path, &_item->st);

      if (_rc) {
         _errno = errno;
         return false;
      }

      // couldn't we just look at S_ISLNK(_item->st.st_mode), when we want to know?
      if (S_ISLNK(_item->st.st_mode))
         _item->ftype = LINKFILE;
      else
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

   // This runs on Pool<POSIX_Path>::get(), when initting an old instance
   virtual ~POSIX_Path() {
      close_all();
   }


   //   virtual bool operator==(POSIX_Path& p) { return (st().st_ino == p.st().st_ino); }
   virtual bool identical(POSIX_Path& p) { 
      return (st().st_ino == p.st().st_ino);
   }


   virtual bool    supports_n_to_1() const  {
      return false;
   }



   //   virtual int    mpi_pack() { NO_IMPL(mpi_pack); } // TBD

   virtual const char* const strerror() {
      return ::strerror(_errno);
   }

   virtual bool    chown(uid_t owner, gid_t group) {
      if (_rc = ::chown(path(), owner, group))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool    chmod(mode_t mode) {
      if (_rc = ::chmod(path(), mode))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool    utime(const struct utimbuf* ut) {
      if (_rc = ::utime(path(), ut))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }

   // see comments at Path::open()
   // NOTE: We don't protect user from calling open when already open
   virtual bool    open(int flags, mode_t mode) {
      _fd = ::open((char*)_item->path, flags, mode);
      if (_fd < 0) {
         _rc = _fd;
         _errno = errno;
         return false; // return _fd;
      }
      set(IS_OPEN);
      return true;  // return _fd;
   }
   virtual bool    opendir() {
      _dirp = ::opendir(_item->path);
      if (! bool(_dirp)) {
         _errno = errno;
         return false;  // return _rc;
      }
      set(IS_OPEN_DIR);
      return true;
   }


   // NOTE: We don't protect user from calling close when already closed
   virtual bool    close() {
      _rc = ::close(_fd);
      if (_rc < 0) {
         _errno = errno;
         return false;  // return _rc;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN);
      return true; // return _fd;
   }
   virtual bool    closedir() {
      _rc = ::closedir(_dirp);
      if (_rc < 0) {
         _errno = errno;
         return false;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN_DIR);
      return true;
   }


   virtual ssize_t read( char* buf, size_t count, off_t offset) {
      ssize_t bytes = pread(_fd, buf, count, offset);
      if (bytes == (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }
   virtual bool    readdir(char* path, size_t size) {
      errno = 0;
      if (size)
         path[0] = 0;

      struct dirent* d = ::readdir(_dirp);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      if (d > 0) {
         strncpy(path, d->d_name, size);
         return true;
      }
      else if (d == 0)          // EOF
         return true;
      else if (d < 0) {
         _errno = errno;
         return bool(_errno == 0);
      }
   }


   virtual ssize_t write(char* buf, size_t count, off_t offset) {
      ssize_t bytes = pwrite(_fd, buf, count, offset);
      if (bytes ==  (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }
   virtual bool    mkdir(mode_t mode) {
      if (_rc = ::mkdir(_item->path, mode)) {
         _errno = errno;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }



   virtual bool    remove() {
      return unlink();
   }
   virtual bool    unlink() {
      if (_rc = ::unlink(_item->path))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   
   virtual bool    symlink(const char* link_name) {
      if (_rc = ::symlink(_item->path, link_name)) {
         _errno = errno;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
};





// ---------------------------------------------------------------------------
// PLFS
//
// plfs has its own functions for stat/open/close/etc, but it was designed
// to act a lot like POSIX.
//
// NOTE: Testing shows that plfs_getattr() returns zero for the
// mount-point, and anything inside the mount-point, including symlinks
// leading outside the mount-point.  Meanwhile, plfs_getattr() returns
// non-zero for everything outside the mount-point, including the
// directories that are mounted on the mount-point, and symlinks leading
// inside the mount-point.
//
// TBD: use "strplfserr(_rc)" to get PLFS-specific error-messages
//
// ---------------------------------------------------------------------------

#ifdef PLFS

class PLFS_Path : public POSIX_Path {
protected:

   friend class Pool<PLFS_Path>;

   Plfs_fd*      _plfs_fd;
   Plfs_dirp*    _plfs_dirp;

   plfs_error_t  _plfs_rc;
   int           _plfs_open_flags; // needed for plfs_close()

   pid_t         _pid;
   int           _rank;


   virtual bool do_stat_internal() {
      _errno = 0;
      _plfs_rc = plfs_getattr(NULL, _item->path, &_item->st, 0);
      if (_plfs_rc == PLFS_SUCCESS) {
         _item->ftype = PLFSFILE;
         return true;
      }
      return false;
   }

   // private.  Use PathFactory to create paths.
   PLFS_Path()
      : POSIX_Path(),
        _plfs_fd(NULL),
        _plfs_dirp(NULL),
        _plfs_rc(PLFS_SUCCESS),
        _pid(0),
        _rank(-1),
        _plfs_open_flags(0)
   { }


   // we expect args from the Factory:
   //
   // (0) int               [pid]
   // (1) int               [rank]
   //
   virtual void factory_install_list(int count, va_list list) {
      _pid  = va_arg(list, int);
      _rank = va_arg(list, int);
   }


public:

   // This runs on Pool<PLFS_Path>::get(), when initting an old instance
   virtual ~PLFS_Path() {
      close_all();
   }

   virtual bool    supports_n_to_1() const  { return true; }


   virtual const char* const strerror() { return strplfserr(_plfs_rc); }


   virtual bool    chown(uid_t owner, gid_t group) {
      if (_rc = plfs_chown(path(), owner, group))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool    chmod(mode_t mode) {
      if (_rc = plfs_chmod(path(), mode))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }
   virtual bool    utime(const struct utimbuf* ut) {
      // PLFS version of utime() doesn't have the same const-ness as POSIX version
      if (_rc = plfs_utime(path(), const_cast<struct utimbuf*>(ut)))
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_rc == 0);
   }

   // see comments at Path::open()
   // NOTE: We don't protect user from calling plfs_open when already open
   // NOTE: We return <_plfs_fd> or -1, just like POSIX "open"
   virtual bool    open(int flags, mode_t mode) {
      _plfs_open_flags = flags; // save for plfs_close()
      _plfs_rc = plfs_open(&_plfs_fd, _item->path, flags, _pid+_rank, _item->st.st_mode, NULL);
      if (_plfs_rc == PLFS_SUCCESS)
         set(IS_OPEN);
      return(_plfs_rc == PLFS_SUCCESS);
   }
   virtual bool    opendir() {
      _plfs_rc = ::plfs_opendir_c(_item->path, &_plfs_dirp);
      if (_plfs_rc == PLFS_SUCCESS)
         set(IS_OPEN_DIR);
      return(_plfs_rc == PLFS_SUCCESS);
   }



   // NOTE: We don't protect user from calling plfs_close when already closed
   // NOTE: We return {0, -1}, just like POSIX "close"
   virtual bool    close() {
      int  num_ref;
      _plfs_rc = plfs_close(_plfs_fd, _pid+_rank, _pid+_rank, _plfs_open_flags, NULL, &num_ref);
      if (_plfs_rc == PLFS_SUCCESS)
         unset(IS_OPEN);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return(_plfs_rc == PLFS_SUCCESS);
   }
   virtual bool    closedir() {
      _plfs_rc = plfs_closedir_c(_plfs_dirp);
      if (_plfs_rc == PLFS_SUCCESS)
         unset(IS_OPEN_DIR);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return(_plfs_rc == PLFS_SUCCESS);
   }


   virtual ssize_t read( char* buf, size_t count, off_t offset) {
      ssize_t bytes_read = 0;
      _plfs_rc = plfs_read(_plfs_fd, buf, count, offset, &bytes_read);
      if (_plfs_rc != PLFS_SUCCESS) {
         return -1;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes_read;
   }
   virtual bool    readdir(char* path, size_t size) {
      _plfs_rc = plfs_readdir_c(_plfs_dirp, path, size);
      if (_plfs_rc != PLFS_SUCCESS)
         path[0] = 0;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_plfs_rc == PLFS_SUCCESS);
   }


   virtual ssize_t write(char* buf, size_t count, off_t offset) {
      ssize_t bytes_written = 0;
      _plfs_rc = plfs_write(_plfs_fd, buf, count, offset, _pid+_rank, &bytes_written);
      if (_plfs_rc != PLFS_SUCCESS)
         return -1;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes_written;
   }
   virtual bool    mkdir(mode_t mode) {
      _plfs_rc = plfs_mkdir(_item->path, mode);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_plfs_rc == PLFS_SUCCESS);
   }


   virtual bool    remove() {
      return unlink();
   }
   virtual bool    unlink() {
      _plfs_rc = plfs_unlink(_item->path);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_plfs_rc == PLFS_SUCCESS);
   }
   virtual bool    symlink(const char* link_name) {
      _plfs_rc = plfs_symlink(_item->path, link_name);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return (_plfs_rc == PLFS_SUCCESS);
   }

};

#endif






// ---------------------------------------------------------------------------
// FUSE-CHUNKER
//
// I haven't been able to find anyone who can explain the intent of the
// fuse-chunker actions.  It looks like it might be an early attempt to do
// what was later done more effectively with PLFS.
//
// It appears that
//
// ---------------------------------------------------------------------------






// ---------------------------------------------------------------------------
// S3
//
// Using the aws4c library, version 5.2.0, extended by LANL.
// [https://github.com/jti-lanl/aws4c.git]
//
// Because pftool currently uses 'struct stat' so extensively, we're going
// to fake one by installing our metadata into a structu stat.  Hopefully,
// this isn't totally crazy.
//
// Here's how I'm thinking about "directories" in S3.  If you have a long
// path-name with many '/' in it, the first one (after the host) is the
// bucket.  Any would-be "directory" under that can be "listed" by doing
// queries for bucket-members that have object names matching that part of
// the path.  Therefore, there is nothing to do, to actually create those
// "directories".  The only special case is the bucket, at top-level, which
// must be created.  We (will eventually) do that automatically for you.
//
// Therefore, if you call mkdir() on a path like
// "http://10.142.0.1:9020/A/B/C/" We will create a bucket named "A".  Our
// mkdir() is a no-op.  When you later go to create an object named
// "http://10.142.0.1:9020/A/B/C/my_file.txt", we will create it as an
// object named "B/C/my_file.txt" in the bucket "A".  When you call
// readir() on a path named "A/B/C/", we will query the bucket "A" for
// objects with a prefix matching "B/C/", using "?delimiter=/".  This will
// do what you expected. See:
//
//     http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
//
//
//
//
// TBD: pftool makes extensive use of struct stat.  pftool uses struct stat
//      to check whether something is a directory or symlink, what it's
//      size is, etc.  The classes here can eventually abstract some of
//      that away, but the mindset of checking for symlinks, etc, will
//      remain.  the simplest way to try to shoehorn an S3 object into
//      pftool is to fake a stat struct, like we do in fake_stat().
//
//      This fake struct is probably going to be expensive and full of
//      confusion.  For example:
//
//      If you want to know whether an object is a directory, what do you
//      mean?  Do you mean is this an S3 bucket?  If so, we have to do a
//      separate query to find out whether it's a bucket.  But, maybe we
//      have a "dual personality" object/file-system.  In that case, maybe
//      you mean, "does this object correspond to something that is a
//      directory in the FS view?"  That might be complicated/expensive to
//      figure out.  Or maybe you just mean "does this destination-path end
//      in a slash, so that I should treat it like it is a directory, in
//      terms of appending directory-elements from the source-directory to
//      this path-name?"
//
//      Similar ambiguity comes up regarding symlinks.  Do you want to know
//      whether there's a file-view of this object (e.g. through Scality
//      sfused) in which it is a symlink?  Or, maybe you meant that e.g. a
//      destination-side object corresponds to a source-side POSIX symlink.
//      Or, did you encrypt something in the metadata to simulate a
//      symlink?
//
//      Object ACLS do not have the same fields as modes in the stat struct
//      for a POSIX filesystem.
//
//      When you ask about the size of an object, I think you might get a
//      not-yet-consistent report, if the object is being written by
//      parallel writers, or was recently written, even if the writes are
//      complete.
//
//
//
// ***  I think the read/write methods of the S3_Path class have to do
//      their own coordination of S3 multi-part-upload (or EMC writing
//      ranges, or Scality filejoin).  The caller just gives us offsets,
//      and we take care of collecting ETags, writing the manifest, etc.
//
// ---------------------------------------------------------------------------

#ifdef S3

class S3_Path : public Path {
protected:

   friend class Pool<S3_Path>;

   typedef SharedPtr<IOBuf>  IOBufPtr;

   IOBufPtr        _iobuf;

   std::string     _host;
   std::string     _bucket;
   std::string     _obj;        // path after bucket (and '/')

   bool is_service() const { return (!_bucket.size()); }
   bool is_bucket()  const { return (_bucket.size() && _obj.size()); }
   bool is_object()  const { return (_obj.size()); }

   // generic things to be done to set up a query with our path.
   //
   // NOTE: The same user may have multiple S3_Paths, potentially having
   //       different server, bucket, etc.  So this must be done before
   //       every query.
   void prepare_query() {
      // do_stat(false);           // no-op, unless needed

      // install host/bucket for S3 queries
      s3_set_host(_host.c_str());
      if (_bucket.size())
         s3_set_bucket(_bucket.c_str());
   }

   // return true for success, false for failure.
   virtual bool do_stat_internal() {
      fake_stat(_item->path, &_item->st);
   }

   // Intercept path-changes, so we can parse the new host, bucket, etc.
   // These parsed values are useful for quick determination of whether we
   // are dealing with a bucket or an object.
   virtual void path_change_post() {
      parse_host_bucket_object(_host, _bucket, _obj, _item->path);
      if (! _host.size()) {
         errsend_fmt(FATAL,
                     "S3_Path::path_change_post -- no host in '%s'\n",
                     _item->path);
      }

      // pass method-call on, to the base-class
      Path::path_change_post();
   }


   // interpret the fields of _item->st (which have been modified, e.g. by
   // chmod()), and apply them to the object ACLs, as much as is possible.
   // Return false only for errors.
   bool apply_stat() { NO_IMPL(apply_stat); } // TBD  (See fake_stat() impl)


   S3_Path()
      : Path(),
        _iobuf(Pool<IOBuf>::get(true)) // aws_iobuf_new() is just malloc + memset
   {
      //      memset(_iobuf.get(), 0, sizeof(IOBuf));
   }


public:

   // This runs on Pool<S3_Path>::get(), when initting an old instance
   virtual ~S3_Path() {
      close_all();
      aws_iobuf_reset(_iobuf.get());
   }

   // TBD: Save our object ID somehow.  Maybe give fake_stat() an extra
   //      arg, and have the factory call a factory_install_list() method
   //      to install it.  Then two S3_Path objects are identical if they
   //      have the same ID.  For now, the NO_IMPL() will help me remember
   //      that this needs doing.
   //
   //   virtual bool operator==(const S3_Path& p) { NO_IMPL(op==); }
   virtual bool identical(const S3_Path& p) { NO_IMPL(identical); }


   // Strict S3 support N:1 via Multi-Part-Upload.  Scality adds a
   // "filejoin" operator, which resembles MPU.  EMC extensions also
   // support writing to object+offset.
   virtual bool    supports_n_to_1() const  { return true; }


   // Fill out a struct stat, using S3 metadata from an object filesystem.
   // PathFactory can use this (via stat_item()) when determining what
   // subclass to allocate.  Return true for success, false for error.
   static bool fake_stat(const char* path_name, struct stat* st);

   // extracting this to a static method, from path_change_post(), so it
   // can be re-used in fake_stat()
   static bool parse_host_bucket_object(std::string& host,
                                        std::string& bucket,
                                        std::string& obj,
                                        const char*  path) {
      bool trailing_slash = false;

      host.clear();
      bucket.clear();
      obj.clear();


      // Parse host (and maybe bucket (and maybe object)) from the URL/path.
      // Strip off the leading and trailing '/' from each.
      char* host_begin = (char*)strstr(path, "://") +3; // factory found this, so it exists
      if (*host_begin) {
         char*                  host_end   = strchr(host_begin, '/');
         std::string::size_type host_size = (host_end
                                             ? (host_end - host_begin)
                                             : strlen(host_begin));
         host   = std::string(host_begin, host_size);

         // if path includes a bucket, parse it out
         if (host_end && *(host_end +1)) {
            char*                  bkt_begin = host_end +1;
            char*                  bkt_end   = strchr(bkt_begin, '/');
            std::string::size_type bkt_size  = (bkt_end
                                                ? (bkt_end - bkt_begin)
                                                : strlen(bkt_begin));
            bucket = std::string(bkt_begin, bkt_size);

            // if path includes an object, beyond the bucket, then extract
            // that.  We don't want the (possible) '/', at the end of the
            // bucket-name.  Also don't want trailing slashes in the
            // object-name.  However, if there were trailing slashes, we
            // want to return true, but only if there was some object-name.
            //
            //            if (bkt_end)
            //               obj = bkt_end;
            if (bkt_end && *(bkt_end +1)) {
               char*  ptr        = bkt_end +1; // beginning of object
               int    tail_pos   = strlen(ptr) -1;
               bool   tail_slash = (ptr[tail_pos] == '/');
               // for (size_t pos=tail_pos; ((pos >= 0) && (ptr[pos] == '/')); --pos) {
               while ((tail_pos >= 0) && (ptr[tail_pos] == '/'))
                  --tail_pos;
               if (tail_pos >= 0) {
                  obj = std::string(ptr, tail_pos+1);
                  trailing_slash = tail_slash;
               }
            }
         }
      }
      return trailing_slash;
   }


   virtual const char* const strerror() { return _iobuf->result; }

   // aint no such thing as a specific "group"
   // And your <owner> probably has no relationship at all with S3 owners.
   //
   // TBD: This should make an effort to update the ACLs for this object.
   //
   // NOTE: Don't want to update ACLs for the hybrid approach.  And
   //       returning false will cause pftool to emit a FATAL error.  So,
   //       don't do anything, and say that we succeeded.
   virtual bool    chown(uid_t owner, gid_t group) {
      //      _item->st.st_uid = owner;
      //      _item->st.st_gid = group;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::chown() ]
   }
   virtual bool    chmod(mode_t mode) {
      //      _item->st.st_mode = mode;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::chown() ]
   }
   virtual bool    utime(const struct utimbuf* ut) {
      //      _item->st.st_atime = ut->actime;
      //      _item->st.st_mtime = ut->modtime;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::chown() ]
   }


   // Should we replicate POSIX behavior, where open() fails if you try it
   // multiple times?
   //
   // NOTE: S3_Path::write() expects to be able to write byte-ranges.  That
   //       fails with '404 Not Found', if the object doesn't exist.
   //       Therefore, our open() must create the object.  We should also
   //       be looking at O_TRUNC, etc.  If it's not O_CREAT and file
   //       doesn't exist, we should avoid creating it?  Forget dealing
   //       with O_WRONLY, for now.  We could implement that by
   //       manipulating ACLs for this object, but in the future we'll be
   //       handling permissions through the metadata filesystem, so skip
   //       it for now.
   virtual bool    open(int flags, mode_t mode) {
      if (flags & O_CREAT)
         write((char*)"", 0, 0);
      set(IS_OPEN);
      return true;
   }
   // TBD: GET _obj + "?prefix=DIRNAME&delimiter=/" This will return XML
   //      with "common prefixes".  Parse the XML into a tree.  Find the
   //      token <IsTruncated> and read its value ("true" or "false", etc).
   //      Save this somewhere.  Parse to the beginning of the
   //      CommonPrefixes.  Each call to readdir() will then return the
   //      next XML entry.  When the XML is exhausted, if IsTruncated was
   //      true, reiterate and get more XML, and do the same parsing etc.
   //      Otherwise, treat as EOF.
   //
   //      [Leaving this undone, for now, because the first priority is
   //      moving data *onto* S3 systems.]
   //
   virtual bool    opendir() {
      NO_IMPL(opendir);
      set(IS_OPEN_DIR);
   }



   virtual bool    close() {
      unset(IS_OPEN);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return true;
   }
   // TBD: See opendir().  For the closedir case, be need to deallocate
   //      whatever is still hanging around from the opendir.
   virtual bool    closedir() {
      NO_IMPL(opendir);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN_DIR);
   }




   // read/write to/from caller's buffer
   // TBD: fix the malloc/free in aws_iobuf_extend/aws_iobuf_reset (see TBD.txt).
   // NOTE: S3 has no access-time, so we don't reset DID_STAT
   virtual ssize_t read( char* buf, size_t count, off_t offset) {
      IOBuf* b = _iobuf.get();
      prepare_query();

      s3_set_byte_range(offset, count);

      // For performance, we add <buf> directly into the linked list of
      // data in _iobuf.  In this case (i.e. reading), we're "extending"
      // rather than "appending".  That means the added buffer represents
      // empty storage, which will be filled by the libcurl writefunction,
      // invoked via s3_get().

      aws_iobuf_reset(b);
      aws_iobuf_extend_static(b, buf, count);
      AWS4C_CHECK   ( s3_get(b, (char*)_obj.c_str()) );

      // AWS4C_CHECK_OK( b );
      ssize_t byte_count = count; // default
      if (b->code == 206) {  /* 206: Partial Content */
         byte_count = b->contentLen;
      }
      else if (b->code != 200) {
         byte_count = -1;
      }

      // drop ptrs to <buf>
      aws_iobuf_reset(b);

      return byte_count;
   }
   // TBD: See opendir()
   virtual bool    readdir(char* path, size_t size) {
      NO_IMPL(readdir);
   }



   virtual ssize_t write(char* buf, size_t count, off_t offset) {
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      IOBuf* b = _iobuf.get();
      prepare_query();

      // by calling with count=0, offset=0, open() can use write
      // to initialize a zero-size object.
      if (count || offset)
         s3_set_byte_range(offset, count);

      // For performance, we add <buf> directly into the linked list of
      // data in _iobuf.  In this case (i.e. writing), we're "appending"
      // rather than "extending".  That means the added buffer represents
      // valid data, which will be read by the libcurl readfunction,
      // invoked via s3_put().
      aws_iobuf_reset(b);
      aws_iobuf_append_static(b, buf, count);
      AWS4C_CHECK    ( s3_put(b, (char*)_obj.c_str()) );

      // AWS4C_CHECK_OK ( b );
      ssize_t byte_count = count; // default
      if (b->code != 200) {
         byte_count = -1;
      }

      // drop ptrs to <buf>
      aws_iobuf_reset(b);

      return byte_count;
   }


   // NOTE: See comments above this class for discussion of how
   //       "directories" are handled.  If full path includes an obj, we'll
   //       have to assume you intend it as an empty directory.
   virtual bool    mkdir(mode_t mode) {
      IOBuf* b = _iobuf.get();
      aws_iobuf_reset(b);                     // clear everything

      prepare_query();                        // update stat, install host and bucket
      mode_t new_mode = _item->st.st_mode | __S_IFDIR;

      // install value for "mode_bits" key, into meta-data.
      //
      // NOTE: We will ignore all permission-related bits!
      //       See S3_Path::fake_stat()
      const size_t STR_SIZE = 16;
      char new_mode_str[STR_SIZE];
      snprintf(new_mode_str, STR_SIZE, "0x%08X", new_mode);
      aws_metadata_set(&(b->meta), "mode_bits", new_mode_str);

#if 0
      // create the directory with a name that includes a final slash.
      // That way, it will be obvious during readdir() that this thing is a
      // directory, and S3_Path::fake_stat() can set the mode bits, saving
      // us ever having to check the metadata.
      std::string dirname(_obj + "/");
      AWS4C_CHECK( s3_put(b, (char*)dirname.c_str()) ); // PUT creates bucket + obj
#else
      // No, it will be only be obvious this is a directory (among the
      // results of readdir), if there is anything in the directory.  In
      // that case, one of the readdir results will include the slash.
      // Otherwise, fake_stat will just have to query metadata.
      AWS4C_CHECK( s3_put(b, (char*)_obj.c_str()) ); // PUT creates bucket + obj
#endif
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      if (b->code == 200)
         _item->st.st_mode = new_mode;

      return (b->code == 200);
   }


   // For S3, we'll assume you called unlink(), rather than remove(),
   // because you think that, when you unlink() something, it should
   // continue to be usable until no one is referring to it, or having it
   // open.  S3 don't play that.
   virtual bool    unlink() {
      return true;              // Sure, whatever.
   }

   virtual bool    remove() {
      IOBuf* b = _iobuf.get();
      prepare_query();

      aws_iobuf_reset(b);                     // empty
      AWS4C_CHECK( s3_delete(b, (char*)(_obj.size() ? _obj.c_str(): "")) );
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date

      // DELETE returns '204 No Content'  (?)
      // return (b->code == 200);
      return true;
   }
};

#endif



// ---------------------------------------------------------------------------
// MARFS
//
//
//
// ---------------------------------------------------------------------------

#ifdef MARFS

#include <linux/limits.h>

typedef struct {
   char valid; // lets us know if name is valid. 1 == valid
   char name[PATH_MAX];
} marfs_dirp_t;

int marfs_readdir_filler(void *buf, const char *name, const struct stat *stbuf, off_t off);

int marfs_readdir_wrapper(marfs_dirp_t* dir, const char* path, struct fuse_file_info* ffi);

class MARFS_Path : public Path {
protected:

   friend class Pool<MARFS_Path>;

   typedef SharedPtr<IOBuf>  IOBufPtr;

   IOBufPtr        _iobuf;
   struct fuse_file_info ffi_directory;
   struct fuse_file_info ffi_file;
   //const char * dirname;

   //int            _fd;          // after open()
   //DIR*           _dirp;        // after opendir()


   // FUSE_CHUNKER seems to be the only one that uses stat() instead of lstat()
   virtual bool do_stat_internal() {
      return mar_stat(_item->path, &_item->st);
   }

   MARFS_Path()
      : Path()
   { 
      unset(DID_STAT);
      unset(IS_OPEN_DIR);
      unset(IS_OPEN);
   }

   /**
    * converts the path provided to pftool to the path expected by marfs
    * This strips off the begining of the path
    *
    * @param fs_path The path provided to pftool
    * @return The path expected by marfs
    */
   static const char* fs_to_mar_path(const char* fs_path) {
      return fs_path+6; // adds the number of characters in "/marfs" TODO: make this more robust
   }


public:

   virtual const char* const strerror() {
      return ::strerror(_errno);
   }

   // This runs on Pool<S3_Path>::get(), when initting an old instance
   virtual ~MARFS_Path() {
      close_all();
      //aws_iobuf_reset(_iobuf.get()); TODO: fix
   }

   // TBD: Save our object ID somehow.  Maybe give fake_stat() an extra
   //      arg, and have the factory call a factory_install_list() method
   //      to install it.  Then two S3_Path objects are identical if they
   //      have the same ID.  For now, the NO_IMPL() will help me remember
   //      that this needs doing.
   //
   //   virtual bool operator==(const S3_Path& p) { NO_IMPL(op==); }
   virtual bool identical(const MARFS_Path& p) {
      NO_IMPL(identical);
   }

   // Fill out a struct stat, using metadata from gpfs backend
   static bool mar_stat(const char* path_name, struct stat* st) {
      int rc;

      // get the attributes for the file from marfs
      // TODO: is there a way to detect links
      rc = marfs_getattr(fs_to_mar_path(path_name), st);

      if (rc) {
         errno = errno;
         return false;
      }

      return true;
   }

   // Strict S3 support N:1 via Multi-Part-Upload.  Scality adds a
   // "filejoin" operator, which resembles MPU.  EMC extensions also
   // support writing to object+offset.
   //virtual bool    supports_n_to_1() const  { return true; }


   // Fill out a struct stat, using S3 metadata from an object filesystem.
   // PathFactory can use this (via stat_item()) when determining what
   // subclass to allocate.  Return true for success, false for error.
   //static bool fake_stat(const char* path_name, struct stat* st);


   // TODO: fix
   //virtual const char* const strerror() { return _iobuf->result; }

   virtual bool    supports_n_to_1() const  {
      return false;
   }

   virtual bool    chown(uid_t owner, gid_t group) {
      NO_IMPL(chown);
   }
   virtual bool    chmod(mode_t mode) {
      NO_IMPL(chmod);
   }
   virtual bool    utime(const struct utimbuf* ut) {
      NO_IMPL(utime);
   }


   // TODO: what do I do with mode?
   virtual bool    open(int flags, mode_t mode) {
      int rc;
      const char* marPath;

      marPath = fs_to_mar_path(_item->path);

      // clear the ffi_file structure
      memset(&ffi_file, 0, sizeof(struct fuse_file_info ));

      // check to see if we are createing. if so we must also truncate
      // TODO: this might be a deaper problem with pftool

      if(O_CREAT & flags) {
         /* we need to create the node */
         if(0 != marfs_mknod(marPath, mode, 0)) {
            fprintf(stderr, "marfs_mknod failed\n");
            _errno = errno;
            return false;
         }
         ffi_file.flags = (flags & ~O_CREAT);
      } else {
         ffi_file.flags = flags;
      }

      rc = marfs_open(marPath, &ffi_file);
      if (0 != rc) {
         fprintf(stderr, "marfs_open failed\n");
         _rc = ffi_file.fh;
         _errno = errno;
         return false;
      }

      set(IS_OPEN);
      unset(DID_STAT);
      return true;
   }
   virtual bool    opendir() {
      if(0 != marfs_opendir(fs_to_mar_path(_item->path), &ffi_directory)) {
         _errno = errno;
         return false;  // return _rc;
      }
      set(IS_OPEN_DIR);
      return true;

      //PathInfo info;
      //memset((char*)&info, 0, sizeof(PathInfo));

      ////EXPAND_PATH_INFO(&info, path);
      //if(0 != expand_path_info(&info, fs_to_mar_path(_item->path))) {
      //   fprintf(stderr, "pftool was unable to expand the path \"%s\" for marfs\n");
      //   return false;
      //}

      //if (IS_ROOT_NS(info.ns)) {
      //   // TODO: Deal with root namespaces
      //   fprintf(stderr, "pftool does not yet support root namespaces in marfs\n");
      //   return false;
      //}

      //_dirp = ::opendir(info.post.md_path);
      //if (! bool(_dirp)) {
      //   _errno = errno;
      //   return false;  // return _rc;
      //}
      //set(IS_OPEN_DIR);
      //return true;
   }



   virtual bool    close() {
      int rc;

      rc = marfs_release(fs_to_mar_path(_item->path), &ffi_file);
      if (0 != rc) {
         _errno = errno;
         return false;  // return _rc;
      }

      unset(IS_OPEN);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return true;
   }
   // TBD: See opendir().  For the closedir case, be need to deallocate
   //      whatever is still hanging around from the opendir.
   virtual bool    closedir() {
      if(0 != marfs_releasedir(fs_to_mar_path(_item->path), &ffi_directory)) {
         _errno = errno;
         return false;
      }
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      unset(IS_OPEN_DIR);
      return true;
   }




   // read/write to/from caller's buffer
   // TBD: fix the malloc/free in aws_iobuf_extend/aws_iobuf_reset (see TBD.txt).
   // NOTE: S3 has no access-time, so we don't reset DID_STAT
   virtual ssize_t read( char* buf, size_t count, off_t offset) {
      ssize_t bytes;

      bytes = marfs_read(fs_to_mar_path(_item->path), buf, count, offset, &ffi_file);
      if (bytes == (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }

   // TBD: See opendir()
   virtual bool    readdir(char* path, size_t size) {
      int rc;
      errno = 0;
      if (size)
         path[0] = 0;

      marfs_dirp_t d;
      rc = marfs_readdir_wrapper(&d, fs_to_mar_path(_item->path), &ffi_directory);
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      if (rc > 0) {
         strncpy(path, d.name, size);
         return true;
      } else if (rc == 0) {         // EOF
         return true;
      } else if (rc < 0) {
         perror("readdir failure");
         _errno = errno;
         return bool(_errno == 0);
      }
   }



   virtual ssize_t write(char* buf, size_t count, off_t offset) {
      ssize_t bytes;
      bytes = marfs_write(fs_to_mar_path(_item->path), buf, count, offset, &ffi_file);
      if (bytes ==  (ssize_t)-1)
         _errno = errno;
      unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
      return bytes;
   }


   // NOTE: See comments above this class for discussion of how
   //       "directories" are handled.  If full path includes an obj, we'll
   //       have to assume you intend it as an empty directory.
   virtual bool    mkdir(mode_t mode) {
      NO_IMPL(mkdir);
   }


   // For S3, we'll assume you called unlink(), rather than remove(),
   // because you think that, when you unlink() something, it should
   // continue to be usable until no one is referring to it, or having it
   // open.  S3 don't play that.
   virtual bool    unlink() {
      NO_IMPL(unlink);
   }

   virtual bool    remove() {
      NO_IMPL(remove);
   }
};

#endif









// ---------------------------------------------------------------------------
// PathFactory
//
// If you giveus a raw path-name, we'll parse it, and run a series of
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
// TBD: Add some command-line options to control S3 settings (e.g. debugging,
//      reuse-connections, etc.)
// ---------------------------------------------------------------------------


// template<typename T>  class PathPtr; // fwd decl

class PathFactory {
protected:

   static       uint8_t    _flags;
   static const uint8_t    INIT = 0x01;

   static struct options*  _opts;
   static pid_t            _pid;       // for PLFS
   static int              _rank;      // for PLFS


public:

   // pid and rank are needed for PLFS.  Because the main program always
   // has them, we expect main to call this once, after rank and PID are
   // known, before constructing any paths.  Shouldn't be a hardship for
   // non-PLFS builds.
   //
   // NOTE: Do not call this more than once, because we reset the flags
   // here.
   static void initialize(struct options* opts,
                          int             rank,
                          const char*     src_path,
                          const char*     dest_path) {
      _flags = 0;
      _opts  = opts;
      _pid   = getpid();
      _rank  = rank;

#ifdef S3
      if (! _flags & INIT) {

         // --- Done once-only (per rank).  Perform all first-time inits.
         //
         // The aws library requires a config file, as illustrated below.
         // We assume that the user running the test has an entry in this
         // file, using their login moniker (e.g. `echo $USER`) as the key,
         // something like this:
         //
         //     <user>:<s3_login_id>:<s3_private_key>
         //
         // This file must not be readable by other than user.

         aws_read_config(getenv("USER"));  // requires ~/.awsAuth
         aws_reuse_connections(1);
         aws_set_debug(_opts->verbose >= 3);

         // allow EMC extensions to S3
         s3_enable_EMC_extensions(1);
      }
#endif

      _flags |= INIT;
   }


   // construct appropriate subclass from raw path.  We will have to guess
   // at the type, using stat_item().
   static PathPtr create(const char* path_name) {
      PathItemPtr  item(Pool<path_item>::get(true));
      strncpy(item->path, path_name, PATHSIZE_PLUS);
      item->ftype = TBD;        // invoke stat_item() to determine type

      return create_shallow(item);
   }




   // --- these 2 methods assume the path_item either (a) has ftype
   //     correctly initialized (e.g. via stat_item()), or, (b) has ftype
   //     set to TBD.

   // deep copy
   static PathPtr create(const path_item* item) {
      PathItemPtr  item_ptr(Pool<path_item>::get());
      *item_ptr = *item;        // deep copy;

      return create_shallow(item_ptr);
   }
   // shallow copy  (changes to p->_item will affect caller's item)
   static PathPtr create_shallow(path_item* item) {
      NoOpSharedPtr<path_item>  no_delete(item);
      PathItemPtr               item_ptr(no_delete);

      return create_shallow(item_ptr);
   }



   // --- these 2 methods assume the path_item either (a) has ftype
   //     correctly initialized (e.g. via stat_item()), or, (b) has ftype
   //     set to TBD.

   // deep copy
   static PathPtr create(const PathItemPtr& item) {
      return create(item.get());
   }
   // shallow copy.  If you want a deep copy, and you have a PathItemPtr
   // named item_ptr, then use create(item_ptr.get())
   static PathPtr create_shallow(const PathItemPtr& item) {
      PathPtr p;

      if (! item) {
         PRINT_MPI_DEBUG("PathFactory::create(PathItemPtr&) -- null-pointer\n");
         return PathPtr();
      }

      switch (item->ftype) {
      case NONE:
      case TBD: {
         int rc = stat_item(item.get(), *_opts); // initialize item->ftype
         p = create_shallow(item);      // recurse
         p->did_stat(rc == 0);          // avoid future repeats of failed stat
         return p;
      }

      case REGULARFILE:
      case LINKFILE:
         p = Pool<POSIX_Path>::get();
         break;


#ifdef FUSE_CHUNKER
      case FUSEFILE:
         p = Pool<Fuse_Path>::get(); // TBD
         break;
#endif


#ifdef PLFS
      case PLFSFILE:
         p = Pool<PLFS_Path>::get();
         p->factory_install(2, _pid, _rank);
         break;
#endif


#ifdef S3
      case S3FILE:
         p = Pool<S3_Path>::get();
         break;
#endif

#ifdef MARFS
      case MARFSFILE:
         p = Pool<MARFS_Path>::get();
         break;
#endif

#if GEN_SYNDATA
      case SYNDATA:
         p = Pool<SyntheticDataSource>::get();
         p->factory_install(1, _opts, _rank);
         break;
#endif


#ifdef TAPE
      case PREMIGRATEFILE:
      case MIGRATEFILE:
         p = Pool<Tape_Path>::get(); // TBD
         break;
#endif


      default:
         PRINT_MPI_DEBUG("PathFactory::create(PathItemPtr&) -- unknown type\n", (unsigned)item->ftype);
         return p;
      }

      *p = item;                 // shallow copy of caller's item
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
   PathPtr create(Path& path) {

      if (! path._item.get()) { // impossible?
         errsend(FATAL, "PathFactory::create(Path&) -- no path_item!\n");
         return PathPtr();      // for the compiler
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
   PathPtr create_shallow(Path& path) {
      if (! path._item.get()) { // impossible?
         errsend(FATAL, "PathFactory::create_shallow(Path&) -- no path_item!\n");
         return PathPtr();      // for the compiler
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
   static PathPtr unpack(char* buff, size_t size, int position, MPI_Comm comm=MPI_COMM_WORLD) {
      PathItemPtr work_node_ptr(Pool<path_item>::get());
      MPI_Unpack(buff, size, &position, work_node_ptr.get(), sizeof(path_item), MPI_CHAR, comm);
      return create(work_node_ptr);
   }
#endif


};



#undef NO_IMPL
#undef NO_IMPL_STATIC



#endif // __PATH_H


