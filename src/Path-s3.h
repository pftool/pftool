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
#ifndef  __PATH_S3_H
#define  __PATH_S3_H

#  include<aws4c.h>             // must have version >= 5.2 !


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
        _iobuf(Pool<IOBuf>::get()) // aws_iobuf_new() is just malloc + memset
   {
      //      memset(_iobuf.get(), 0, sizeof(IOBuf));
   }


public:

   virtual ~S3_Path() {
      close_all();              // see Path::operator=()
   }

   virtual void close_all() {
      Path::close_all();
      aws_iobuf_reset(_iobuf.get());
   }

   // TBD: Save our object ID somehow.  Maybe give fake_stat() an extra
   //      arg, and have the factory call a factory_install_list() method
   //      to install it.  Then two S3_Path objects are identical if they
   //      have the same ID.  For now, the NO_IMPL() will help me remember
   //      that this needs doing.
   //
   //   virtual bool operator==(const S3_Path& p) { NO_IMPL(op==); }
   virtual bool identical(S3_Path* p) {
      S3_Path* p2 = dynamic_cast<S3_Path*>(p);
      if (! p2)
         return false;
      NO_IMPL(identical);
   }


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
   virtual bool    lchown(uid_t owner, gid_t group) {
      //      _item->st.st_uid = owner;
      //      _item->st.st_gid = group;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::lchown() ]
   }
   virtual bool    chmod(mode_t mode) {
      //      _item->st.st_mode = mode;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::lchown() ]
   }
   virtual bool    utime(const struct utimbuf* ut) {
      //      _item->st.st_atime = ut->actime;
      //      _item->st.st_mtime = ut->modtime;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::lchown() ]
   }
   virtual bool    utimensat(const struct timespec times[2], int flags) {
      //      _item->st.st_atim.tv_sec  = times[0].tv_sec;
      //      _item->st.st_atim.tv_nsec = times[0].tv_nsec;
      //      _item->st.st_mtim.tv_sec  = times[1].tv_sec;
      //      _item->st.st_mtim.tv_nsec = times[1].tv_nsec;
      //      return apply_stat();
      return true;              // [ see NOTE above S3_Path::lchown() ]
   }

   virtual bool    access(int mode) {
      return true; // untested
   }
   virtual bool    access(int mode, int flags) {
      return true; // untested
   }
   virtual bool    faccessat(int mode, int flags) { NO_IMPL(faccessat); }

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

#endif // __PATH_S3_H
