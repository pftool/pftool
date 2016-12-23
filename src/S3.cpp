#include "pftool.h"
#include "Path.h"

#include "aws4c_extra.h"        // XML-parsing tools

// definitions of static vector-members for Pool templated-classes
//template<typename T> std::vector<T*> Pool<T>::_pool;


//// defns for static PathFactory members
//uint8_t          PathFactory::_flags   = 0;
//struct options*  PathFactory::_opts    = NULL;
//pid_t            PathFactory::_pid     = 0;       // for PLFS
//int              PathFactory::_rank    = 1;
//int              PathFactory::_n_ranks = 1;

// Parse metadata for a given path, and translate into a 'struct stat'.
// Having a sat struct allows pftool to compare with results from stat() on
// POSIX fielsystems (or with similar faked versions from plfs_getattr()).
// However, there's a lot of cludgery needed, to shoehorn the kind of
// metadata we get from S3 into a stat struct (see NOTEs, inline).
//
// Static method, so it can be used from the PathFactory to test paths that
// might be S3.
//
//      struct stat {
//          dev_t     st_dev;     /* ID of device containing file */
//          ino_t     st_ino;     /* inode number */
//          mode_t    st_mode;    /* protection */
//          nlink_t   st_nlink;   /* number of hard links */
//          uid_t     st_uid;     /* user ID of owner */
//          gid_t     st_gid;     /* group ID of owner */
//          dev_t     st_rdev;    /* device ID (if special file) */
//          off_t     st_size;    /* total size, in bytes */
//          blksize_t st_blksize; /* blocksize for file system I/O */
//          blkcnt_t  st_blocks;  /* number of 512B blocks allocated */
//          time_t    st_atime;   /* time of last access */
//          time_t    st_mtime;   /* time of last modification */
//          time_t    st_ctime;   /* time of last status change */
//      };
//
// Here's sample response XML
// [see http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETacl.html]
//
//      HTTP/1.1 200 OK
//      x-amz-id-2: eftixk72aD6Ap51TnqcoF8eFidJG9Z/2mkiDFu8yU9AS1ed4OpIszj7UDNEHGran
//      x-amz-request-id: 318BC8BC148832E5
//      Date: Wed, 28 Oct 2009 22:32:00 GMT
//      Last-Modified: Sun, 1 Jan 2006 12:00:00 GMT
//      Content-Length: 124
//      Content-Type: text/plain
//      Connection: close
//      Server: AmazonS3
//      
//      <AccessControlPolicy>
//        <Owner>
//          <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
//          <DisplayName>CustomersName@amazon.com</DisplayName>
//        </Owner>
//        <AccessControlList>
//          <Grant>
//            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
//      			xsi:type="CanonicalUser">
//              <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
//              <DisplayName>CustomersName@amazon.com</DisplayName>
//            </Grantee>
//            <Permission>FULL_CONTROL</Permission>
//          </Grant>
//        </AccessControlList>
//      </AccessControlPolicy> 
//
//


bool
S3_Path::fake_stat(const char* path_name, struct stat* st) {

   //   NO_IMPL_STATIC(fake_stat, S3_Path); // TBD

   // get these from the pool, instead of was_iobuf_new, to avoid mallocs
   IOBufPtr b_ptr(Pool<IOBuf>::get());
   IOBuf*   b = b_ptr.get();

   // defaults
   memset((char*)st, 0, sizeof(struct stat));


   // .................................................................
   // device    (N/A)
   // inode     (N/A)
   // .................................................................


   // .................................................................
   // mode
   //
   //     Use a "?acl" query to get permissions metadata.
   //     Use a HEAD request to get user-metadata
   //
   //     NOTE: There's no notion of "execute permission" in S3 ACLs.
   //
   //     NOTE: The "owner" in the ACLs for a given object/bucket is some
   //           64-hex-digit number.  We can also find the "DisplayName" for
   //           that owner.  Neither of these have any necessary connection
   //           with UIDs in a POSIX filesystem.
   //
   //     NOTE: We might be able to force notions of access for "group" and "other"
   //           using something like the "predefined groups", mentioned here:
   //
   //           http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   //
   //           A problem is that the ACL can apparent grant different permissions
   //           to a multitude of groups.  It's also not clear to me whether an
   //           EMC installation at LANL would have similar groups.  Can we create
   //           such groups?  If so, should we get a list of them from the
   //           service?  Maybe we should just assume that the first non-owner
   //           with permissions is a group.  Then the "all users" group becomes
   //           "other"?
   // .................................................................

   // Need to parse path to figure out wether this is a bucket, or not.
   // These elements are like S3_Path members.
   std::string  host;
   std::string  bucket;
   std::string  obj;

   bool trailing_slash = S3_Path::parse_host_bucket_object(host, bucket, obj, path_name);

   if (! host.size()) {
      errsend_fmt(NONFATAL, "couldn't parse host from '%s'\n", path_name);
      return false;
   }
   s3_set_host(host.c_str());
   if (bucket.size())
      s3_set_bucket(bucket.c_str());


   // query for metadata, capturing XML response
#if 0
   std::string  acl_path(obj);
   acl_path += (trailing_slash ? "/?acl" : "?acl");
#else
   std::string  acl_path(obj + "?acl");
#endif
   
   const size_t xml_buf_size = 1024 * 256;		// plenty for ACL-XML or error response (?)
   char xml_buf[xml_buf_size];
   aws_iobuf_reset(b);
   aws_iobuf_extend_static(b, xml_buf, xml_buf_size);
   aws_iobuf_growth_size(b, xml_buf_size); 		// in case we overflow

   AWS4C_CHECK( s3_get(b, (char*)acl_path.c_str()) );

   ///   AWS4C_CHECK_OK( b );
   if (b->code != 200) {
      if(b->code != 404)				 // e.g. 404 'Not Found'
//        errsend_fmt(NONFATAL, "unexpected AWS query result for %s: %d (%s)\n", path_name, b->code, b->result);
        fprintf(stderr, "unexpected AWS query result for %s: %d (%s)\n", path_name, b->code, b->result);
      return false; 
   }
   // prepare to parse response-XML
   aws_iobuf_realloc(b);
   xmlDocPtr doc = xmlReadMemory(b->first->buf, b->first->len, NULL, NULL, 0);
   if (! doc) {
      errsend_fmt(NONFATAL, "couldn't xmlReadMemory in ACL-response from '%s'\n",
                  acl_path.c_str());
      return false;
   }

   char* uid       = NULL;
   char* uid_name  = NULL;
   char* uid_perms = NULL;

   // navigate parsed XML-tree to find "<Owner>"
   xmlNode* root_element = xmlDocGetRootElement(doc);
   xmlNode* owner = find_xml_element_named(root_element, "Owner");
   if (! owner) {
      errsend_fmt(FATAL, "No 'owner' in ACL-response from '%s'\n", acl_path.c_str());
      return false;
   }
   else {

      // find owner's "ID" (this will be some 64-hex-digit number)
      uid       = (char*)find_element_named(owner, "ID");
      uid_name  = (char*)find_element_named(owner, "DisplayName"); // email address?

      // find the list of "grant" elements (XML siblings) in the ACL XML
      xmlNode* grant = find_xml_element_named(root_element, "Grant");
      if (! grant) {
         errsend_fmt(FATAL, "No 'grant' in ACL-response from '%s'\n", acl_path.c_str());
         return false;
      }
      else {

         // find the grant that has the same ID as "Owner"
         for (xmlNode* gr=grant; gr; gr=gr->next) {
            char* id = (char*)find_element_named(gr, "ID");
            if (!strcmp(id, uid)) {

               // get the permissions for the grant matching Owner's ID.
               // this will be one of the following value (strings):
               //   FULL_CONTROL | WRITE | WRITE_ACP | READ | READ_ACP
               uid_perms = (char*)find_element_named(gr, "Permission");
               break;
            }
         }

         if (! uid_perms) {
            errsend_fmt(FATAL, "No 'grant' in ACL-response from '%s', for user '%s'\n",
                        acl_path.c_str(), uid_name);
            return false;
         }
      }
   }

   // pretend it's possible to translate S3 ACL permissions into POSIX.
   if      (! strcmp(uid_perms, "READ"))
      st->st_mode |= (S_IRUSR);
   else if (! strcmp(uid_perms, "WRITE"))
      st->st_mode |= (S_IWUSR);
   else if (! strcmp(uid_perms, "FULL_CONTROL"))
      st->st_mode |= (S_IRUSR | S_IWUSR);  // what, I can't "execute" this object?


   // There is no easy way to tell if this thing "is a directory". We could
   // query the bucket with "?prefix=<obj>&Delimiter=/", then look through
   // "common substrings" to see whether this path appeared, but that
   // sounds expensive.  We can also store custom metadata, when we create
   // objects containing "/", at objects representing each of the
   // directory-components along the way.  Or, instead of that, we could
   // create directories with names ending in "/" for each directory
   // component; then by adding a slash at the end of the current path, we
   // could just ask whether that object (with the trailing slash) exists,
   // and use that to indicate that this is a directory, but that's awkward
   // for directory listings.
   //
   // NOTE: When copying to S3 we have an advantage that S3_Path::mkdir()
   //       is called, which initializes the struct stat.  Those objects
   //       passed around in pftool will already be known to be directories,
   //       so fake_stat() will not be used.

   // (a) if it's a bucket with no path, then it's a directory
   if (!obj.size() || !strcmp(obj.c_str(), "/"))
      st->st_mode |= (__S_IFDIR);

   // (b) if path has more than 1 char and ends in "/", then it's a directory
   //
   // NOTE: This is a short-cut that lets us avoid the expensive operations
   //       in (c).  Here's how this is useful: When doing a readdir on an
   //       S3 "directory", we use the prefix+delimiter query described
   //       above.  (In this case, it's not inefficient.)  For example,
   //       suppose in bucket "A" we have objects with the following names:
   //
   //     
   //           B/C/D
   //           B/C2
   //
   //       Then the query for contents of the "directory" named "B" could
   //       look like this: "http://hostname/A?prefix=B/&delimiter=/" and
   //       the results would be:
   //
   //           C/
   //           C2
   //
   //       If we leave the slash in, then we can immediately determine
   //       that "C" is a directory, though for "C2", we'd have to query
   //       metadata, because it might have been created as an object
   //       (possibly having no contents), or as an empty "directory"
   //       (definitely having no objects below it).
   else if (trailing_slash)
      st->st_mode |= (__S_IFDIR);

#if 1
   // TBD: Defer this until needed.  Maybe nobody is going to ask whether
   //      this is a directory, in which case the effort here is wasted.
   //      Instead, return-value (or something) indicates we didn't check
   //      metadata yet.  If someone calls S3_Path::is_dir(), and we
   //      don't already know that it is a dir, *then* we do this.


   // (c) if user meta-data says it's a directory, then it's a directory.
   else if (obj.size()) {
      IOBufPtr b2_ptr(Pool<IOBuf>::get());
      IOBuf*   b2 = b2_ptr.get();

      // parse user-defined meta-data into <b>->meta
      AWS4C_CHECK( s3_head(b, (char*)obj.c_str()) );
      if (b->code == 200 && b->meta) {

         // meta-data includes key="mode_bits" ?
         //
         // NOTE: We only use the file-type bits in the meta-data.  The ACL
         //       (parsed above) says something real about access-mode
         //       bits.  I think we shouldn't ignore that.  Therefore, we
         //       OR the meta-data value with __S_IFMT, before installing,
         //       to make sure we aren't munging permissions.
         const char* mode_bits_string = aws_metadata_get((const MetaNode**)&(b->meta), "mode_bits");
         if (mode_bits_string) {
            mode_t mode;
            if (sscanf(mode_bits_string, "0x%08x", &mode) != 1) {
               errsend_fmt(FATAL, "Couldn't parse 'mode_bits' meta-data value ('%s') for '%s'\n",
                           mode_bits_string, acl_path.c_str());
               aws_iobuf_reset(b2);
               return false;
            }
            st->st_mode |= (mode & __S_IFMT);
         }
      }
      aws_iobuf_reset(b2);
   }
#endif


   // .................................................................
   // nlink     (N/A)
   // UID       (N/A)
   // GID       (N/A)
   // rdev      (N/A)
   // .................................................................


   // .................................................................
   // size
   // .................................................................
   st->st_size = b->contentLen;


   // .................................................................
   // block-size
   // .................................................................
   st->st_blksize = 512;        // ... imagine owning the Brooklyn Bridge!


   // .................................................................
   // block-count
   // .................................................................
   st->st_blocks = b->contentLen / 512;
   if (b->contentLen & (512 -1))
      ++ st->st_blocks;         // ... all the bridge tolls would go to you!


   // .................................................................
   // atime
   // mtime
   // ctime
   //
   //     NOTE: S3 doesn't maintain distinct ctime, atime, and mtime.
   //           The best we can do is use the returned
   //           modification-time to fill *all* of those fields.
   //
   //           [If we really wanted to be aggressive, we could try to
   //           chase down all existing versions of this
   //           object/bucket, and use the earliest as the ctime.]
   //
   //
   //     NOTE: Amazon uses two different date-formats for reporting
   //           object modification-times: (1) is in the response
   //           header for GET on an object, (2) is in the XML for
   //           individual objects in the response to GET on a bucket.
   //
   //           (1)   "Wed, 12 Oct 2009 17:50:00 GMT"
   //           (2)   "2009-10-12T17:50:30.000Z"        (ISO8601 in XML)
   //
   //           (Apparently, the ACL-query should return format (1) in
   //           the headers, for both objects and buckets.)
   //
   // .................................................................

   // extract modification-time (string).  Translate to time_t.  Use this
   // for ctime, mtime, and atime.
   if (b->lastMod) {
      struct tm   tm;
      time_t      mod_time;
      strptime(b->lastMod, "%a, %d %b %Y %H:%M:%S %Z", &tm);
      mod_time = mktime(&tm);

      st->st_atime = mod_time;
      st->st_mtime = mod_time;
      st->st_ctime = mod_time;
   }
   else {
      st->st_atime = 0;
      st->st_mtime = 0;
      st->st_ctime = 0;
   }


   // free storage for parsed XML tree
   xmlFreeDoc(doc);

   // These are now gotten from the pool, and all buffers we added are
   // static, so no need to free/reset them.  However, it's possible that
   // the response was longer than xml_buf_size, which would have forced
   // the system to extend with dynamically-allocated data.  Also, somone
   // may explicitly add dynamically-allocated buffers.  Thus, it's
   // probably wise to free, here.

   ///   aws_iobuf_free(response);
   aws_iobuf_reset(b);

   return true;
}

