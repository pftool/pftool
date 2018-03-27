
#ifndef  __PATH_LIST_H
#define  __PATH_LIST_H

#define LIST_PREFIX "list://"

class List_Path: public Path {
    protected:

        friend class Pool<List_Path>;

        int            _fd;


        virtual bool do_stat_internal() {
            _errno = 0;
            if(!list_stat(_item->path, &_item->st, _flags)) {
                _errno = errno;
                return false;
            }

            _item->ftype = LISTFILE;

            return true;
        }

        static const char* list_sub_path(const char* path) {
            // check to make sure the prefix is on the path
            if (strncmp(path, LIST_PREFIX, strlen(LIST_PREFIX))) {
                return NULL;
            } else {
                return path + strlen(LIST_PREFIX);
            }
        }

        const char* list_sub_path() {
            return list_sub_path(_item->path);
        }

        // private.  Use PathFactory to create paths.
        List_Path()
            : Path(),
            _fd(0)
    { 
    }



    public:

        static bool list_stat(const char* path_name, struct stat* st, FlagType flags) {
            int rc;

            // run appropriate POSIX stat function
            if (flags & FOLLOW)
                rc = ::stat(list_sub_path(path_name), st);
            else
                rc = lstat(list_sub_path(path_name), st);

            if (rc) {
                return false;
            }

            // update file to look like a directory
            st->st_mode = st->st_mode | S_IFDIR;
            st->st_mode = st->st_mode & ~S_IFREG;

            return true;
        }



        virtual ~List_Path() {
            close_all();              // see Path::operator=()
        }

        virtual bool identical(Path* p) { 
            // TODO: Finish
            List_Path* p2 = dynamic_cast<List_Path*>(p);
            return (p2 &&
                    p2->exists() &&
                    (st().st_ino == p2->st().st_ino));
        }


        virtual bool    supports_n_to_1() const  {
            return false;
        }

        virtual const char* const strerror() {
            return ::strerror(_errno);
        }

        virtual bool    lchown(uid_t owner, gid_t group) {
            NO_IMPL(lchown);
            return false;
        }
        virtual bool    chmod(mode_t mode) {
            NO_IMPL(chmod);
            return false;
        }
        // WARNING: This follows links.  Reimplement with lutimes().  Meanwhile, use utimensat()
        virtual bool    utime(const struct utimbuf* ut) {
            NO_IMPL(utime);
            return false;
        }
        virtual bool    utimensat(const struct timespec times[2], int flags) {
            NO_IMPL(utimensat);
            return false;
        }

        virtual bool    access(int mode) {
            // TODO: check path here
            if (_rc = ::access(path(), mode))
                _errno = errno;
            return (_rc == 0);
        }
        // path must not be relative
        virtual bool    faccessat(int mode, int flags) {
            // TODO: check path here
            if (_rc = ::faccessat(-1, list_sub_path(), mode, flags))
                _errno = errno;
            return (_rc == 0);
        }

        // see comments at Path::open()
        // NOTE: We don't protect user from calling open when already open
        virtual bool    open(int flags, mode_t mode) {
            NO_IMPL(open);
            return false;
        }
        virtual bool    opendir() {
            _fd = ::open(list_sub_path, flags, mode);
            if (_fd < 0) {
                _rc = _fd;
                _errno = errno;
                return false; // return _fd;
            }
            set(IS_OPEN_DIR);
            return true;
        }


        // NOTE: We don't protect user from calling close when already closed
        virtual bool    close() {
            NO_IMPL(close);
            return true; // return _fd;
        }
        virtual bool    closedir() {
            // TODO: check, but I think it is ok
            _rc = ::close(_fd);
            if (_rc < 0) {
                _errno = errno;
                return false;  // return _rc;
            }
            unset(DID_STAT);          // instead of updating _item->st, just mark it out-of-date
            unset(IS_OPEN_DIR);
            return true;
        }

        virtual char *realpath(char *resolved_path) {
            char buf[PATH_MAX];
            char *ret;
            ret = ::realpath(list_sub_path(), buf);
            if(NULL == ret) {
                _errno = errno;
            } else {
                strcpy(resolved_path, LIST_PREFIX);
                strcpy(resolved_path+strlen(LIST_PREFIX), buf);
            }
            return ret;
        }


        virtual ssize_t read( char* buf, size_t count, off_t offset) {
            NO_IMPL(read);
            return 0;
        }
        virtual bool    readdir(char* path, size_t size) {
            // TODO: write this function
        }


        virtual ssize_t write(char* buf, size_t count, off_t offset) {
            NO_IMPL(write);
            return 0;
        }
        virtual bool    mkdir(mode_t mode) {
            NO_IMPL(mkdir);
            return false;
        }

        virtual bool    remove() {
            return unlink();
        }

        virtual bool    unlink() {
            NO_IMPL(unlink);
            return false;
        }

        // WARNING: this behaves like POSIX readlink(), not writing final '\0'
        virtual ssize_t readlink(char *buf, size_t bufsiz) {
            NO_IMPL(readlink);
            return -1;
        }

        virtual bool    symlink(const char* link_name) {
            NO_IMPL(symlink);
            return false;
        }
};

#endif // __PATH_LIST_H

