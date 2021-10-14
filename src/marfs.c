/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#define LOG_PREFIX "marfs"

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <attr/xattr.h>

#include "marfs.h"
#include "logging.h"

typedef struct marfs_dhandle_struct
{
  DIR *fd;
} * marfs_dhandle;

typedef struct marfs_fhandle_struct
{
  int fd;
} * marfs_fhandle;

marfs_ctxt marfs_init(const char *configpath, marfs_interface type)
{
  return NULL;
}

int marfs_settag(marfs_ctxt ctxt, const char *tag)
{
  return 0;
}

int marfs_configver(marfs_ctxt ctxt, char *buf, ssize_t size, off_t offset)
{
  return 0;
}

int marfs_term(marfs_ctxt ctxt)
{
  return 0;
}

int marfs_faccess(marfs_ctxt ctxt, const char *path, int mode, int flags)
{
  if (path[0] != '/')
  {
    errno = ENOENT;
    return -1;
  }

  return faccessat(-1, path, mode, 0);
}

int marfs_lstat(marfs_ctxt ctxt, const char *path, struct stat *buf)
{
  if (strncmp(path, "/tmp/marfs", 10))
  {
    errno = ENOSYS;
    return -1;
  }

  return lstat(path, buf);
}

int marfs_chmod(marfs_ctxt ctxt, const char *path, mode_t mode)
{
  return chmod(path, mode);
}

int marfs_lchown(marfs_ctxt ctxt, const char *path, uid_t uid, gid_t gid)
{
  return lchown(path, uid, gid);
}

int marfs_rename(marfs_ctxt ctxt, const char *oldpath, const char *newpath)
{
  return rename(oldpath, newpath);
}

int marfs_symlink(marfs_ctxt ctxt, const char *target, const char *linkname)
{
  return symlink(target, linkname);
}

ssize_t marfs_readlink(marfs_ctxt ctxt, const char *path, char *buf, size_t size)
{
  return readlink(path, buf, size);
}

int marfs_unlink(marfs_ctxt ctxt, const char *path)
{
  return unlink(path);
}

int marfs_link(marfs_ctxt ctxt, const char *oldpath, const char *newpath)
{
  return link(oldpath, newpath);
}

int marfs_mkdir(marfs_ctxt ctxt, const char *path, mode_t mode)
{
  return mkdir(path, mode);
}

marfs_dhandle marfs_opendir(marfs_ctxt ctxt, const char *path)
{
  marfs_dhandle dh = (marfs_dhandle)malloc(sizeof(struct marfs_dhandle_struct));

  dh->fd = opendir(path);

  if (dh->fd == NULL)
  {
    free(dh);
    return NULL;
  }

  return dh;
}

struct dirent *marfs_readdir(marfs_dhandle handle)
{
  return readdir(handle->fd);
}

int marfs_closedir(marfs_dhandle handle)
{
  if (closedir(handle->fd))
  {
    return -1;
  }

  free(handle);

  return 0;
}

int marfs_rmdir(marfs_ctxt ctxt, const char *path)
{
  return rmdir(path);
}

marfs_dhandle marfs_chdir(marfs_ctxt ctxt, marfs_dhandle newdir)
{
  return NULL;
}

int marfs_futimens(marfs_fhandle stream, const struct timespec times[2])
{
  return futimens(stream->fd, times);
}

int marfs_statvfs(marfs_ctxt ctxt, const char *path, struct statvfs *buf)
{
  return statvfs(path, buf);
}

int marfs_lsetxattr(marfs_ctxt ctxt, const char *path, const char *name, const char *value, size_t size, int flags)
{
  return 0;
}

ssize_t marfs_lgetxattr(marfs_ctxt ctxt, const char *path, const char *name, void *value, size_t size)
{
  return 0;
}

ssize_t marfs_llistxattr(marfs_ctxt ctxt, const char *path, char *list, size_t size)
{
  return 0;
}

int marfs_lremovexattr(marfs_ctxt ctxt, const char *path, const char *name)
{
  return 0;
}

marfs_fhandle marfs_creat(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, mode_t mode)
{
  marfs_fhandle fh = (marfs_fhandle)malloc(sizeof(struct marfs_fhandle_struct));
  fh->fd = creat(path, mode);

  return fh;
}

marfs_fhandle marfs_open(marfs_ctxt ctxt, const char *path, marfs_flags flags)
{
  int oflags = O_RDONLY;
  if (flags == MARFS_WRITE)
  {
    oflags = O_WRONLY;
  }

  marfs_fhandle fh = (marfs_fhandle)malloc(sizeof(struct marfs_fhandle_struct));
  fh->fd = open(path, oflags);

  if (fh->fd == -1)
  {
    free(fh);
    return NULL;
  }

  return fh;
}

ssize_t marfs_read(marfs_fhandle stream, void *buf, size_t size)
{
  return read(stream->fd, buf, size);
}

ssize_t marfs_write(marfs_fhandle stream, const void *buf, size_t size)
{
  return write(stream->fd, buf, size);
}

off_t marfs_seek(marfs_fhandle stream, off_t offset, int whence)
{
  return lseek(stream->fd, offset, whence);
}

int marfs_chunkbounds(marfs_fhandle stream, int chunknum, off_t *offset, size_t *size)
{
  return 0;
}

int marfs_ftruncate(marfs_fhandle stream, off_t length)
{
  return ftruncate(stream->fd, length);
}

int marfs_extend(marfs_fhandle stream, off_t length)
{
  return 0;
}

int marfs_flush(marfs_fhandle stream)
{
  return 0;
}

int marfs_close(marfs_fhandle stream)
{
  if (close(stream->fd))
  {
    return -1;
  }

  free(stream);

  return 0;
}

int marfs_release(marfs_fhandle stream)
{
  return 0;
}
