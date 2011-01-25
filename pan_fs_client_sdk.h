/*
 * pan_fs_client_sdk.h
 * 
 * IOCTL interface into the PanFS client 
 *
 * @author  dchang
 * @version 3.0
 *
 */
/*
 * Copyright (c) 1999-2006 Panasas, Inc.  All rights reserved.  See the
 * LICENSE file included in this package for licensing details.
 */
#ifndef _PAN_FS_CLIENT__PAN_FS_CLIENT_SDK_H_
#define _PAN_FS_CLIENT__PAN_FS_CLIENT_SDK_H_

#ifndef KERNEL

#if (__FreeBSD__ > 0)
#include <sys/ioccom.h>
#endif /* (__FreeBSD__ > 0) */

#if (__linux__ > 0)
#include <sys/ioctl.h>
#endif /* (__linux__ > 0) */

#endif /* !defined(KERNEL) */

/* Magic number for PanFS File system identification */
#define PAN_FS_CLIENT_MAGIC 0xAAD7AAEA

#define PAN_FS_CLIENT_SDK_IOCTL                ((unsigned int)0x24)

/**
 * An ioctl to get and set CW_OPEN attribute on a file or directory object.
 * All files created under CW_OPEN directory will inherit CW_MODE attribute.
 * open() on a file with CW_OPEN attribute is treated as if open(O_CONCURRENT_WRITE)
 * 
 * @author  dchang
 * @version 3.0
 *
 * @since   3.0
 */

/* mask for pan_fs_client_sdk_attr_get_args_t and
 * pan_fs_client_sdk_attr_set_args_t
 */
#define PAN_FS_CLIENT_SDK_ATTR_MASK__NONE           0x00000000
#define PAN_FS_CLIENT_SDK_ATTR_MASK__CW_OPEN        0x00000001

/* version for pan_fs_client_sdk_attr_get_args_t */
#define PAN_FS_CLIENT_SDK_ATTR_GET_VERSION                  1

typedef struct pan_fs_client_sdk_attr_get_args_s pan_fs_client_sdk_attr_get_args_t;
struct pan_fs_client_sdk_attr_get_args_s {
  unsigned short                   version;
  unsigned int                     mask; 
  unsigned int                     cw_open;  /* On: 1 or non-zero, Off: 0 */
};
#define PAN_FS_CLIENT_SDK_ATTR_GET \
  _IOWR(PAN_FS_CLIENT_SDK_IOCTL,90, pan_fs_client_sdk_attr_get_args_t)

/* version for pan_fs_client_sdk_attr_set_args_t */
#define PAN_FS_CLIENT_SDK_ATTR_SET_VERSION                  1

typedef struct pan_fs_client_sdk_attr_set_args_s pan_fs_client_sdk_attr_set_args_t;
struct pan_fs_client_sdk_attr_set_args_s {
  unsigned short                   version;
  unsigned int                     mask; 
  unsigned int                     cw_open;  /* On: 1 or non-zero, Off: 0 */
};
#define PAN_FS_CLIENT_SDK_ATTR_SET \
  _IOWR(PAN_FS_CLIENT_SDK_IOCTL,91, pan_fs_client_sdk_attr_set_args_t)

#endif /* _PAN_FS_CLIENT__PAN_FS_CLIENT_SDK_H_ */

/* Local Variables:  */
/* indent-tabs-mode: nil */
/* tab-width: 2 */
/* End: */
