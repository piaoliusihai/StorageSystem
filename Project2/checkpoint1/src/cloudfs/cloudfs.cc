#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <sys/types.h>
#include <utime.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"
#include <sys/time.h>
#include <fcntl.h> /* Definition of AT_* constants */
#include <sys/stat.h>

#define UNUSED __attribute__((unused))

#define log_struct(st, field, format, typecast) \
  log_msg(logfile, "    " #field " = " #format "\n", typecast st->field)

static struct cloudfs_state state_;
static FILE *logfile;

void log_msg(FILE *logfile, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(logfile, format, ap);
    va_end(ap);
}

void log_retstat(char *func, int retstat)
{
    int errsave = errno;
    log_msg(logfile, "    %s returned %d\n", func, retstat);
    errno = errsave;
}

int log_error(char *func)
{
    int ret = -errno;
    
    log_msg(logfile, "    ERROR %s: %s\n", func, strerror(errno));
    
    return ret;
}

// make a system call, checking (and reporting) return status and
// possibly logging error
int log_syscall(char *func, int retstat, int min_ret)
{
    log_retstat(func, retstat);

    if (retstat < min_ret) {
      retstat = log_error(func);
    }
    return retstat;
}

void log_stat(struct stat *si)
{
  printf("    si:\n");
    
    //  dev_t     st_dev;     /* ID of device containing file */
	log_struct(si, st_dev, %lld, );
	
    //  ino_t     st_ino;     /* inode number */
	log_struct(si, st_ino, %lld, );
	
    //  mode_t    st_mode;    /* protection */
	log_struct(si, st_mode, 0%o, );
	
    //  nlink_t   st_nlink;   /* number of hard links */
	log_struct(si, st_nlink, %d, );
	
    //  uid_t     st_uid;     /* user ID of owner */
	log_struct(si, st_uid, %d, );
	
    //  gid_t     st_gid;     /* group ID of owner */
	log_struct(si, st_gid, %d, );
	
    //  dev_t     st_rdev;    /* device ID (if special file) */
	log_struct(si, st_rdev, %lld,  );
	
    //  off_t     st_size;    /* total size, in bytes */
	log_struct(si, st_size, %lld,  );
	
    //  blksize_t st_blksize; /* blocksize for filesystem I/O */
	log_struct(si, st_blksize, %ld,  );
	
    //  blkcnt_t  st_blocks;  /* number of blocks allocated */
	log_struct(si, st_blocks, %lld,  );

    //  time_t    st_atime;   /* time of last access */
	log_struct(si, st_atime, 0x%08lx, );

    //  time_t    st_mtime;   /* time of last modification */
	log_struct(si, st_mtime, 0x%08lx, );

    //  time_t    st_ctime;   /* time of last status change */
	log_struct(si, st_ctime, 0x%08lx, );
	
}

// struct fuse_file_info keeps information about files (surprise!).
// This dumps all the information in a struct fuse_file_info.  The struct
// definition, and comments, come from /usr/include/fuse/fuse_common.h
// Duplicated here for convenience.
void log_fi (struct fuse_file_info *fi)
{
    log_msg(logfile, "    fi:\n");
    
    /** Open flags.  Available in open() and release() */
    //	int flags;
	log_struct(fi, flags, 0x%08x, );
	
    /** Old file handle, don't use */
    //	unsigned long fh_old;	
	log_struct(fi, fh_old, 0x%08lx,  );

    /** In case of a write operation indicates if this was caused by a
        writepage */
    //	int writepage;
	log_struct(fi, writepage, %d, );

    /** Can be filled in by open, to use direct I/O on this file.
        Introduced in version 2.4 */
    //	unsigned int keep_cache : 1;
	log_struct(fi, direct_io, %d, );

    /** Can be filled in by open, to indicate, that cached file data
        need not be invalidated.  Introduced in version 2.4 */
    //	unsigned int flush : 1;
	log_struct(fi, keep_cache, %d, );

    /** Padding.  Do not use*/
    //	unsigned int padding : 29;

    /** File handle.  May be filled in by filesystem in open().
        Available in all other file operations */
    //	uint64_t fh;
	log_struct(fi, fh, 0x%016llx,  );
	
    /** Lock owner id.  Available in locking operations and flush */
    //  uint64_t lock_owner;
	log_struct(fi, lock_owner, 0x%016llx, );
}

static int UNUSED cloudfs_error(char *error_str)
{
    int retval = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //
    
    fprintf(stderr, "CloudFS Error: %s\n", error_str);

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return retval;
}

/*
 * Initializes the FUSE file system (cloudfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 * 
 * FUSE reference: https://github.com/libfuse/libfuse/blob/fuse_2_6_0/include/fuse.h
 *                 https://libfuse.github.io/doxygen/structfuse__operations.html
 * 
 * 
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED)
{
  cloud_init(state_.hostname);
  return NULL;
}

void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
}

void cloudfs_fullpath(char *func, char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, state_.ssd_path);
    strncat(fpath, path + 1, PATH_MAX);
    log_msg(logfile, "\ncloudfs_fullpath:  func= \"%s\", rootdir = \"%s\", path = \"%s\", fpath = \"%s\"", func, state_.ssd_path, path, fpath);
}

int cloudfs_getattr(const char *path UNUSED, struct stat *statbuf UNUSED)
{
  int retstat;
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_getattr", fpath, path);
  log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", statbuf=0x%08x)\n", fpath, statbuf);
  retstat = log_syscall("cloudfs_getattr", lstat(fpath, statbuf), 0);
  log_stat(statbuf);
  return retstat;
}

int cloudfs_getxattr(const char *path, const char *name, char *value, size_t size) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_getxattr", fpath, path);
  int retstat;
  log_msg(logfile, "\ncloudfs_getxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d)\n", fpath, name, value, size);
  retstat = log_syscall("getxattr", getxattr(fpath, name, value, size), 0);
  if (retstat >= 0) {
    log_msg(logfile, "    value = \"%s\"\n", value);
  }
  return retstat;
}

int cloudfs_setxattr(const char *, const char *, const char *, size_t, int) {
  log_msg(logfile, "cloudfs_setxattr called!\n");
  return 0;
}

int cloudfs_mkdir(const char *path, mode_t mode) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_mkdir", fpath, path);
  log_msg(logfile, "\ncloudfs_mkdir(path=\"%s\", mode=0%3o)\n", fpath, mode);
  return log_syscall("mkdir", mkdir(fpath, mode), 0);
}

int cloudfs_mknod(const char *path, mode_t mode, dev_t dev) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_mknod", fpath, path);
  log_msg(logfile, "\ncloudfs_mknod(path=\"%s\", mode=0%3o)\n", fpath, mode);
  return log_syscall("mknod", mknod(fpath, mode, dev), 0);
}

int cloudfs_open(const char *path, struct fuse_file_info *fi) {
  char fpath[PATH_MAX];
  int fd;
  int retstat = 0;
  cloudfs_fullpath("cloudfs_open", fpath, path);
  log_msg(logfile, "\ncloudfs_open(path=\"%s\"\n", fpath);
  log_fi(fi);
  fd = log_syscall("open", open(fpath, fi->flags), 0);
  if (fd < 0) {
    retstat = log_error("open");
  }
  fi->fh = fd;
  log_fi(fi);
  return retstat;
}

/*
 * Linux reference: https://man7.org/linux/man-pages//man2/read.2.html
 *                  https://man7.org/linux/man-pages//man2/pread.2.html
 */
int cloudfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  log_msg(logfile, "\ncloudfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
  log_fi(fi);
  return log_syscall("cloudfs_read", pread(fi->fh, buf, size, offset), 0);
}

/*
 * Linux reference: https://man7.org/linux/man-pages//man2/pread.2.html
 * 
 * Write data to an open file
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 */
int cloudfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  log_msg(logfile, "\ncloudfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
  log_fi(fi);
  return log_syscall("cloudfs_write", pwrite(fi->fh, buf, size, offset), 0);
}

/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 * 
 * Linux reference: https://man7.org/linux/man-pages/man2/close.2.html
 */
int cloudfs_release(const char *path, struct fuse_file_info *fi) {
  log_msg(logfile, "\ncloudfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
  log_fi(fi);
  return log_syscall("cloudfs_release", close(fi->fh), 0);
}

/*
 * Linux reference: https://man7.org/linux/man-pages/man3/opendir.3.html
 */
int cloudfs_opendir(const char *path, struct fuse_file_info *fi) {
    DIR *dp;
    int retstat = 0;
    char fpath[PATH_MAX];
    cloudfs_fullpath("cloudfs_opendir", fpath, path);
    dp = opendir(fpath);
    log_msg(logfile, "\ncloudfs_opendir(path=\"%s\", fi=0x%08x)\n", fpath, fi);
    if (dp == NULL) {
      retstat = log_error("cloudfs_opendir opendir");
    }
    fi->fh = (intptr_t) dp;
    log_fi(fi);
    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 * 
 * Linux Reference: https://man7.org/linux/man-pages/man3/readdir.3.html
 *                  https://blog.csdn.net/u012349696/article/details/50083787
 */

int couldfs_readdir_contains_lost_found_dir(const char *path, char* filename) {
  char root_path[2] = "/";
  char lost_found_file_path[11] = "lost+found";
  int root_compare = strcmp(root_path, path);
  int filename_compare = strcmp(lost_found_file_path, filename);
  if (root_compare == 0 && filename_compare == 0) return 0;
  return 1;
}

int cloudfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  int retstat = 0;
  DIR *dp;
  struct dirent *de;
    
  log_msg(logfile, "\ncloudfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
    path, buf, filler, offset, fi);
  dp = (DIR *) (uintptr_t) fi->fh;
  de = readdir(dp);
  log_msg(logfile, "    readdir returned 0x%p\n", de);
  if (de == 0) {
    retstat = log_error("bb_readdir readdir");
    return retstat;
  }
  do {
    log_msg(logfile, "calling filler with name %s\n", de->d_name);
    int not_lost_found_directory = couldfs_readdir_contains_lost_found_dir(path, de->d_name);
    if (not_lost_found_directory == 0) continue;
    if (filler(buf, de->d_name, NULL, 0) != 0) {
        log_msg(logfile, "    ERROR bb_readdir filler:  buffer full");
        return -ENOMEM;
    }
  } while ((de = readdir(dp)) != NULL);
  log_fi(fi);
  return retstat;
}

/*
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 */
int cloudfs_access(const char *path, int mode) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_access", fpath, path);
  return log_syscall("access", access(fpath, mode), 0);
}

/*
 * Change the access and modification times of a file with
 * nanosecond resolution
 *
 */
int cloudfs_utimens(const char *path, const struct timespec tv[2]) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_utimens", fpath, path);
  log_msg(logfile, "\ncloudfs_utimens(path=\"%s\")\n", fpath);
  return log_syscall("utimens", utimensat(0, fpath, tv, 0), 0);
}

int cloudfs_chmod(const char *, mode_t) {
  log_msg(logfile, "cloudfs_chmod called!\n");
  return 0;
}

int cloudfs_link(const char *, const char *) {
  log_msg(logfile, "cloudfs_link called!\n");
  return 0;
}

int cloudfs_symlink(const char *, const char *) {
  log_msg(logfile, "cloudfs_symlink called!\n");
  return 0;
}

int cloudfs_readlink(const char *, char *, size_t) {
  log_msg(logfile, "cloudfs_readlink called!\n");
  return 0;
}

int cloudfs_unlink(const char *) {
  log_msg(logfile, "cloudfs_unlink called!\n");
  return 0;
}

int cloudfs_rmdir(const char *) {
  log_msg(logfile, "cloudfs_rmdir called!\n");
  return 0;
}

int cloundfs_truncate(const char *, off_t) {
  log_msg(logfile, "cloundfs_truncate called!\n");
  return 0;
}

int is_directory(const char *path) {
   struct stat statbuf;
   if (stat(path, &statbuf) != 0)
       return 0;
   return S_ISDIR(statbuf.st_mode);
}

int cloudfs_start(struct cloudfs_state *state,
                  const char* fuse_runtime_name) {
  // This is where you add the VFS functions for your implementation of CloudFS.
  // You are NOT required to implement most of these operations, see the writeup
  //
  // Different operations take different types of parameters, see
  // /usr/include/fuse/fuse.h for the most accurate information
  //
  // In addition to this, the documentation for the latest version of FUSE
  // can be found at the following URL:
  // --- https://libfuse.github.io/doxygen/structfuse__operations.html
  struct fuse_operations cloudfs_operations = {
  };
  cloudfs_operations.init = cloudfs_init;
  cloudfs_operations.destroy = cloudfs_destroy;
  cloudfs_operations.getattr = cloudfs_getattr;
  cloudfs_operations.getxattr = cloudfs_getxattr;
  cloudfs_operations.setxattr = cloudfs_setxattr;
  cloudfs_operations.mkdir = cloudfs_mkdir;
  cloudfs_operations.mknod =  cloudfs_mknod;
  cloudfs_operations.open = cloudfs_open;
  cloudfs_operations.read = cloudfs_read;
  cloudfs_operations.write = cloudfs_write;
  cloudfs_operations.release = cloudfs_release;
  cloudfs_operations.opendir = cloudfs_opendir;
  cloudfs_operations.readdir = cloudfs_readdir;
  cloudfs_operations.access = cloudfs_access;
  cloudfs_operations.utimens = cloudfs_utimens;
  cloudfs_operations.chmod = cloudfs_chmod;
  cloudfs_operations.link = cloudfs_link;
  cloudfs_operations.symlink = cloudfs_symlink;
  cloudfs_operations.readlink = cloudfs_readlink;
  cloudfs_operations.unlink = cloudfs_unlink;
  cloudfs_operations.rmdir = cloudfs_rmdir;
  cloudfs_operations.truncate = cloundfs_truncate;
  int argc = 0;
  char* argv[10];
  argv[argc] = (char *) malloc(strlen(fuse_runtime_name) + 1);
  strcpy(argv[argc++], fuse_runtime_name);
  argv[argc] = (char *) malloc(strlen(state->fuse_path) + 1);
  strcpy(argv[argc++], state->fuse_path);
  argv[argc] = (char *) malloc(strlen("-s") + 1);
  strcpy(argv[argc++], "-s"); // set the fuse mode to single thread
  // argv[argc] = (char *) malloc(sizeof("-f") * sizeof(char));
  // argv[argc++] = "-f"; // run fuse in foreground 
  state_  = *state;
  printf("ssd_path: %s\n", state_.ssd_path);
  printf("fuse_path: %s\n", state_.fuse_path);
  printf("hostname: %s\n", state_.hostname);
  logfile = fopen("/home/student/Project/Project2/checkpoint1/src/cloudfs.log", "w");
  setvbuf(logfile, NULL, _IOLBF, 0);
  log_msg(logfile, "cloudfs_init()\n");
  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  return fuse_stat;
}
