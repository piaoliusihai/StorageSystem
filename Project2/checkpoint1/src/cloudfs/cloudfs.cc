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
#include "libs3.h"

#define UNUSED __attribute__((unused))

#define log_struct(st, field, format, typecast) \
  log_msg(logfile, "    " #field " = " #format "\n", typecast st->field)

static struct cloudfs_state state_;
static FILE *logfile;
static FILE *infile;
static FILE *outfile;

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

int put_buffer_in_cloud(char *buffer, int bufferLength) {
  int retstat = fread(buffer, 1, bufferLength, infile);
  log_msg(logfile, "put_buffer %d, retstat %d\n", bufferLength, retstat);
  return retstat;
}

int get_buffer_save_in_file(const char *buffer, int bufferLength) {
  log_msg(logfile, "get_buffer %d\n", bufferLength);
  int retstat = fwrite(buffer, 1, bufferLength, outfile);
  return retstat;
}

int cloudfs_list_bucket(const char *key, time_t modified_time, uint64_t size) {
  log_msg(logfile, "cloudfs_list_bucket\n");
  log_msg(logfile, "%s %lu %d\n", key, modified_time, size);
  return 0;
}

int cloudfs_list_service(const char *bucketName) {
  log_msg(logfile, "%s\n", bucketName);
  return 0; 
}

void generate_bucket_name(const char *path, char bucket_name[PATH_MAX], char file_name[PATH_MAX]) {
  int last_slash_index = 0;
  for (int i = 0; i < strlen(path); i++) {
    if (path[i] == '/') {
      last_slash_index = i;
    }
  }
  int i = 0, j = 0;
  if (path[i] == '/') i++;
  for (; i <= last_slash_index; i++, j++) {
    if (path[i] == '/') {
      bucket_name[j] = '-';
    } else {
      bucket_name[j] = path[i];
    }
  }
  bucket_name[j] = '\0';
  i = 0;
  j = last_slash_index + 1;
  for (; j < strlen(path); j++, i++) {
    if (path[j] == '/') {
      file_name[j] = '-';
    } else {
      file_name[i] = path[j];
    }
  }
  file_name[i] = '\0';
  log_msg(logfile, "\ngenerate_bucket_name(path=\"%s\", bucket_name=\"%s\", file_name=\"%s\")\n", path, bucket_name, file_name);
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
  char on_cloud[2];
  char on_cloud_size[64];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  if (oncloud_signal > 0) {
    cloudfs_getxattr(path, "user.on_cloud_size", on_cloud_size, 64);
    log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", oncloud=\"%s\", oncloud_size=\"%d\")\n", fpath, on_cloud, atoi(on_cloud_size));
    statbuf->st_size = atoi(on_cloud_size);
  }
  log_stat(statbuf);
  return retstat;
}

/** Get extended attributes */
int cloudfs_getxattr(const char *path, const char *name, char *value, size_t size) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_getxattr", fpath, path);
  int retstat;
  log_msg(logfile, "\ncloudfs_getxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d)\n", fpath, name, value, size);
  retstat = log_syscall("getxattr", lgetxattr(fpath, name, value, size), 0);
  if (retstat >= 0) {
    log_msg(logfile, "    value = \"%s\"\n", value);
  }
  return retstat;
}

/** Set extended attributes https://man7.org/linux/man-pages/man2/setxattr.2.html*/
int cloudfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  char fpath[PATH_MAX]; 
  log_msg(logfile, "\ncloudfs_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
    path, name, value, size, flags);
  cloudfs_fullpath("cloudfs_setxattr", fpath, path);
  return log_syscall("cloudfs_setxattr", lsetxattr(fpath, name, value, size, flags), 0);
}

int cloudfs_removexattr(const char *path, const char *name) {
  char fpath[PATH_MAX]; 
  log_msg(logfile, "\ncloudfs_removexattr(path=\"%s\", name=\"%s\")\n", path, name);
  cloudfs_fullpath("cloudfs_removexattr", fpath, path);
  return log_syscall("cloudfs_removexattr", lremovexattr(fpath, name), 0);
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
  char on_cloud[2];
  char on_cloud_size[64];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  struct stat statbuf;
  lstat(fpath, &statbuf);
  if (oncloud_signal > 0) {
    log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time before=%ld %ld, last modificaton time before=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    struct timespec timesaved[2];
    mode_t old_mode =  statbuf.st_mode;
    timesaved[1] = statbuf.st_mtim;
    cloudfs_getxattr(path, "user.on_cloud_size", on_cloud_size, 64);
    log_msg(logfile, "\ncloudfs_open(path=\"%s\", oncloud=\"%s\", oncloud_size=\"%d\")\n", path, on_cloud, atoi(on_cloud_size));
    char bucket_name[PATH_MAX];
    char file_name[PATH_MAX];
    generate_bucket_name(path, bucket_name, file_name);
    strcat(bucket_name, file_name);
    log_msg(logfile, "get object with bucket name %s and file name %s\n", bucket_name, file_name);
    cloud_list_bucket(bucket_name, cloudfs_list_bucket);
    lstat(fpath, &statbuf);
    log_stat(&statbuf);
    cloudfs_chmod(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    log_msg(logfile, "before getting from server\n");
    outfile = fopen(fpath, "wb");
    cloud_get_object(bucket_name, bucket_name, get_buffer_save_in_file);
    fclose(outfile);
    lstat(fpath, &statbuf);
    log_stat(&statbuf);
    log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time after=%ld %ld, last modificaton time after=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    timesaved[0] = statbuf.st_atim;
    int reverttime = utimensat(0, fpath, timesaved, 0);
    log_msg(logfile, "revert time result %d\n", reverttime);
    lstat(fpath, &statbuf);
    log_stat(&statbuf);
    log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time ultimate=%ld %ld, last modificaton time ultimate=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    cloudfs_chmod(path, old_mode);
  }
  if (fd < 0) {
    retstat = log_error("open");
    return retstat;
  }
  fi->fh = fd;
  log_fi(fi);
  return retstat;
}

/*
 * Linux reference: https://man7.org/linux/man-pages//man2/read.2.html
 *                  https://man7.org/linux/man-pages//man2/pread.2.html
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 */
int cloudfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_open", fpath, path);
  log_msg(logfile, "\ncloudfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
  log_fi(fi);
  int restat = log_syscall("cloudfs_read", pread(fi->fh, buf, size, offset), 0);
  return restat;
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
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_open", fpath, path);
  log_msg(logfile, "\ncloudfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
  log_fi(fi);
  struct stat statbuf;
  int getattr_result = cloudfs_getattr(path, &statbuf);
  log_stat(&statbuf);
  log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time before release=%ld %ld, last modificaton time before release=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
  int file_size = statbuf.st_size;
  int retstat = log_syscall("cloudfs_release", close(fi->fh), 0);
  char on_cloud[2];
  char on_cloud_size[64];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  if (file_size > state_.threshold) {
    char bucket_name[PATH_MAX];
    char file_name[PATH_MAX];
    struct timespec timesaved[2];
    mode_t old_mode =  statbuf.st_mode;
    timesaved[1] = statbuf.st_atim;
    timesaved[1] = statbuf.st_mtim;
    generate_bucket_name(path, bucket_name, file_name);
    strcat(bucket_name, file_name);
    log_msg(logfile, "\ncloudfs_release(path=\"%s\") size %d bigger than cloudfs threshold %d\n", path, file_size, state_.threshold);
    log_msg(logfile, "Create bucket with bucket name %s\n", bucket_name);
    S3Status s3status = cloud_create_bucket(bucket_name);
    log_msg(logfile, "S3Status %d\n", s3status);
    cloudfs_chmod(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    infile = fopen(fpath, "rb");
    cloud_put_object(bucket_name, bucket_name, statbuf.st_size, put_buffer_in_cloud);
    fclose(infile);
    s3status = cloud_list_bucket(bucket_name, cloudfs_list_bucket);
    log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
    cloudfs_setxattr(path, "user.on_cloud", "1", strlen("1"), 0);
    char file_size_char[64];
    sprintf(file_size_char, "%d", file_size);
    cloudfs_setxattr(path, "user.on_cloud_size", file_size_char, strlen(file_size_char), 0);
    log_msg(logfile, "Set cloud size %s\n", file_size_char);
    log_msg(logfile, "cloud_put_object end\n");
    int fd = open(fpath, O_RDWR);
    ftruncate(fd,0);
    lseek(fd,0,SEEK_SET);
    close(fd);
    int getattr_result = cloudfs_getattr(path, &statbuf);
    log_stat(&statbuf);
    log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time after release=%ld %ld, last modificaton time after release=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    int reverttime = utimensat(0, fpath, timesaved, 0);
    log_msg(logfile, "revert time result %d\n", reverttime);
    lstat(fpath, &statbuf);
    log_stat(&statbuf);
    log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time ultimate=%ld %ld, last modificaton time ultimate=%ld %ld)\n", 
      fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    cloudfs_chmod(path, old_mode);
  } else if (file_size <= state_.threshold && oncloud_signal > 0) {
    log_msg(logfile, "\ncloudfs_release(path=\"%s\") size %d smaller than cloudfs threshold %d, and exists on cloud\n", path, file_size, state_.threshold);
    char bucket_name[PATH_MAX];
    char file_name[PATH_MAX];
    generate_bucket_name(path, bucket_name, file_name);
    strcat(bucket_name, file_name);
    log_msg(logfile, "Delete bucket with bucket name %s\n", bucket_name);
    S3Status s3status = cloud_delete_object(bucket_name, bucket_name);
    log_msg(logfile, "S3Status %d\n", s3status);
    s3status = cloud_delete_bucket(bucket_name);
    log_msg(logfile, "S3Status %d\n", s3status);
  }
  return retstat;
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
 */
int cloudfs_utimens(const char *path, const struct timespec tv[2]) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_utimens", fpath, path);
  struct stat stat_buf;
  cloudfs_getattr(path, &stat_buf);
  int retstat = log_syscall("utimens", utimensat(0, fpath, tv, 0), 0);
  cloudfs_getattr(path, &stat_buf);
  return retstat;
}

/** Change the permission bits of a file */
int cloudfs_chmod(const char *path, mode_t mode) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_chmod", fpath, path);
  log_msg(logfile, "\ncloudfs_chmod(path=\"%s\", mode=0%03o)\n", fpath, mode);
  return log_syscall("cloudfs_chmod", chmod(fpath, mode), 0);
}

/** Create a hard link to a file */
int cloudfs_link(const char *oldpath, const char *newpath) {
  char foldpath[PATH_MAX];
  char fnewpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_link", foldpath, oldpath);
  cloudfs_fullpath("cloudfs_link", fnewpath, newpath);
  log_msg(logfile, "\ncloudfs_link(oldpath=\"%s\", newpath=\"%s\")\n", foldpath, fnewpath);
  return log_syscall("cloudfs_link", link(foldpath, fnewpath), 0);
}

/** Create a symbolic link */
int cloudfs_symlink(const char *target, const char *link) {
    char flinkpath[PATH_MAX];
    log_msg(logfile, "\ncloudfs_symlink(target=\"%s\", link=\"%s\")\n", target, link);
    cloudfs_fullpath("cloudfs_symlink", flinkpath, link);
    return log_syscall("symlink", symlink(target, flinkpath), 0);
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
int cloudfs_readlink(const char *path, char *buf, size_t bufsize) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_readlink", fpath, path);
  log_msg(logfile, "\ncloudfs_readlink(path=\"%s\", fpath=\"%s\"\n", path, fpath);
  int retstat = log_syscall("cloudfs_readlink", readlink(fpath, buf, bufsize - 1), 0);
  if (retstat >= 0) {
	  buf[retstat] = '\0';
	  retstat = 0;
	  log_msg(logfile, "read result from readlink buf=\"%s\"\n", buf);
  }
  return retstat;
}

/*
 * Linux Reference: https://linux.die.net/man/2/unlink
 */
int cloudfs_unlink(const char *path) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_unlink", fpath, path);
  log_msg(logfile, "\ncloudfs_unlink(path=\"%s\")\n", fpath);
  char on_cloud[2];
  char on_cloud_size[64];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  if (oncloud_signal > 0) {
    char bucket_name[PATH_MAX];
    char file_name[PATH_MAX];
    generate_bucket_name(path, bucket_name, file_name);
    strcat(bucket_name, file_name);
    log_msg(logfile, "Delete bucket with bucket name %s\n", bucket_name);
    S3Status s3status = cloud_delete_object(bucket_name, bucket_name);
    log_msg(logfile, "S3Status %d\n", s3status);
    s3status = cloud_delete_bucket(bucket_name);
    log_msg(logfile, "S3Status %d\n", s3status);
  }
  return log_syscall("cloudfs_unlink", unlink(fpath), 0);
}

/*
 * Linux Reference: https://linux.die.net/man/2/rmdir
 */
int cloudfs_rmdir(const char *path) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_rmdir", fpath, path);
  log_msg(logfile, "\ncloudfs_rmdir(path=\"%s\")\n", fpath);
  int retstat = log_syscall("cloudfs_rmdir", rmdir(fpath), 0);
  cloud_list_service(cloudfs_list_service);
  return retstat;
}

int cloundfs_truncate(const char *path, off_t length) {
  char fpath[PATH_MAX];
  cloudfs_fullpath("cloudfs_rmdir", fpath, path);
  log_msg(logfile, "\ncloundfs_truncate(path=\"%s\", length=%d)\n", fpath, length);
  int retstat = log_syscall("cloundfs_truncate", truncate(fpath, length), 0);
  char file_size_char[64];
  sprintf(file_size_char, "%d", length);
  cloudfs_setxattr(path, "user.on_cloud_size", file_size_char, strlen(file_size_char), 0);
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
  cloudfs_operations.removexattr = cloudfs_removexattr;
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
  logfile = fopen("/tmp/cloudfs.log", "w");
  setvbuf(logfile, NULL, _IOLBF, 0);
  log_msg(logfile, "cloudfs_init()\n");
  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  return fuse_stat;
}
