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
#include "unordered_map"
#include <openssl/md5.h>
#include <archive.h>
#include <archive_entry.h>
#include "libs3.h"
#include <map>
#include <iostream>
#include <deque>
#include <vector>
#include "../snapshot/snapshot-api.h"

#define UNUSED __attribute__((unused))
#define CLOUDFS_IOCTL_NAME "/.snapshot"
#define CLOUDFS_INSTALL_DIR_NAME "/snapshot_"

#define log_struct(st, field, format, typecast) \
  log_msg(logfile, "    " #field " = " #format "\n", typecast st->field)

struct file_content_index {
  int segment_index;
  int offset;
  int size;
  std::string md5;
  int complete; // if 1, then complete
};

static struct cloudfs_state state_;
static FILE *logfile;
static FILE *infile;
static FILE *outfile;
static std::unordered_map<std::string, int> md5_to_frequency_map;
static rabinpoly_t *rp;
static int uploadFdCh2;
static int uploadFdCh2Offset;
static int verbosePrint = 1;
static std::unordered_map<std::string, bool> keys_in_bucket_map;

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
void log_msg(FILE *logfile, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    if (verbosePrint >= 1) vfprintf(logfile, format, ap);
    va_end(ap);
}

// Compute absolute path from path and saved result in fpath
void cloudfs_fullpath(char *func, char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, state_.ssd_path);
    strncat(fpath, path + 1, strlen(path) - 1);
    log_msg(logfile, "\ncloudfs_fullpath:  func= \"%s\", rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n", func, state_.ssd_path, path, fpath);
}

std::vector<std::string> split(const std::string& str, const std::string& delim) {  
	std::vector<std::string> res;  
	if("" == str) return res;   
	char * strs = new char[str.length() + 1]; 
	strcpy(strs, str.c_str());   
 
	char * d = new char[delim.length() + 1];  
	strcpy(d, delim.c_str());  
 
	char *p = strtok(strs, d);  
	while(p) {  
		std::string s = p;  
		res.push_back(s);
		p = strtok(NULL, d);  
	}
	return res;
}

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
// Retrurn negative error if some error happens after system call.
void log_retstat(char *func, int retstat)
{
    int errsave = errno;
    log_msg(logfile, "    %s returned %d\n", func, retstat);
    errno = errsave;
}

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
// Report errors to logfile and give -errno to caller
int log_error(char *func)
{
    int ret = -errno;
    
    log_msg(logfile, "    ERROR %s: %s\n", func, strerror(errno));
    
    return ret;
}

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
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

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
// This dumps the info from a struct stat.  The struct is defined in
// <bits/stat.h>; this is indirectly included from <fcntl.h>
void log_stat(struct stat *si)
{
  log_msg(logfile, "    si:\n");
    
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

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
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

// Callback function for putting content to cloud
// from infile.
int put_buffer_in_cloud(char *buffer, int bufferLength) {
  int retstat = fread(buffer, 1, bufferLength, infile);
  if (verbosePrint >= 2) log_msg(logfile, "put_buffer %d, retstat %d\n", bufferLength, retstat);
  return retstat;
}

int put_buffer_in_cloud_for_ch2(char *buffer, int bufferLength) {
  int retstat = pread(uploadFdCh2, buffer, bufferLength, uploadFdCh2Offset);
  if (verbosePrint >= 2) log_msg(logfile, "put_buffer %d\n", bufferLength);
  return retstat;
}

// Callback function for getting content of from cloud and
// put those content in outfile.
int get_buffer_save_in_file(const char *buffer, int bufferLength) {
  if (verbosePrint >= 2) log_msg(logfile, "get_buffer %d\n", bufferLength);
  int retstat = fwrite(buffer, 1, bufferLength, outfile);
  return retstat;
}

// Callback function for list all keys of bucket
int cloudfs_list_bucket(const char *key, time_t modified_time, uint64_t size) {
  log_msg(logfile, "cloudfs_list_bucket\n");
  log_msg(logfile, "%s %lu %d\n", key, modified_time, size);
  return 0;
}

int cloudfs_list_bucket_and_save_in_map(const char *key, time_t modified_time, uint64_t size) {
  log_msg(logfile, "cloudfs_list_bucket_and_save_in_map\n");
  log_msg(logfile, "%s %lu %d\n", key, modified_time, size);
  keys_in_bucket_map[std::string(key)] = true;
  return 0;
}

// Callback function for list all buckets in cloud
int cloudfs_list_service(const char *bucketName) {
  if (verbosePrint >= 2) log_msg(logfile, "%s\n", bucketName);
  return 0; 
}

// Compute path name and bucket name from path and file name
void generate_bucket_name(const char *path, char bucket_name[PATH_MAX], char file_name[PATH_MAX]) {
  unsigned int last_slash_index = 0;
  for (unsigned int i = 0; i < strlen(path); i++) {
    if (path[i] == '/') {
      last_slash_index = i;
    }
  }
  unsigned int i = 0, j = 0;
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
  if (verbosePrint >= 2) log_msg(logfile, "\ngenerate_bucket_name(path=\"%s\", bucket_name=\"%s\", file_name=\"%s\")\n", path, bucket_name, file_name);
}


std::map<int, file_content_index> generateFileLocationMap(const char *path) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_getattr", fpath, path);
  char on_cloud[2];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  std::map<int, file_content_index> file_info_map;
  if (oncloud_signal <= 0) {
    return file_info_map;
  } else {
    if (verbosePrint >= 2) log_msg(logfile, "generateFileLocationMap debug\n");
    char buf[1024];  
    FILE *fp;            
    int len;         
    if((fp = fopen(fpath,"r")) == NULL)
    {
        log_error((char *) "open file error");
    }
    while(fgets(buf,1024,fp) != NULL)
    {
        len = strlen(buf);
        buf[len-1] = '\0';
        std::vector<std::string> allStr = split(buf, " ");
        file_content_index index_info = {
          segment_index: atoi(allStr.at(0).c_str()),
          offset: atoi(allStr.at(1).c_str()),
          size: atoi(allStr.at(2).c_str()),
          md5: allStr.at(3),
          complete: atoi(allStr.at(4).c_str()),
        };
        file_info_map[index_info.offset] = index_info;
        if (verbosePrint >= 3) log_msg(logfile, "segment index in generateFileLocationMap %d, offset %d, size %d, md5 %s, md5 length %d\n", index_info.segment_index, index_info.offset, index_info.size, index_info.md5.c_str(), strlen(index_info.md5.c_str()));
    }
    fclose(fp);
  }
  return file_info_map;
}

void saveInfoInMapToFile(const char *path, std::map<int, file_content_index> file_map) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_getxattr", fpath, path);
  int segment_index = 0;
  FILE *fptr;
  fptr = fopen(fpath, "w");
  if (fptr == NULL) {
    log_msg(logfile, "open file error\n");
  }
  for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
    std::string line = std::to_string(segment_index) + std::string(" ") + std::to_string(iter->second.offset) + std::string(" ") + std::to_string(iter->second.size) + std::string(" ") + iter->second.md5  + std::string(" ") + std::to_string(iter->second.complete);
    if (verbosePrint >= 3) log_msg(logfile, "Saving file info from map to file index %d, offset %d, size %d, md5 %s\n", segment_index, iter->second.offset, iter->second.size, iter->second.md5.c_str());
    fprintf(fptr,"%s\n", line.c_str());
    segment_index++;
  }
  fclose(fptr);
}

/** 
 * Get file attributes.
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 * Linux reference: https://linux.die.net/man/2/lstat
 */
int cloudfs_getattr(const char *path UNUSED, struct stat *statbuf UNUSED)
{
  int retstat;
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_getattr", fpath, path);
  log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", statbuf=0x%08x)\n", fpath, statbuf);
  retstat = log_syscall((char *) "cloudfs_getattr", lstat(fpath, statbuf), 0);
  if (verbosePrint >= 2) log_stat(statbuf);
  char on_cloud[2];
  int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
  if (oncloud_signal > 0) {
    int on_cloud_char_length = cloudfs_getxattr(path, "user.on_cloud_size", on_cloud, 0);
    char on_cloud_size[on_cloud_char_length];
    cloudfs_getxattr(path, "user.on_cloud_size", on_cloud_size, on_cloud_char_length);
    on_cloud_size[on_cloud_char_length] ='\0';
    log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", oncloud=\"%s\", oncloud_size=\"%d\")\n", fpath, on_cloud, atoi(on_cloud_size));
    statbuf->st_size = atoi(on_cloud_size);
  }
  if (strcmp(path, CLOUDFS_IOCTL_NAME) == 0) {
    statbuf->st_size = 0;
  }
  log_stat(statbuf);
  return retstat;
}

void recover_md5_frequency_map(const char *bucket_name, const char *key_name, std::unordered_map<std::string, int> map) {
  map.clear();
  S3Status s3status = cloud_list_bucket(bucket_name, cloudfs_list_bucket);
  log_msg(logfile, "S3Status of cloud_list_bucket in loudfs_init %d\n", s3status);
  if (s3status == S3StatusOK) {
    std::string fileName = "/.frequecyMap";
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_init", fpath, fileName.c_str());

    outfile = fopen(fpath, "wb");
    cloud_get_object(bucket_name, key_name, get_buffer_save_in_file);
    fclose(outfile);

    FILE *fp;
    fp = fopen(fpath,"r");
    char buf[1024];            
    int len;     
    while(fgets(buf,1024,fp) != NULL) {
        len = strlen(buf);
        buf[len-1] = '\0';
        std::vector<std::string> allStr = split(buf, " ");
        map[allStr.at(0)] = atoi(allStr.at(1).c_str());
        if (verbosePrint >= 2) log_msg(logfile, "md5 %s, md5 length %d\n", allStr.at(0).c_str(), atoi(allStr.at(1).c_str()));
    }
    fclose(fp);
    remove(fpath);
  }
}

void download_whole_file_from_cloud(const char *bucket_name, const char *key_name, const char *fileName) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "download_whole_file_from_cloud", fpath, fileName);
  outfile = fopen(fpath, "wb");
  cloud_get_object(bucket_name, key_name, get_buffer_save_in_file);
  fclose(outfile);
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
  log_msg(logfile, "\ncloudfs_init called\n");
  if (state_.no_dedup == NULL) {
    FILE *fp;
    std::string ioctlName = CLOUDFS_IOCTL_NAME;
    char fpath_ioctl[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_init", fpath_ioctl, ioctlName.c_str());
    log_msg(logfile, "\nCreate iocrl file %s\n", fpath_ioctl);
    fp = fopen(fpath_ioctl, "w");
    fclose(fp);
    cloudfs_chmod(ioctlName.c_str(), S_IRUSR|S_IRGRP|S_IROTH);  
    recover_md5_frequency_map("system_status", "system_status", md5_to_frequency_map);
  }
  return NULL;
}

void upload_whole_file_in_clould(const char *relative_file_path, const char *bucket_name, const char *key_name, bool deleteFile) {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "upload_whole_file_in_clould", fpath, relative_file_path);
    struct stat statbuf;
    cloudfs_getattr(relative_file_path, &statbuf);
    log_stat(&statbuf);
    infile = fopen(fpath, "rb");
    cloud_put_object(bucket_name, key_name, statbuf.st_size, put_buffer_in_cloud);
    fclose(infile);
    cloud_list_bucket(bucket_name, cloudfs_list_bucket);
    if (deleteFile) {
      remove(fpath);
    }
}

void upload_md5_frequecy_map_to_cloud(const char *bucket_name, const char *key_name, std::unordered_map<std::string, int> map) {
  std::string fileName = "/.frequecyMap";
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "upload_md5_frequecy_map_to_cloud", fpath, fileName.c_str());
  FILE *fptr;
  fptr = fopen(fpath, "w");
  if (fptr == NULL) {
    log_msg(logfile, "open file error\n");
    exit(0);
  }
  for (std::unordered_map<std::string, int>::iterator iter = map.begin(); iter != map.end(); iter++) {
    std::string line = iter->first + std::string(" ") + std::to_string(iter->second);
    fprintf(fptr,"%s\n", line.c_str());
  }
  fclose(fptr);
  upload_whole_file_in_clould(fileName.c_str(), bucket_name, key_name, true);
}

void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
  log_msg(logfile, "\ncloudfs_destroy called\n");
  if (state_.no_dedup == NULL) {
    std::string fileName = ".frequecyMap";
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_destroy", fpath, fileName.c_str());
    upload_md5_frequecy_map_to_cloud("system_status", "system_status", md5_to_frequency_map);
    remove(fpath);
  }
}

/** 
 * Get extended attributes 
 * Linux reference: https://linux.die.net/man/2/lgetxattr
 */
int cloudfs_getxattr(const char *path, const char *name, char *value, size_t size) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_getxattr", fpath, path);
  int retstat;
  retstat = log_syscall((char *) "getxattr", lgetxattr(fpath, name, value, size), 0);
  log_msg(logfile, "cloudfs_getxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d)\n", fpath, name, value, size);
  if (retstat >= 0) {
    if (verbosePrint >= 2) log_msg(logfile, "    value = \"%s\"\n", value);
  }
  return retstat;
}

/**
 * Set extended attributes
 * Linux reference: https://man7.org/linux/man-pages/man2/setxattr.2.html
 */
int cloudfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  char fpath[PATH_MAX]; 
  cloudfs_fullpath((char *) "cloudfs_setxattr", fpath, path);
  log_msg(logfile, "\ncloudfs_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
    fpath, name, value, size, flags);
  return log_syscall((char *) "cloudfs_setxattr", lsetxattr(fpath, name, value, size, flags), 0);
}

/**
 * Remove extended attributes
 * Linux reference: https://linux.die.net/man/2/removexattr
 */
int cloudfs_removexattr(const char *path, const char *name) {
  char fpath[PATH_MAX]; 
  log_msg(logfile, "\ncloudfs_removexattr(path=\"%s\", name=\"%s\")\n", path, name);
  cloudfs_fullpath((char *) "cloudfs_removexattr", fpath, path);
  return log_syscall((char *) "cloudfs_removexattr", lremovexattr(fpath, name), 0);
}

/** Create a directory */
int cloudfs_mkdir(const char *path, mode_t mode) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_mkdir", fpath, path);
  log_msg(logfile, "\ncloudfs_mkdir(path=\"%s\", mode=0%3o)\n", fpath, mode);
  return log_syscall((char *) "mkdir", mkdir(fpath, mode), 0);
}

/**
 * Create a file node
 * If the filesystem doesn't define a create() operation, mknod()
 * will be called for creation of all non-directory, non-symlink
 * nodes.
 */
int cloudfs_mknod(const char *path, mode_t mode, dev_t dev) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_mknod", fpath, path);
  log_msg(logfile, "\ncloudfs_mknod(path=\"%s\", mode=0%3o)\n", fpath, mode);
  return log_syscall((char *) "mknod", mknod(fpath, mode, dev), 0);
}

/** 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 * 
 * If the file content is on cloud, fetch if from the cloud first.
 * Keep last modification time and last access time right.
 * Also do mode switch in this process.
 * Changed in version 2.2
 */
int cloudfs_open(const char *path, struct fuse_file_info *fi) {
  if (state_.no_dedup == NULL) {
    int fd;
    int retstat = 0;
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_open(path=\"%s\"\n", fpath);
    fd = log_syscall((char *) "open", open(fpath, fi->flags), 0);
    if (fd < 0) {
      retstat = log_error((char *) "open");
      return retstat;
    }
    fi->fh = fd;
    log_fi(fi);
    return retstat;
  } else {
    char fpath[PATH_MAX];
    int fd;
    int retstat = 0;
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_open(path=\"%s\"\n", fpath);
    if (verbosePrint >= 2) log_fi(fi);
    fd = log_syscall((char *) "open", open(fpath, fi->flags), 0);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    struct stat statbuf;
    lstat(fpath, &statbuf);
    if (oncloud_signal > 0) {
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time before=%ld %ld, last modificaton time before=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      struct timespec timesaved[2];
      mode_t old_mode =  statbuf.st_mode;
      timesaved[1] = statbuf.st_mtim;
      int on_cloud_char_length = cloudfs_getxattr(path, "user.on_cloud_size", on_cloud, 0);
      char on_cloud_size[on_cloud_char_length];
      cloudfs_getxattr(path, "user.on_cloud_size", on_cloud_size, on_cloud_char_length);
      on_cloud_size[on_cloud_char_length] ='\0';
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", oncloud=\"%s\", oncloud_size=\"%d\")\n", fpath, on_cloud, atoi(on_cloud_size));
      char bucket_name[PATH_MAX];
      char file_name[PATH_MAX];
      generate_bucket_name(path, bucket_name, file_name);
      strcat(bucket_name, file_name);
      if (verbosePrint >= 2) log_msg(logfile, "get object with bucket name %s and file name %s\n", bucket_name, file_name);
      cloud_list_bucket(bucket_name, cloudfs_list_bucket);
      lstat(fpath, &statbuf);
      log_stat(&statbuf);
      cloudfs_chmod(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (verbosePrint >= 2) log_msg(logfile, "before getting from server\n");
      outfile = fopen(fpath, "wb");
      cloud_get_object(bucket_name, bucket_name, get_buffer_save_in_file);
      fclose(outfile);
      lstat(fpath, &statbuf);
      if (verbosePrint >= 2) log_stat(&statbuf);
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time after=%ld %ld, last modificaton time after=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      timesaved[0] = statbuf.st_atim;
      int reverttime = utimensat(0, fpath, timesaved, 0);
      if (verbosePrint >= 2) log_msg(logfile, "revert time result %d\n", reverttime);
      lstat(fpath, &statbuf);
      log_stat(&statbuf);
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time ultimate=%ld %ld, last modificaton time ultimate=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      cloudfs_chmod(path, old_mode);
    }
    if (fd < 0) {
      retstat = log_error((char *) "open");
      return retstat;
    }
    fi->fh = fd;
    log_fi(fi);
    return retstat;
  }
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
 * 
 * Need change mode here, because if file is in read only mode, we system need to fetch data from cloud
 * and write into file.
 */
int cloudfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  if (state_.no_dedup == NULL) {
    if (strcmp(path, CLOUDFS_IOCTL_NAME) == 0) {
      return -1;
    }
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
        path, buf, size, offset, fi);
    log_fi(fi);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    if (oncloud_signal <= 0) {
      int restat = log_syscall((char *) "cloudfs_read", pread(fi->fh, buf, size, offset), 0);
      return restat;
    } else {
      if (verbosePrint >= 2) log_msg(logfile, "\n content on the cloud\n");
      struct timespec timesaved[2];
      struct stat statbuf;
      lstat(fpath, &statbuf);
      timesaved[1] = statbuf.st_mtim;
      mode_t old_mode =  statbuf.st_mode;
      cloudfs_chmod(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);

      std::map<int, file_content_index> file_map = generateFileLocationMap(path);
      int fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);

      std::deque<file_content_index> changed_vector;
      for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
        if ((iter->second.offset < offset && iter->second.offset + iter->second.size > offset) || 
                (iter->second.offset >= offset && iter->second.offset < offset + size)) {
            changed_vector.push_back(iter->second);
            // log_msg(logfile, "related chunks offset %d, size %d, md5 %s\n", iter->second.offset, iter->second.size, iter->second.md5.c_str());
        }
      }
      outfile = fopen(fpath, "wb");
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
        if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
        cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
      }
      fclose(outfile);
      
      int real_offset = offset - changed_vector.at(0).offset;
      if (verbosePrint >= 2) log_msg(logfile, "\noffset %d, changed_vector.at(0).offset %d, real_offset %d\n", offset, changed_vector.at(0).offset, real_offset);
      int restat = log_syscall((char *) "cloudfs_read", pread(fi->fh, buf, size, real_offset), 0);

      fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);
      
      saveInfoInMapToFile(path, file_map);
      lstat(fpath, &statbuf);
      timesaved[0] = statbuf.st_atim;
      int reverttime = utimensat(0, fpath, timesaved, 0);
      if (verbosePrint >= 2) log_msg(logfile, "revert time result %d\n", reverttime);
      cloudfs_chmod(path, old_mode);
      return restat;
    }
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
        path, buf, size, offset, fi);
    log_fi(fi);
    int restat = log_syscall((char *) "cloudfs_read", pread(fi->fh, buf, size, offset), 0);
    return restat;
  }
}

/*
 * Linux reference: https://linux.die.net/man/2/pwrite
 * 
 * Write data to an open file
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 */
int cloudfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  log_msg(logfile, "\ncloudfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
  if (state_.no_dedup == NULL) {
    if (strcmp(path, CLOUDFS_IOCTL_NAME) == 0) {
      return -1;
    }
    char on_cloud[2];
    char fpath[PATH_MAX];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    if (offset + size <= state_.threshold && oncloud_signal <= 0) {
      return log_syscall((char *) "cloudfs_write", pwrite(fi->fh, buf, size, offset), 0);
    }
    int fd;
    std::map<int, file_content_index> file_map;
    MD5_CTX ctx;
    unsigned char md5[MD5_DIGEST_LENGTH];		
    int new_segment = 0;
    int len, segment_len = 0, b;
    int bytes;
    MD5_Init(&ctx);
    rabin_reset(rp);
    char fileContentBuffer[1024];
    int initial_offset = 0, upload_start_index = 0;
    struct timespec timesaved[2];
    struct stat statbuf;

    if (offset + size > state_.threshold && oncloud_signal <= 0) {
      int retstat = pwrite(fi->fh, buf, size, offset);
      if (retstat < 0) {
        log_msg(logfile, "\n The newly write content fialed\n");
      }
      if (verbosePrint >= 2) log_msg(logfile, "\n file size exceed thread %d and in ssd, just write new content with bytes %d in file, then upload to the cloud\n", state_.threshold, retstat);
    } else {
      if (verbosePrint >= 2) log_msg(logfile, "\n content on the cloud\n");
     
      lstat(fpath, &statbuf);
      timesaved[0] = statbuf.st_atim;

      file_map = generateFileLocationMap(path);
      std::deque<file_content_index> changed_vector;
      for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
        if (verbosePrint >= 2) log_msg(logfile, "chunks index %d, offset %d, size %d, md5 %s, complete %d\n", 
          iter->second.segment_index, iter->second.offset, iter->second.size, iter->second.md5.c_str(), iter->second.complete);
        if ((iter->second.offset < offset && iter->second.offset + iter->second.size >= offset) || 
                (iter->second.offset >= offset && iter->second.offset <= offset + size)) {
            if (!(iter->second.offset < offset && iter->second.offset + iter->second.size == offset && iter->second.complete == 1)) {
              changed_vector.push_back(iter->second);
            }
        }
        md5_to_frequency_map[iter->second.md5] -= 1;
      }
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        file_map.erase(chunk.offset);
        if (verbosePrint >= 2) log_msg(logfile, "related chunks index %d, offset %d, size %d, md5 %s\n", i, chunk.offset, chunk.size, chunk.md5.c_str());
      }

      fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);

      if (verbosePrint >= 2) {
        lstat(fpath, &statbuf);
        log_msg(logfile, "statbuf of fpath before gettting from cloud\n");
        log_stat(&statbuf);
      }

      outfile = fopen(fpath, "wb");
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
        if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
        cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
      }
      fclose(outfile);

      if (verbosePrint >= 2) {
        lstat(fpath, &statbuf);
        log_msg(logfile, "statbuf of fpath after gettting from cloud\n");
        log_stat(&statbuf);
      }

      int real_offset = 0;
      if (!changed_vector.empty()) {
        real_offset = offset - changed_vector.at(0).offset;
        upload_start_index = changed_vector.at(0).offset;
      } else {
        upload_start_index = offset;
      }
      if (verbosePrint >= 2) log_msg(logfile, "\noffset %d, upload_start_index %d, real_offset %d\n", offset, upload_start_index, real_offset);
      int retstat = pwrite(fi->fh, buf, size, real_offset);
      if (retstat < 0) {
        log_msg(logfile, "\n The newly write content fialed\n");
      } else {
        if (verbosePrint >= 2) log_msg(logfile, "\n The newly write content success %d\n", retstat);
      }

      if (verbosePrint >= 2) {
        lstat(fpath, &statbuf);
        log_msg(logfile, "statbuf of fpath after writing in new content\n");
        log_stat(&statbuf);
      }

      if (!changed_vector.empty()) {
        initial_offset = changed_vector.at(0).offset;
      } else {
        initial_offset = offset;
      }
    }

    fd = open(fpath, O_RDONLY);
    while((bytes = read(fd, fileContentBuffer, sizeof fileContentBuffer)) > 0 ) {
      char *buftoread = (char *)&fileContentBuffer[0];
      while ((len = rabin_segment_next(rp, fileContentBuffer, bytes, 
                        &new_segment)) > 0) {
        MD5_Update(&ctx, buftoread, len);
        segment_len += len;
        if (new_segment) {
          MD5_Final(md5, &ctx);
          std::string md5String;
          char bytePresentation[3];
          for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
            if (verbosePrint >= 2) log_msg(logfile, "%02x", md5[b]);
            sprintf(bytePresentation, "%02x", md5[b]);
            md5String += std::string(bytePresentation);
          }
          if (verbosePrint >= 2) log_msg(logfile, "\n find segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
          file_content_index new_index = {
            segment_index: -1,
            offset: initial_offset,
            size: segment_len,
            md5: md5String,
            complete: 1,
          };
          file_map[new_index.offset] = new_index;
          if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
            if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s firstly used in completed md5\n", md5String.c_str());
            md5_to_frequency_map[md5String] = 0;
            S3Status s3status = cloud_create_bucket(md5String.c_str());
            if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
            uploadFdCh2 = fd;
            uploadFdCh2Offset = initial_offset - upload_start_index;
            if (verbosePrint >= 2) log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
            cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
            s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
            if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
          } else {
            if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s previoud frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
          }
          MD5_Init(&ctx);
          initial_offset += segment_len;
          segment_len = 0;
        }
        buftoread += len;
        bytes -= len;
        if (!bytes) {
          break;
        }
      }
      if (len == -1) {
        log_msg(logfile, "Failed to process the segment\n");
      }
    }
    if (segment_len != 0) {
      MD5_Final(md5, &ctx);
      std::string md5String;
      char bytePresentation[3];
      if (verbosePrint >= 2) log_msg(logfile, "%u ", segment_len);
      for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
        if (verbosePrint >= 2) log_msg(logfile, "%02x", md5[b]);
        sprintf(bytePresentation, "%02x", md5[b]);
        md5String += std::string(bytePresentation);
      }
      if (verbosePrint >= 2) log_msg(logfile, "\n uncompleted segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
      file_content_index new_index = {
          segment_index: -1,
          offset: initial_offset,
          size: segment_len,
          md5: md5String,
          complete: 0,
      };
      file_map[new_index.offset] = new_index;
      if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
        if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s firstly used in uncompleted md5\n", md5String.c_str());
        md5_to_frequency_map[md5String] = 0;
        uploadFdCh2 = fd;
        uploadFdCh2Offset = initial_offset - upload_start_index;
        if (verbosePrint >= 2) log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
        S3Status s3status = cloud_create_bucket(md5String.c_str());
        if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
        cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
        s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
        if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
      } else {
        if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s previous frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
      }
    }
    close(fd);
    
    cloudfs_setxattr(path, "user.on_cloud", "1", strlen("1"), 0);
    for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
      int frequency = md5_to_frequency_map[iter->second.md5];
      md5_to_frequency_map[iter->second.md5] = frequency + 1;
      // log_msg(logfile, "Filemap status after finishing new write segment in after round key %d, index %d, offset %d, size %d, md5 %s\n", iter->first, iter->second.segment_index, iter->second.offset, iter->second.size, iter->second.md5.c_str());
    }
    for (std::unordered_map<std::string, int>::iterator iter = md5_to_frequency_map.begin(); iter != md5_to_frequency_map.end();) {
      if (iter->second == 0) {
        std::string md5_to_delete = iter->first;
        if (verbosePrint >= 2) log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
        S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
        if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
        s3status = cloud_delete_bucket(md5_to_delete.c_str());
        if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
        md5_to_frequency_map.erase(iter++);
      } else {
        iter++;
      }
    }
    int on_cloud_char_length = cloudfs_getxattr(path, "user.on_cloud_size", on_cloud, 0);
    if (on_cloud_char_length < 0) {
      cloudfs_setxattr(path, "user.on_cloud_size", std::to_string(offset + size).c_str(), strlen(std::to_string(offset + size).c_str()), 0);
    } else {
      char original_length[on_cloud_char_length];
      cloudfs_getxattr(path, "user.on_cloud_size", original_length, on_cloud_char_length);
      original_length[on_cloud_char_length] ='\0';
      if (verbosePrint >= 2) log_msg(logfile, "original length %s, current length %d\n", original_length, offset + size);
      if (atoi(original_length) < offset + size) {
        cloudfs_setxattr(path, "user.on_cloud_size", std::to_string(offset + size).c_str(), strlen(std::to_string(offset + size).c_str()), 0);
      }
    }

    fd = open(fpath, O_RDWR);
    ftruncate(fd,0);
    lseek(fd,0,SEEK_SET);
    close(fd);

    if (verbosePrint >= 2) {
      lstat(fpath, &statbuf);
      log_msg(logfile, "statbuf of fpath after deleting all\n");
      log_stat(&statbuf);
    }
    saveInfoInMapToFile(path, file_map);

    lstat(fpath, &statbuf);
    timesaved[1] = statbuf.st_mtim;
    int reverttime = utimensat(0, fpath, timesaved, 0);
    if (verbosePrint >= 2) log_msg(logfile, "revert time result %d\n", reverttime);
    return size;
  } else {
    return log_syscall((char *) "cloudfs_write", pwrite(fi->fh, buf, size, offset), 0);
  }
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
  if (state_.no_dedup == NULL) {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    log_fi(fi);
    struct stat statbuf;
    cloudfs_getattr(path, &statbuf);
    log_stat(&statbuf);
    if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time before release=%ld %ld, last modificaton time before release=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    int file_size = statbuf.st_size;
    int retstat = log_syscall((char *) "cloudfs_release", close(fi->fh), 0);
    return retstat;
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    log_fi(fi);
    struct stat statbuf;
    cloudfs_getattr(path, &statbuf);
    log_stat(&statbuf);
    if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time before release=%ld %ld, last modificaton time before release=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    int file_size = statbuf.st_size;
    int retstat = log_syscall((char *) "cloudfs_release", close(fi->fh), 0);
    char on_cloud[2];
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
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\") size %d bigger than cloudfs threshold %d\n", path, file_size, state_.threshold);
      if (verbosePrint >= 2) log_msg(logfile, "Create bucket with bucket name %s\n", bucket_name);
      S3Status s3status = cloud_create_bucket(bucket_name);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
      cloudfs_chmod(path, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      infile = fopen(fpath, "rb");
      cloud_put_object(bucket_name, bucket_name, statbuf.st_size, put_buffer_in_cloud);
      fclose(infile);
      s3status = cloud_list_bucket(bucket_name, cloudfs_list_bucket);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
      cloudfs_setxattr(path, "user.on_cloud", "1", strlen("1"), 0);
      char file_size_char[64];
      sprintf(file_size_char, "%d", file_size);
      cloudfs_setxattr(path, "user.on_cloud_size", file_size_char, strlen(file_size_char), 0);
      if (verbosePrint >= 2) log_msg(logfile, "Set cloud size %s\n", file_size_char);
      if (verbosePrint >= 2) log_msg(logfile, "cloud_put_object end\n");
      int fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);
      cloudfs_getattr(path, &statbuf);
      if (verbosePrint >= 2) log_stat(&statbuf);
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time after release=%ld %ld, last modificaton time after release=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      int reverttime = utimensat(0, fpath, timesaved, 0);
      if (verbosePrint >= 2) log_msg(logfile, "revert time result %d\n", reverttime);
      lstat(fpath, &statbuf);
      log_stat(&statbuf);
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time ultimate=%ld %ld, last modificaton time ultimate=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      cloudfs_chmod(path, old_mode);
    } else if (file_size <= state_.threshold && oncloud_signal > 0) {
      if (verbosePrint >= 2) log_msg(logfile, "\ncloudfs_release(path=\"%s\") size %d smaller than cloudfs threshold %d, and exists on cloud\n", path, file_size, state_.threshold);
      char bucket_name[PATH_MAX];
      char file_name[PATH_MAX];
      generate_bucket_name(path, bucket_name, file_name);
      strcat(bucket_name, file_name);
      if (verbosePrint >= 2) log_msg(logfile, "Delete bucket with bucket name %s\n", bucket_name);
      S3Status s3status = cloud_delete_object(bucket_name, bucket_name);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
      s3status = cloud_delete_bucket(bucket_name);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
    }
    return retstat;
  }
}
/*
 * Linux reference: https://man7.org/linux/man-pages/man3/opendir.3.html
 */
int cloudfs_opendir(const char *path, struct fuse_file_info *fi) {
    DIR *dp;
    int retstat = 0;
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_opendir", fpath, path);
    dp = opendir(fpath);
    log_msg(logfile, "\ncloudfs_opendir(path=\"%s\", fi=0x%08x)\n", fpath, fi);
    if (dp == NULL) {
      retstat = log_error((char *) "cloudfs_opendir opendir");
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

/** 
 * Read directory
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
 * Introduced in version 2.3
 */
int cloudfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  int retstat = 0;
  DIR *dp;
  struct dirent *de;
  log_msg(logfile, "\ncloudfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
    path, buf, filler, offset, fi);
  dp = (DIR *) (uintptr_t) fi->fh;
  de = readdir(dp);
  if (verbosePrint >= 2)  log_msg(logfile, "    readdir returned 0x%p\n", de);
  if (de == 0) {
    retstat = log_error((char *) "bb_readdir readdir");
    return retstat;
  }
  do {
    if (verbosePrint >= 2)  log_msg(logfile, "calling filler with name %s\n", de->d_name);
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
  cloudfs_fullpath((char *) "cloudfs_access", fpath, path);
  return log_syscall((char *) "access", access(fpath, mode), 0);
}

/*
 * Change the access and modification times of a file with
 * nanosecond resolution
 */
int cloudfs_utimens(const char *path, const struct timespec tv[2]) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_utimens", fpath, path);
  log_msg(logfile, "\ncloudfs_utimens(path=\"%s\")\n", fpath);
  struct stat stat_buf;
  cloudfs_getattr(path, &stat_buf);
  int retstat = log_syscall((char *) "utimens", utimensat(0, fpath, tv, 0), 0);
  cloudfs_getattr(path, &stat_buf);
  return retstat;
}

/** Change the permission bits of a file */
int cloudfs_chmod(const char *path, mode_t mode) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_chmod", fpath, path);
  log_msg(logfile, "\ncloudfs_chmod(path=\"%s\", mode=0%03o)\n", fpath, mode);
  return log_syscall((char *) "cloudfs_chmod", chmod(fpath, mode), 0);
}

/** Create a hard link to a file */
int cloudfs_link(const char *oldpath, const char *newpath) {
  char foldpath[PATH_MAX];
  char fnewpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_link", foldpath, oldpath);
  cloudfs_fullpath((char *) "cloudfs_link", fnewpath, newpath);
  log_msg(logfile, "\ncloudfs_link(oldpath=\"%s\", newpath=\"%s\")\n", foldpath, fnewpath);
  return log_syscall((char *) "cloudfs_link", link(foldpath, fnewpath), 0);
}

/** Create a symbolic link */
int cloudfs_symlink(const char *target, const char *link) {
    char flinkpath[PATH_MAX];
    log_msg(logfile, "\ncloudfs_symlink(target=\"%s\", link=\"%s\")\n", target, link);
    cloudfs_fullpath((char *)"cloudfs_symlink", flinkpath, link);
    return log_syscall((char *)"symlink", symlink(target, flinkpath), 0);
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
  cloudfs_fullpath((char *)"cloudfs_readlink", fpath, path);
  log_msg(logfile, "\ncloudfs_readlink(path=\"%s\", fpath=\"%s\"\n", path, fpath);
  int retstat = log_syscall((char *)"cloudfs_readlink", readlink(fpath, buf, bufsize - 1), 0);
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
  if (state_.no_dedup == NULL) {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *)"cloudfs_unlink", fpath, path);
    log_msg(logfile, "\ncloudfs_unlink(path=\"%s\")\n", fpath);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    if (oncloud_signal > 0) {
      std::map<int, file_content_index> file_map = generateFileLocationMap(path);
      for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
        md5_to_frequency_map[iter->second.md5] -= 1;
        // log_msg(logfile, "md5 %s remaininig frequency %d\n", iter->second.md5.c_str(), md5_to_frequency_map[iter->second.md5]);
        if (md5_to_frequency_map[iter->second.md5] == 0) {
          std::string md5_to_delete = iter->second.md5;
          if (verbosePrint >= 2) log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
          S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
          if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
          s3status = cloud_delete_bucket(md5_to_delete.c_str());
          if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
          md5_to_frequency_map.erase(md5_to_delete);
        }
      }
    }
    return log_syscall((char *) "cloudfs_unlink", unlink(fpath), 0);
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *)"cloudfs_unlink", fpath, path);
    log_msg(logfile, "\ncloudfs_unlink(path=\"%s\")\n", fpath);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    if (oncloud_signal > 0) {
      char bucket_name[PATH_MAX];
      char file_name[PATH_MAX];
      generate_bucket_name(path, bucket_name, file_name);
      strcat(bucket_name, file_name);
      if (verbosePrint >= 2) log_msg(logfile, "Delete bucket with bucket name %s\n", bucket_name);
      S3Status s3status = cloud_delete_object(bucket_name, bucket_name);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
      s3status = cloud_delete_bucket(bucket_name);
      if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
    }
    return log_syscall((char *) "cloudfs_unlink", unlink(fpath), 0);
  }
}

/*
 * Linux Reference: https://linux.die.net/man/2/rmdir
 */
int cloudfs_rmdir(const char *path) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_rmdir", fpath, path);
  log_msg(logfile, "\ncloudfs_rmdir(path=\"%s\")\n", fpath);
  int retstat = log_syscall((char *) "cloudfs_rmdir", rmdir(fpath), 0);
  cloud_list_service(cloudfs_list_service);
  return retstat;
}

int cloudfs_truncate(const char *path, off_t length) {
  if (state_.no_dedup == NULL) {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_truncate", fpath, path);
    log_msg(logfile, "\ncloudfs_truncate(path=\"%s\", length=%lld)\n", path, length);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    if (oncloud_signal < 0) {
      int retstat = log_syscall((char *) "cloudfs_truncate", truncate(fpath, length), 0);
      return retstat;
    } else {
      struct timespec timesaved[2];
      struct stat statbuf;
      lstat(fpath, &statbuf);
      timesaved[0] = statbuf.st_atim;

      int fd;
      std::map<int, file_content_index> file_map = generateFileLocationMap(path);
      std::deque<file_content_index> changed_vector;
      std::deque<file_content_index> deleted_vector;

      fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);

      for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
        log_msg(logfile, "segment index in generateFileLocationMap %d, offset %d, size %d, md5 %s, md5 length %d\n", iter->second.segment_index, iter->second.offset, iter->second.size, iter->second.md5.c_str(), strlen(iter->second.md5.c_str()));
        if (iter->second.offset >= length) {
          deleted_vector.push_back(iter->second);
          md5_to_frequency_map[iter->second.md5] -= 1;
        } else if (iter->second.offset < length && iter->second.offset + iter->second.size > length) {
          changed_vector.push_back(iter->second);
          md5_to_frequency_map[iter->second.md5] -= 1;
        }
      }
      for (int i = 0; i < deleted_vector.size(); i++) {
        file_content_index chunk = deleted_vector.at(i);
        file_map.erase(chunk.offset);
        if (verbosePrint >= 2) log_msg(logfile, "truncate remove chunks index %d, offset %d, size %d, md5 %s\n", i, chunk.offset, chunk.size, chunk.md5.c_str());
      }
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        file_map.erase(chunk.offset);
        if (verbosePrint >= 2) log_msg(logfile, "truncate changed chunks index %d, offset %d, size %d, md5 %s\n", i, chunk.offset, chunk.size, chunk.md5.c_str());
      }
      if (length <= state_.threshold) {
        log_msg(logfile, "\ncloudfs_truncate less than threshold\n");
        outfile = fopen(fpath, "wb");
        for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
          md5_to_frequency_map[iter->second.md5] -= 1;
          file_content_index chunk = iter->second;
          S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
          if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
          cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
        }
        for (int i = 0; i < changed_vector.size(); i++) {
          file_content_index chunk = changed_vector.at(i);
          S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
          if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
          cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
        }
        fclose(outfile);
        int retstat = log_syscall((char *) "cloudfs_truncate", truncate(fpath, length), 0);
        cloudfs_removexattr(path, "user.on_cloud");
        cloudfs_removexattr(path, "user.on_cloud_size");
        for (std::unordered_map<std::string, int>::iterator iter = md5_to_frequency_map.begin(); iter != md5_to_frequency_map.end();) {
          if (iter->second == 0) {
            std::string md5_to_delete = iter->first;
            if (verbosePrint >= 2) log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
            S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
            if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
            s3status = cloud_delete_bucket(md5_to_delete.c_str());
            if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
            md5_to_frequency_map.erase(iter++);
          } else {
            iter++;
          }
        }
        return retstat;
      } else {
        if (changed_vector.empty()) {
          for (std::unordered_map<std::string, int>::iterator iter = md5_to_frequency_map.begin(); iter != md5_to_frequency_map.end();) {
            if (iter->second == 0) {
              std::string md5_to_delete = iter->first;
              if (verbosePrint >= 2) log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
              S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
              if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
              s3status = cloud_delete_bucket(md5_to_delete.c_str());
              if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
              md5_to_frequency_map.erase(iter++);
            } else {
              iter++;
            }
          }
          cloudfs_setxattr(path, "user.on_cloud_size", std::to_string(length).c_str(), strlen(std::to_string(length).c_str()), 0);
          saveInfoInMapToFile(path, file_map);
          return 0;
        } else {
          outfile = fopen(fpath, "wb");
          for (int i = 0; i < changed_vector.size(); i++) {
            file_content_index chunk = changed_vector.at(i);
            S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
            if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
            cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
          }
          fclose(outfile);

  
          MD5_CTX ctx;
          unsigned char md5[MD5_DIGEST_LENGTH];       
          int new_segment = 0;
          int len, segment_len = 0, b;
          int bytes;
          MD5_Init(&ctx);
          rabin_reset(rp);
          char fileContentBuffer[1024];
          int initial_offset = changed_vector.at(0).offset, upload_start_index = changed_vector.at(0).offset;
          int retstat = log_syscall((char *) "cloudfs_truncate", truncate(fpath, length - initial_offset), 0);
          fd = open(fpath, O_RDONLY);
          while((bytes = read(fd, fileContentBuffer, sizeof fileContentBuffer)) > 0 ) {
            char *buftoread = (char *)&fileContentBuffer[0];
            while ((len = rabin_segment_next(rp, fileContentBuffer, bytes, 
                              &new_segment)) > 0) {
              MD5_Update(&ctx, buftoread, len);
              segment_len += len;
              if (new_segment) {
                MD5_Final(md5, &ctx);
                std::string md5String;
                char bytePresentation[3];
                for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
                  if (verbosePrint >= 2) log_msg(logfile, "%02x", md5[b]);
                  sprintf(bytePresentation, "%02x", md5[b]);
                  md5String += std::string(bytePresentation);
                }
                if (verbosePrint >= 2) log_msg(logfile, "\n find segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
                file_content_index new_index = {
                  segment_index: -1,
                  offset: initial_offset,
                  size: segment_len,
                  md5: md5String,
                };
                file_map[new_index.offset] = new_index;
                if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
                  if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s firstly used in completed md5\n", md5String.c_str());
                  md5_to_frequency_map[md5String] = 1;
                  S3Status s3status = cloud_create_bucket(md5String.c_str());
                  if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
                  uploadFdCh2 = fd;
                  uploadFdCh2Offset = initial_offset - upload_start_index;
                  if (verbosePrint >= 2) log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
                  cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
                  s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
                  if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
                } else {
                  md5_to_frequency_map[md5String] += 1;
                  if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s previoud frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
                }
                MD5_Init(&ctx);
                initial_offset += segment_len;
                segment_len = 0;
              }
              buftoread += len;
              bytes -= len;
              if (!bytes) {
                break;
              }
            }
            if (len == -1) {
              log_msg(logfile, "Failed to process the segment\n");
            }
          }
          if (segment_len != 0) {
            MD5_Final(md5, &ctx);
            std::string md5String;
            char bytePresentation[3];
            if (verbosePrint >= 2) log_msg(logfile, "%u ", segment_len);
            for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
              if (verbosePrint >= 2) log_msg(logfile, "%02x", md5[b]);
              sprintf(bytePresentation, "%02x", md5[b]);
              md5String += std::string(bytePresentation);
            }
            if (verbosePrint >= 2) log_msg(logfile, "\n uncompleted segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
            file_content_index new_index = {
                segment_index: -1,
                offset: initial_offset,
                size: segment_len,
                md5: md5String,
            };
            file_map[new_index.offset] = new_index;
            if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
              if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s firstly used in uncompleted md5\n", md5String.c_str());
              md5_to_frequency_map[md5String] = 1;
              uploadFdCh2 = fd;
              uploadFdCh2Offset = initial_offset - upload_start_index;
              if (verbosePrint >= 2) log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
              S3Status s3status = cloud_create_bucket(md5String.c_str());
              if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
              cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
              s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
              if (verbosePrint >= 2) log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
            } else {
              md5_to_frequency_map[md5String] += 1;
              if (verbosePrint >= 2) log_msg(logfile, "\n segements with md5 %s previous frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
            }
          }
          close(fd);
          cloudfs_setxattr(path, "user.on_cloud_size", std::to_string(length).c_str(), strlen(std::to_string(length).c_str()), 0);
          for (std::unordered_map<std::string, int>::iterator iter = md5_to_frequency_map.begin(); iter != md5_to_frequency_map.end();) {
            if (iter->second == 0) {
              std::string md5_to_delete = iter->first;
              if (verbosePrint >= 2) log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
              S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
              if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
              s3status = cloud_delete_bucket(md5_to_delete.c_str());
              if (verbosePrint >= 2) log_msg(logfile, "S3Status %d\n", s3status);
              md5_to_frequency_map.erase(iter++);
            } else {
              iter++;
            }
          }
          fd = open(fpath, O_RDWR);
          ftruncate(fd,0);
          lseek(fd,0,SEEK_SET);
          close(fd);
          saveInfoInMapToFile(path, file_map);

          lstat(fpath, &statbuf);
          timesaved[1] = statbuf.st_mtim;
          int reverttime = utimensat(0, fpath, timesaved, 0);
          if (verbosePrint >= 2) log_msg(logfile, "revert time result %d\n", reverttime);
          return 0;
        }
      }
    }
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_truncate", fpath, path);
    log_msg(logfile, "\ncloudfs_truncate(path=\"%s\", length=%d)\n", fpath, length);
    int retstat = log_syscall((char *) "cloudfs_truncate", truncate(fpath, length), 0);
    return retstat;
  }
}

void getRelativePath(const char *absolute_path, char *relative_path) {
  char *prefix_path = state_.ssd_path;
  int k = 0;
	for (int i= strlen(prefix_path); i < strlen(absolute_path); i++) {
		relative_path[k]=absolute_path[i];
		k++;
	}
	relative_path[k]='\0';
  log_msg(logfile, "\ngetRelativePath(absolute_path=\"%s\", relative_path=\"%s\")\n", absolute_path, relative_path);
}

void create(const char *filename, const char *folderpath) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "create", fpath, filename);
  log_msg(logfile, "\narchive_write_open_filename fpath %s\n", fpath);
	struct archive *a;
	struct archive_entry *entry;
	ssize_t len;
  char buff[16384];
	int fd;
	a = archive_write_new();
	archive_write_set_format_pax(a);
  int r;
	r = archive_write_open_filename(a, fpath);
  if (r != ARCHIVE_OK) {
      log_msg(logfile, "archive_write_open_filename\n");
      exit(1);
    }
	struct archive *disk = archive_read_disk_new();
	archive_read_disk_set_standard_lookup(disk);
	
  r = archive_read_disk_open(disk, folderpath);
  if (r != ARCHIVE_OK) {
    log_msg(logfile, "archive_read_disk_open error %s\n", archive_error_string(disk));
    exit(1);
  }

  for (;;) {
    entry = archive_entry_new();
    r = archive_read_next_header2(disk, entry);
    if (r == ARCHIVE_EOF)
      break;
    if (r != ARCHIVE_OK) {
      log_msg(logfile, "archive_read_next_header2 %s\n", archive_error_string(disk));
      exit(1);
    }
    archive_read_disk_descend(disk);
    const char *full_entry_pathname = archive_entry_pathname(entry);
    if (strcmp(full_entry_pathname, "/home/student/mnt/ssd") == 0) {
      log_msg(logfile, "full_entry_pathname is state_.ssd\n");
      archive_entry_free(entry);
      continue;
    } else {
      char relative_path[1024];
      getRelativePath(full_entry_pathname, relative_path);
      if (strcmp(relative_path, "lost+found") == 0 || strcmp(relative_path, ".snapshot") == 0) {
        log_msg(logfile, "relative_path is lost+found or .snapshot or compressed file name: %s\n", relative_path);
        archive_entry_free(entry);
        continue;
      }
      const char* assigned_path = relative_path;
      archive_entry_set_pathname(entry, assigned_path);
    }
    struct stat statbuf;
    lstat(archive_entry_sourcepath(entry), &statbuf);
    mode_t old_mode =  statbuf.st_mode;
    log_msg(logfile, "change mode %s\n", archive_entry_sourcepath(entry));
    chmod(archive_entry_sourcepath(entry), S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    log_msg(logfile, "archive_entry_pathname(entry) %s\n", archive_entry_pathname(entry));
    r = archive_write_header(a, entry);
    if (r < ARCHIVE_OK) {
      log_msg(logfile, "archive_write_header error %s\n", archive_error_string(a));
      archive_entry_free(entry);
      continue;
    }
    if (r == ARCHIVE_FATAL)
      exit(1);
    if (r > ARCHIVE_FAILED) {
      log_msg(logfile, "archive_entry_sourcepath(entry) %s\n", archive_entry_sourcepath(entry));
      fd = open(archive_entry_sourcepath(entry), O_RDONLY);
      len = read(fd, buff, sizeof(buff));
      while (len > 0) {
        archive_write_data(a, buff, len);
        len = read(fd, buff, sizeof(buff));
      }
      close(fd);
    }
    log_msg(logfile, "change mode %s\n", archive_entry_sourcepath(entry));
    chmod(archive_entry_sourcepath(entry), old_mode);
    archive_entry_set_perm(entry, old_mode);
    archive_entry_free(entry);
  }
  archive_read_close(disk);
  archive_read_free(disk);
	archive_write_close(a);
	archive_write_free(a);
}

int copy_data(struct archive *ar, struct archive *aw) {
	int r;
	const void *buff;
	size_t size;
	int64_t offset;
	for (;;) {
    log_msg(logfile, "copy_data process\n");
		r = archive_read_data_block(ar, &buff, &size, &offset);
		if (r == ARCHIVE_EOF)
			return (ARCHIVE_OK);
		if (r != ARCHIVE_OK) {
			log_msg(logfile, "archive_read_data_block error %s\n", archive_error_string(ar));
			return (r);
		}
		r = archive_write_data_block(aw, buff, size, offset);
		if (r != ARCHIVE_OK) {
			log_msg(logfile, "archive_write_data_block %s\n", archive_error_string(ar));
			return (r);
		}
	}
}

void extract(const char *filename, const char *foldername) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "extract", fpath, filename);
  char fpath_for_folder[PATH_MAX];
  cloudfs_fullpath((char *) "extract", fpath_for_folder, foldername);
	struct archive *a;
	struct archive *ext;
	struct archive_entry *entry;
	int r;
  int flags = ARCHIVE_EXTRACT_TIME;
  flags |= ARCHIVE_EXTRACT_OWNER;
  flags |= ARCHIVE_EXTRACT_PERM;
	flags |= ARCHIVE_EXTRACT_ACL;
	flags |= ARCHIVE_EXTRACT_FFLAGS;
  flags |= ARCHIVE_EXTRACT_XATTR;

	a = archive_read_new();
	ext = archive_write_disk_new();
	archive_write_disk_set_options(ext, flags);
	archive_read_support_format_tar(a);
	archive_write_disk_set_standard_lookup(ext);
	if ((r = archive_read_open_filename(a, fpath, 10240))) {
    log_msg(logfile, "archive_read_open_filename %s\n", archive_error_string(a));
		exit(r);
	}
	for (;;) {
		int needcr = 0;
		r = archive_read_next_header(a, &entry);
		if (r == ARCHIVE_EOF)
			break;
		if (r != ARCHIVE_OK) {
			log_msg(logfile, "archive_read_next_header %s\n", archive_error_string(a));
			exit(1);
		}
    archive_entry_set_pathname(entry, (std::string(fpath_for_folder) + "/" + archive_entry_pathname(entry)).c_str());
		log_msg(logfile, "extracting %s\n", archive_entry_pathname(entry));
    r = archive_write_header(ext, entry);
    log_msg(logfile, "1\n");
    if (r != ARCHIVE_OK) {
      log_msg(logfile, "archive_write_header error %s\n", archive_error_string(a));
      needcr = 1;
    } else {
      log_msg(logfile, "2\n");
      log_msg(logfile, "going into copy_data process\n");
      r = copy_data(a, ext);
      if (r != ARCHIVE_OK)
        log_msg(logfile, "copy_data error\n");
    }
	}
	archive_read_close(a);
	archive_read_free(a);
	archive_write_close(ext);
  archive_write_free(ext);
}

int cloudfs_snapshort(unsigned long *timestamp) {
  log_msg(logfile, "\ncloudfs_snapshort called\n");
  keys_in_bucket_map.clear();
  S3Status s3status = cloud_list_bucket("cloudfs_snapshort_bucket", cloudfs_list_bucket_and_save_in_map);
  if (s3status == 0 && keys_in_bucket_map.size() / 2 == CLOUDFS_MAX_NUM_SNAPSHOTS) {
    log_msg(logfile, "\nexceed CLOUDFS_MAX_NUM_SNAPSHOTS\n");
    return -1;
  }
  struct timeval time;
  int result = gettimeofday(&time, NULL);
  if (result < 0) {
    log_error((char *) "cloudfs_snapshort");
  }
  for (std::unordered_map<std::string, int>::iterator iter = md5_to_frequency_map.begin(); iter != md5_to_frequency_map.end(); iter++) {
    md5_to_frequency_map[iter->first] += 1;
  }
  std::string snapshot_name = "cloudfs_snapshort_" + std::to_string(time.tv_usec);
  create(("/" + snapshot_name).c_str(), state_.ssd_path);
  s3status = cloud_create_bucket("cloudfs_snapshort_bucket");
  upload_whole_file_in_clould(("/" + snapshot_name).c_str(), "cloudfs_snapshort_bucket", snapshot_name.c_str(), true);
  upload_md5_frequecy_map_to_cloud("cloudfs_snapshort_bucket", (snapshot_name  + "_md5_frequency").c_str(), md5_to_frequency_map);
  *timestamp = time.tv_usec;
  return 0;
}

void getfilepath(const char *path, const char *filename,  char *filepath)
{
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
	  log_msg(logfile, "getfilepath is = %s\n",filepath);
}

bool deleteFileInDirectory(const char* path, bool rootDir) {
    DIR *dir;
    struct dirent *dirinfo;
    struct stat statbuf;
    char filepath[256] = {0};
    lstat(path, &statbuf);
    if (S_ISREG(statbuf.st_mode)) {
        chmod(path, S_IRWXU|S_IRWXG|S_IRWXO);
        remove(path);
        log_msg(logfile, "file to delete is = %s\n", path);
    } else if (S_ISDIR(statbuf.st_mode)){
        if (strcmp(path, state_.ssd_path) != 0) {
          chmod(path, S_IRWXU|S_IRWXG|S_IRWXO);
        }
        if ((dir = opendir(path)) == NULL)
            return 1;
        while ((dirinfo = readdir(dir)) != NULL) {
            if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0 || strcmp(dirinfo->d_name, ".snapshot") == 0 || strcmp(dirinfo->d_name, "lost+found") == 0) {
              continue;
            }
            getfilepath(path, dirinfo->d_name, filepath);
            log_msg(logfile, "path to delete in directory is = %s\n", filepath);
            deleteFileInDirectory(filepath, false);
            if (!rootDir) {
              chmod(path, S_IRWXU|S_IRWXG|S_IRWXO);
              rmdir(path);
              log_msg(logfile, "dir to delete is = %s\n", path);
            }
        }
        closedir(dir);
    }
    return 0;
}

void change_all_to_be_read_only(char* path) {
    DIR *dir;
    struct dirent *dirinfo;
    struct stat statbuf;
    char filepath[256] = {0};
    lstat(path, &statbuf);
    if (S_ISREG(statbuf.st_mode)) {
        chmod(path, S_IRUSR|S_IRGRP|S_IROTH);
        log_msg(logfile, "file changed mod is = %s\n", path);
    } else if (S_ISDIR(statbuf.st_mode)){
        log_msg(logfile, "path to changed mod in directory is = %s\n", path);
        chmod(path, S_IRUSR|S_IXUSR|S_IRGRP|S_IROTH);
        if ((dir = opendir(path)) == NULL)
            return;
        while ((dirinfo = readdir(dir)) != NULL) {
            if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0) {
              continue;
            }
            getfilepath(path, dirinfo->d_name, filepath);
            log_msg(logfile, "path to changed mod in directory is = %s\n", filepath);
            change_all_to_be_read_only(filepath);
        }
        closedir(dir);
    }
}

void cloudfs_list_snapshort(unsigned long timestamp_list[CLOUDFS_MAX_NUM_SNAPSHOTS]) {
  keys_in_bucket_map.clear();
  S3Status s3status = cloud_list_bucket("cloudfs_snapshort_bucket", cloudfs_list_bucket_and_save_in_map);
  int index = 0;
  for (std::unordered_map<std::string, bool>::iterator iter = keys_in_bucket_map.begin(); iter != keys_in_bucket_map.end(); iter++) {
      std::string full_name = iter->first;
      std::string timestamp_string = full_name.substr(18, full_name.length());
      std::string md5_string = "_md5_frequency";
      std::string::size_type idx = full_name.find(md5_string);
      if (idx == std::string::npos) {
        unsigned long timestamp = std::stoul(timestamp_string, nullptr, 0);
        log_msg(logfile, "cloudfs_list_snapshort timestamp %ul\n", timestamp);
        *timestamp_list = timestamp;
        timestamp_list++;
        index++;
      }
  }
  while(index < CLOUDFS_MAX_NUM_SNAPSHOTS) {
    *timestamp_list = 0;
    timestamp_list++;
    index++;
  }
}

bool timestamp_exist_in_the_cloud(unsigned long timestamp) {
  unsigned long timstamp_list[CLOUDFS_MAX_NUM_SNAPSHOTS];
  cloudfs_list_snapshort(timstamp_list);
  bool found = false;
  for (int i = 0; i < CLOUDFS_MAX_NUM_SNAPSHOTS; i++) {
    if (timstamp_list[i] == timestamp) found = true;
  }
  return found;
}

int cloudfs_restore(unsigned long timestamp) {
  if (timestamp_exist_in_the_cloud(timestamp) == false) {
    log_msg(logfile, "\ncloudfs_restore called, can not find timestamp %lu in the cloud\n", timestamp);
    return -1;
  }
  log_msg(logfile, "\ncloudfs_restore called, timestamp %lu\n", timestamp);
  std::string snapshot_name = "cloudfs_snapshort_" + std::to_string(timestamp);
  deleteFileInDirectory(state_.ssd_path, true);
  download_whole_file_from_cloud("cloudfs_snapshort_bucket", snapshot_name.c_str(), ("/" + snapshot_name).c_str());
  extract(("/" + snapshot_name).c_str(), "/");
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_restore", fpath, ("/" + snapshot_name).c_str());
  remove(fpath);
  recover_md5_frequency_map("cloudfs_snapshort_bucket", (snapshot_name  + "_md5_frequency").c_str(), md5_to_frequency_map);
  return 0;
}

int cloudfs_install_snapshort(unsigned long timestamp) {
  if (timestamp_exist_in_the_cloud(timestamp) == false) {
    log_msg(logfile, "\ncloudfs_instal called, can not find timestamp %lu in the cloud\n", timestamp);
    return -1;
  }
  std::string dir_name = "snapshot_" + std::to_string(timestamp);
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_install_snapshort", fpath, ("/" + dir_name).c_str());
  if (access(fpath, 0) == 0) {
    log_msg(logfile, "\ncloudfs_instal called, already installed timestamp %lu\n", timestamp);
    return -1;
  }
  mkdir(fpath, S_IRWXU);
  std::string snapshot_name = "cloudfs_snapshort_" + std::to_string(timestamp);
  download_whole_file_from_cloud("cloudfs_snapshort_bucket", snapshot_name.c_str(), ("/" + snapshot_name).c_str());
  extract(("/" + snapshot_name).c_str(), ("/" + dir_name).c_str());
  change_all_to_be_read_only(fpath);
  cloudfs_fullpath((char *) "cloudfs_install_snapshort", fpath, ("/" + snapshot_name).c_str());
  remove(fpath);
  return 0;
}

int cloudfs_uninstall_snapshort(unsigned long timestamp) {
  std::string dir_name = "snapshot_" + std::to_string(timestamp);
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_uninstall_snapshort", fpath, ("/" + dir_name).c_str());
  if (access(fpath, 0) != 0) {
    log_msg(logfile, "\ncloudfs_uninstall_snapshort, not installed timestamp %lu before\n", timestamp);
    return -1;
  }
  deleteFileInDirectory(fpath, false);
  return 0;
}

std::vector<unsigned long> snapshort_after_timestamp_in_the_cloud(unsigned long timestamp) {
  unsigned long timstamp_list[CLOUDFS_MAX_NUM_SNAPSHOTS];
  std::vector<unsigned long> bigger_timestamp_list;
  cloudfs_list_snapshort(timstamp_list);
  for (int i = 0; i < CLOUDFS_MAX_NUM_SNAPSHOTS; i++) {
    if (timstamp_list[i] > timestamp) {
      bigger_timestamp_list.push_back(timestamp);
    }
  }
  return bigger_timestamp_list;
}

void minus_frequency_in_deleted_map(std::unordered_map<std::string, int> original_map, std::unordered_map<std::string, int> deleted_snapshot_map) {
  for (std::unordered_map<std::string, int>::iterator iter = deleted_snapshot_map.begin(); iter != deleted_snapshot_map.end(); iter++) {
    if (original_map.find(iter->first) != original_map.end()) {
      original_map[iter->first] = original_map[iter->first] -= 1;
      if (original_map[iter->first] == 0) {
        original_map.erase(iter->first);
      }
    }
  }
}

int cloudfs_delete_snapshort(unsigned long timestamp) {
  if (timestamp_exist_in_the_cloud(timestamp) == false) {
    log_msg(logfile, "\ncloudfs_delete called, can't find timestamp %lu in the cloud\n", timestamp);
    return -1;
  }
  std::string snapshot_name = "snapshot_" + std::to_string(timestamp);
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_delete_snapshort", fpath, ("/" + snapshot_name).c_str());
  if (access(fpath, 0) == 0) {
    log_msg(logfile, "\ncloudfs_delete_snapshort, has installed %lu before, not allowed to delete\n", timestamp);
    return -1;
  }
  std::unordered_map<std::string, int> deleted_snapshot_map;
  recover_md5_frequency_map("cloudfs_snapshort_bucket", (snapshot_name  + "_md5_frequency").c_str(), deleted_snapshot_map);
  std::vector<unsigned long> bigger_timestamp_list = snapshort_after_timestamp_in_the_cloud(timestamp);
  std::unordered_map<std::string, int> snapshot_to_update_map;
  for (int i = 0; i < bigger_timestamp_list.size(); i++) {
    log_msg(logfile, "\ncloudfs_delete_snapshort, exists snapshot  %d after this one, need to update md5_frequency\n", bigger_timestamp_list.at(i));
    std::string snapshot_to_update_name = "snapshot_" + std::to_string(bigger_timestamp_list.at(i));
    recover_md5_frequency_map("cloudfs_snapshort_bucket", (snapshot_to_update_name  + "_md5_frequency").c_str(), snapshot_to_update_map);
    minus_frequency_in_deleted_map(snapshot_to_update_map, deleted_snapshot_map);
    cloud_delete_object("cloudfs_snapshort_bucket", (snapshot_to_update_name  + "_md5_frequency").c_str());
    upload_md5_frequecy_map_to_cloud("cloudfs_snapshort_bucket", (snapshot_to_update_name + "_md5_frequency").c_str(), snapshot_to_update_map);
  }
  for (std::unordered_map<std::string, int>::iterator iter = deleted_snapshot_map.begin(); iter != deleted_snapshot_map.end(); iter++) {
    if (md5_to_frequency_map.find(iter->first) != md5_to_frequency_map.end()) {
      md5_to_frequency_map[iter->first] = md5_to_frequency_map[iter->first] -= 1;
      if (md5_to_frequency_map[iter->first] == 0) {
        cloud_delete_object(iter->first.c_str(), iter->first.c_str());
        md5_to_frequency_map.erase(iter->first);
      }
    }
  }
  cloud_delete_object("cloudfs_snapshort_bucket", snapshot_name.c_str());
  cloud_delete_object("cloudfs_snapshort_bucket", (snapshot_name  + "_md5_frequency").c_str());
}

int cloudfs_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi, unsigned int flags, void *data) {
  int ans = 0;
  switch (cmd)
  {
  case CLOUDFS_SNAPSHOT:
    ans = cloudfs_snapshort((unsigned long *) data);
    if (ans == -1) {
      return -EINVAL;
    }
    return 0;
  case CLOUDFS_RESTORE:
    log_msg(logfile, "\ncloudfs CLOUDFS_RESTORE called\n");
    ans = cloudfs_restore(*(unsigned long *) data);
    if (ans == -1) {
      return -EINVAL;
    } else {
      return 0;
    }
  case CLOUDFS_SNAPSHOT_LIST:
    log_msg(logfile, "\ncloudfs CLOUDFS_SNAPSHOT_LIST called\n");
    cloudfs_list_snapshort((unsigned long *) data);
    return 0;
  case CLOUDFS_DELETE:
    log_msg(logfile, "\ncloudfs CLOUDFS_DELETE called\n");
    ans = cloudfs_delete_snapshort(*(unsigned long *) data);
    if (ans == -1) {
      return -EINVAL;
    } else {
      return 0;
    }
  case CLOUDFS_INSTALL_SNAPSHOT:
    log_msg(logfile, "\ncloudfs CLOUDFS_INSTALL_SNAPSHOT called\n");
    ans = cloudfs_install_snapshort(*(unsigned long *) data);
    if (ans == -1) {
      return -EINVAL;
    } else {
      return 0;
    }
  case CLOUDFS_UNINSTALL_SNAPSHOT:
    log_msg(logfile, "\ncloudfs CLOUDFS_UNINSTALL_SNAPSHOT called\n");
    ans = cloudfs_uninstall_snapshort(*(unsigned long *) data);
    if (ans == -1) {
      return -EINVAL;
    } else {
      return 0;
    }
  default:
    return 0;
  }
  return 0;
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
  cloudfs_operations.truncate = cloudfs_truncate;
  cloudfs_operations.ioctl = cloudfs_ioctl;
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
  printf("state_.rabin_window_size: %d, state_.avg_seg_size: %d, state_.min_seg_size: %d, state_.max_seg_size: %d\n", 
    state_.rabin_window_size, state_.avg_seg_size, state_.min_seg_size, state_.max_seg_size);
  if (state_.no_dedup == NULL) {
    printf("Dedup mode\n");
  } else {
    printf("Disable Dedup mode\n");   
  }
  rp = rabin_init(state_.rabin_window_size, state_.avg_seg_size, 
								  state_.min_seg_size, state_.max_seg_size);
  if (!rp) {
		printf("Failed to init rabinhash algorithm\n");
		exit(1);
	}
  logfile = fopen("/tmp/cloudfs.log", "w");
  setvbuf(logfile, NULL, _IOLBF, 0);
  log_msg(logfile, "cloudfs_init()\n");
  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  return fuse_stat;
}
