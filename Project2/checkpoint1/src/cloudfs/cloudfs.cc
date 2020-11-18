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
#include "libs3.h"
#include <map>
#include <iostream>
#include <deque>
#include <vector>

#define UNUSED __attribute__((unused))

#define log_struct(st, field, format, typecast) \
  log_msg(logfile, "    " #field " = " #format "\n", typecast st->field)

struct file_content_index {
  int segment_index;
  int offset;
  int size;
  std::string md5;
};

static struct cloudfs_state state_;
static FILE *logfile;
static FILE *infile;
static FILE *outfile;
static std::unordered_map<std::string, int> md5_to_frequency_map;
static rabinpoly_t *rp;
static int uploadFdCh2;
static int uploadFdCh2Offset;

// Copied from reference code https://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/
void log_msg(FILE *logfile, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(logfile, format, ap);
    va_end(ap);
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
  log_msg(logfile, "put_buffer %d, retstat %d\n", bufferLength, retstat);
  return retstat;
}

int put_buffer_in_cloud_for_ch2(char *buffer, int bufferLength) {
  int retstat = pread(uploadFdCh2, buffer, bufferLength, uploadFdCh2Offset);
  log_msg(logfile, "put_buffer %d\n", bufferLength);
  return retstat;
}

// Callback function for getting content of from cloud and
// put those content in outfile.
int get_buffer_save_in_file(const char *buffer, int bufferLength) {
  log_msg(logfile, "get_buffer %d\n", bufferLength);
  int retstat = fwrite(buffer, 1, bufferLength, outfile);
  return retstat;
}

// Callback function for list all keys of bucket
int cloudfs_list_bucket(const char *key, time_t modified_time, uint64_t size) {
  log_msg(logfile, "cloudfs_list_bucket\n");
  log_msg(logfile, "%s %lu %d\n", key, modified_time, size);
  return 0;
}

// Callback function for list all buckets in cloud
int cloudfs_list_service(const char *bucketName) {
  log_msg(logfile, "%s\n", bucketName);
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
  log_msg(logfile, "\ngenerate_bucket_name(path=\"%s\", bucket_name=\"%s\", file_name=\"%s\")\n", path, bucket_name, file_name);
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
    log_msg(logfile, "generateFileLocationMap debug\n");
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
        // log_msg(logfile, "%s %d \n", buf, len - 1);
        std::vector<std::string> allStr = split(buf, " ");
        file_content_index index_info = {
          segment_index: atoi(allStr.at(0).c_str()),
          offset: atoi(allStr.at(1).c_str()),
          size: atoi(allStr.at(2).c_str()),
          md5: allStr.at(3),
        };
        file_info_map[index_info.offset] = index_info;
        log_msg(logfile, "segment index in generateFileLocationMap %d, offset %d, size %d, md5 %s, md5 length %d\n", index_info.segment_index, index_info.offset, index_info.size, index_info.md5.c_str(), strlen(index_info.md5.c_str()));
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
    std::string line = std::to_string(segment_index) + std::string(" ") + std::to_string(iter->second.offset) + std::string(" ") + std::to_string(iter->second.size) + std::string(" ") + iter->second.md5;
    // log_msg(logfile, "Saving file info from map to file index %d, offset %d, size %d, md5 %s\n", segment_index, iter->second.offset, iter->second.size, iter->second.md5.c_str());
    fprintf(fptr,"%s\n", line.c_str());
    segment_index++;
  }
  fclose(fptr);
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

// Compute absolute path from path and saved result in fpath
void cloudfs_fullpath(char *func, char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, state_.ssd_path);
    strncat(fpath, path + 1, strlen(path) - 1);
    log_msg(logfile, "\ncloudfs_fullpath:  func= \"%s\", rootdir = \"%s\", path = \"%s\", fpath = \"%s\"", func, state_.ssd_path, path, fpath);
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
  log_stat(statbuf);
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
  log_stat(statbuf);
  return retstat;
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
  log_msg(logfile, "\ncloudfs_getxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d)\n", fpath, name, value, size);
  if (retstat >= 0) {
    log_msg(logfile, "    value = \"%s\"\n", value);
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
    log_fi(fi);
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
    log_fi(fi);
    fd = log_syscall((char *) "open", open(fpath, fi->flags), 0);
    char on_cloud[2];
    int oncloud_signal = cloudfs_getxattr(path, "user.on_cloud", on_cloud, 2);
    struct stat statbuf;
    lstat(fpath, &statbuf);
    if (oncloud_signal > 0) {
      log_msg(logfile, "\ncloudfs_utimens(path=\"%s\", last access time before=%ld %ld, last modificaton time before=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
      struct timespec timesaved[2];
      mode_t old_mode =  statbuf.st_mode;
      timesaved[1] = statbuf.st_mtim;
      int on_cloud_char_length = cloudfs_getxattr(path, "user.on_cloud_size", on_cloud, 0);
      char on_cloud_size[on_cloud_char_length];
      cloudfs_getxattr(path, "user.on_cloud_size", on_cloud_size, on_cloud_char_length);
      on_cloud_size[on_cloud_char_length] ='\0';
      log_msg(logfile, "\ncloudfs_getattr(path=\"%s\", oncloud=\"%s\", oncloud_size=\"%d\")\n", fpath, on_cloud, atoi(on_cloud_size));
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
 */
int cloudfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  if (state_.no_dedup == NULL) {
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
      log_msg(logfile, "\n content on the cloud\n");
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
            log_msg(logfile, "related chunks offset %d, size %d, md5 %s\n", iter->second.offset, iter->second.size, iter->second.md5.c_str());
        }
      }
      outfile = fopen(fpath, "wb");
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
        log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
        cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
      }
      fclose(outfile);
      
      int real_offset = offset - changed_vector.at(0).offset;
      log_msg(logfile, "\noffset %d, changed_vector.at(0).offset %d, real_offset %d\n", offset, changed_vector.at(0).offset, real_offset);
      int restat = log_syscall((char *) "cloudfs_read", pread(fi->fh, buf, size, real_offset), 0);

      fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);
      
      saveInfoInMapToFile(path, file_map);
      return restat;
    }
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
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
  if (state_.no_dedup == NULL) {
    log_msg(logfile, "\ncloudfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);
    log_fi(fi);
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

    if (offset + size > state_.threshold && oncloud_signal <= 0) {
      int retstat = pwrite(fi->fh, buf, size, offset);
      if (retstat < 0) {
        log_msg(logfile, "\n The newly write content fialed\n");
      }
      log_msg(logfile, "\n file size exceed thread %d and in ssd, just write new content with bytes %d in file, then upload to the cloud\n", state_.threshold, retstat);
    } else {
      log_msg(logfile, "\n content on the cloud\n");
      file_map = generateFileLocationMap(path);
      std::deque<file_content_index> changed_vector;
      for (std::map<int, file_content_index>::iterator iter = file_map.begin(); iter != file_map.end(); iter++) {
        if ((iter->second.offset < offset && iter->second.offset + iter->second.size >= offset) || 
                (iter->second.offset >= offset && iter->second.offset <= offset + size)) {
            changed_vector.push_back(iter->second);
        }
        md5_to_frequency_map[iter->second.md5] -= 1;
      }
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        file_map.erase(chunk.offset);
        log_msg(logfile, "related chunks index %d, offset %d, size %d, md5 %s\n", i, chunk.offset, chunk.size, chunk.md5.c_str());
      }

      fd = open(fpath, O_RDWR);
      ftruncate(fd,0);
      lseek(fd,0,SEEK_SET);
      close(fd);

      struct stat statbuf;
      lstat(fpath, &statbuf);
      log_msg(logfile, "statbuf of fpath before gettting from cloud\n");
      log_stat(&statbuf);

      outfile = fopen(fpath, "wb");
      for (int i = 0; i < changed_vector.size(); i++) {
        file_content_index chunk = changed_vector.at(i);
        S3Status s3status = cloud_list_bucket(chunk.md5.c_str(), cloudfs_list_bucket);
        log_msg(logfile, "S3Status of cloud_list_bucket %d, %s\n", s3status, chunk.md5.c_str());
        cloud_get_object(chunk.md5.c_str(), chunk.md5.c_str(), get_buffer_save_in_file);
      }
      fclose(outfile);

      lstat(fpath, &statbuf);
      log_msg(logfile, "statbuf of fpath after gettting from cloud\n");
      log_stat(&statbuf);

      int real_offset = offset - changed_vector.at(0).offset;
      upload_start_index = changed_vector.at(0).offset;
      log_msg(logfile, "\noffset %d, changed_vector.at(0).offset %d, real_offset %d\n", offset, changed_vector.at(0).offset, real_offset);
      int retstat = pwrite(fi->fh, buf, size, real_offset);
      if (retstat < 0) {
        log_msg(logfile, "\n The newly write content fialed\n");
      } else {
        log_msg(logfile, "\n The newly write content success %d\n", retstat);
      }

      lstat(fpath, &statbuf);
      log_msg(logfile, "statbuf of fpath after writing in new content\n");
      log_stat(&statbuf);

      initial_offset = changed_vector.at(0).offset;
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
            log_msg(logfile, "%02x", md5[b]);
            sprintf(bytePresentation, "%02x", md5[b]);
            md5String += std::string(bytePresentation);
          }
          log_msg(logfile, "\n find segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
          file_content_index new_index = {
            segment_index: -1,
            offset: initial_offset,
            size: segment_len,
            md5: md5String,
          };
          file_map[new_index.offset] = new_index;
          if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
            log_msg(logfile, "\n segements with md5 %s firstly used in completed md5\n", md5String.c_str());
            md5_to_frequency_map[md5String] = 0;
            S3Status s3status = cloud_create_bucket(md5String.c_str());
            log_msg(logfile, "S3Status %d\n", s3status);
            uploadFdCh2 = fd;
            uploadFdCh2Offset = initial_offset - upload_start_index;
            log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
            cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
            s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
            log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
          } else {
            log_msg(logfile, "\n segements with md5 %s previoud frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
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
      log_msg(logfile, "%u ", segment_len);
      for(b = 0; b < MD5_DIGEST_LENGTH; b++) {
        log_msg(logfile, "%02x", md5[b]);
        sprintf(bytePresentation, "%02x", md5[b]);
        md5String += std::string(bytePresentation);
      }
      log_msg(logfile, "\n uncompleted segements with md5 value %s, initial_offset %d, size %d\n", md5String.c_str(), initial_offset, segment_len);
      file_content_index new_index = {
          segment_index: -1,
          offset: initial_offset,
          size: segment_len,
          md5: md5String,
      };
      file_map[new_index.offset] = new_index;
      if (md5_to_frequency_map.find(md5String) == md5_to_frequency_map.end()) {
        log_msg(logfile, "\n segements with md5 %s firstly used in uncompleted md5\n", md5String.c_str());
        md5_to_frequency_map[md5String] = 0;
        uploadFdCh2 = fd;
        uploadFdCh2Offset = initial_offset - upload_start_index;
        log_msg(logfile, "start upload to cloud from file offset %d, actual offset %d\n", uploadFdCh2Offset, initial_offset);
        S3Status s3status = cloud_create_bucket(md5String.c_str());
        log_msg(logfile, "S3Status %d\n", s3status);
        cloud_put_object(md5String.c_str(), md5String.c_str(), segment_len, put_buffer_in_cloud_for_ch2);
        s3status = cloud_list_bucket(md5String.c_str(), cloudfs_list_bucket);
        log_msg(logfile, "S3Status of cloud_list_bucket %d\n", s3status);
      } else {
        log_msg(logfile, "\n segements with md5 %s previous frequency %d\n", md5String.c_str(), md5_to_frequency_map.find(md5String)->second);
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
        log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
        S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
        log_msg(logfile, "S3Status %d\n", s3status);
        s3status = cloud_delete_bucket(md5_to_delete.c_str());
        log_msg(logfile, "S3Status %d\n", s3status);
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
      log_msg(logfile, "original length %s, current length %d\n", original_length, offset + size);
      if (atoi(original_length) < offset + size) {
        cloudfs_setxattr(path, "user.on_cloud_size", std::to_string(offset + size).c_str(), strlen(std::to_string(offset + size).c_str()), 0);
      }
    }

    fd = open(fpath, O_RDWR);
    ftruncate(fd,0);
    lseek(fd,0,SEEK_SET);
    close(fd);

    struct stat statbuf;
    lstat(fpath, &statbuf);
    log_msg(logfile, "statbuf of fpath after deleting all\n");
    log_stat(&statbuf);

    saveInfoInMapToFile(path, file_map);
    return size;
  } else {
    log_msg(logfile, "\ncloudfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    log_fi(fi);
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
    log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time before release=%ld %ld, last modificaton time before release=%ld %ld)\n", 
        fpath, ((&statbuf)->st_atim).tv_sec, fpath, ((&statbuf)->st_atim).tv_nsec, ((&statbuf)->st_mtim).tv_sec, ((&statbuf)->st_mtim).tv_nsec);
    int file_size = statbuf.st_size;
    int retstat = log_syscall((char *) "cloudfs_release", close(fi->fh), 0);
    return retstat;
  } else {
    char fpath[PATH_MAX];
    cloudfs_fullpath((char *) "cloudfs_open", fpath, path);
    log_msg(logfile, "\ncloudfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    log_fi(fi);
    struct stat statbuf;
    cloudfs_getattr(path, &statbuf);
    log_stat(&statbuf);
    log_msg(logfile, "\ncloudfs_release(path=\"%s\", last access time before release=%ld %ld, last modificaton time before release=%ld %ld)\n", 
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
      cloudfs_getattr(path, &statbuf);
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
  log_msg(logfile, "    readdir returned 0x%p\n", de);
  if (de == 0) {
    retstat = log_error((char *) "bb_readdir readdir");
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
        log_msg(logfile, "md5 %s remaininig frequency %d\n", iter->second.md5.c_str(), md5_to_frequency_map[iter->second.md5]);
        if (md5_to_frequency_map[iter->second.md5] == 0) {
          std::string md5_to_delete = iter->second.md5;
          log_msg(logfile, "deleting md5 %s\n", md5_to_delete.c_str());
          S3Status s3status = cloud_delete_object(md5_to_delete.c_str(), md5_to_delete.c_str());
          log_msg(logfile, "S3Status %d\n", s3status);
          s3status = cloud_delete_bucket(md5_to_delete.c_str());
          log_msg(logfile, "S3Status %d\n", s3status);
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
      log_msg(logfile, "Delete bucket with bucket name %s\n", bucket_name);
      S3Status s3status = cloud_delete_object(bucket_name, bucket_name);
      log_msg(logfile, "S3Status %d\n", s3status);
      s3status = cloud_delete_bucket(bucket_name);
      log_msg(logfile, "S3Status %d\n", s3status);
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

int cloundfs_truncate(const char *path, off_t length) {
  char fpath[PATH_MAX];
  cloudfs_fullpath((char *) "cloudfs_rmdir", fpath, path);
  log_msg(logfile, "\ncloundfs_truncate(path=\"%s\", length=%d)\n", fpath, length);
  int retstat = log_syscall((char *) "cloundfs_truncate", truncate(fpath, length), 0);
  char file_size_char[64];
  cloudfs_setxattr(path, "user.on_cloud_size", file_size_char, strlen(file_size_char), 0);
  return retstat;
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
