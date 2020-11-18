#ifndef __CLOUDFS_H_
#define __CLOUDFS_H_

#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024

struct cloudfs_state {
  char ssd_path[MAX_PATH_LEN];
  char fuse_path[MAX_PATH_LEN];
  char hostname[MAX_HOSTNAME_LEN];
  int ssd_size;
  int threshold;
  int avg_seg_size;
  int min_seg_size;
  int max_seg_size;
  int cache_size;
  int rabin_window_size;
  char no_dedup;
};

int cloudfs_start(struct cloudfs_state* state,
                  const char* fuse_runtime_name);  
void cloudfs_get_fullpath(const char *path, char *fullpath);
int cloudfs_getxattr(const char *path, const char *name, char *value, size_t size);
int cloudfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
int cloudfs_chmod(const char *path, mode_t mode);
int cloudfs_removexattr(const char *path, const char *name);
#endif

