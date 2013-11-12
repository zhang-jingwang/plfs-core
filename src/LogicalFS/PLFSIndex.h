#ifndef __PLFSINDEX_H__
#define __PLFSINDEX_H__
#include "IOStore.h"


/**
 * Abstract class for data-index information.
 *
 * If a class is derived from this class and implements all its interfaces,
 * then the user could use the function 'plfs_reader()' to read the data
 * at the given position.
 */
class PLFSIndex {
public:
    virtual ~PLFSIndex() {};
    virtual void lock(const char *function) = 0;
    virtual void unlock(const char *function) = 0;
    virtual IOSHandle *getChunkFh( pid_t chunk_id ) = 0;
    virtual plfs_error_t setChunkFh( pid_t chunk_id, IOSHandle *fh ) = 0;
    virtual plfs_error_t globalLookup( IOSHandle **fh, off_t *chunk_off, size_t *length,
                                       string& path, struct plfs_backend **backp,
                                       bool *hole, pid_t *chunk_id,
                                       off_t logical ) = 0;
    virtual plfs_error_t newLookup(off_t, IOSHandle **, string& ,
				   struct plfs_backend **, pid_t *,
				   off_t *, size_t *,
				   off_t *, Plfs_checksum *,
				   off_t *, size_t *,
				   bool *) {
	return PLFS_ENOSYS;
    }

};

/**
 * This function performs multi-threaded read.
 *
 * This function takes care of thread pool and open file cache. The only
 * thing you need to do is providing a class derived from PLFSIndex.
 */
plfs_error_t plfs_reader(void *unused, char *buf, size_t size,
                         off_t offset, PLFSIndex *index, ssize_t *bytes_read);

plfs_error_t plfs_xreader(void *pfd, struct iovec *iov, int iovcnt,
                          plfs_xvec *xvec, int xvcnt, PLFSIndex *index,
                          ssize_t *bytes_read);

plfs_error_t plfs_shard_builder(PLFSIndex *index, off_t offset, size_t size,
                                int loc_required, plfs_shard **head);
plfs_error_t free_shard_list(plfs_shard *, int);
plfs_error_t plfs_readerc(void *unused, char *buf, size_t size,
			  off_t offset, PLFSIndex *index, ssize_t *bytes_read,
			  Plfs_checksum *checksum);

#endif
