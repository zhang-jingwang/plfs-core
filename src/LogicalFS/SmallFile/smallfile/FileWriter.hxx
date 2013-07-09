#ifndef __FILEWRITER_HXX__
#define __FILEWRITER_HXX__

#include <stdio.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <Util.h>
#include "SmallFileLayout.h"

/**
 * It represents a physical file for writing.
 */

class FileWriter {
private:
#ifdef SMALLFILE_USE_LIBC_FILEIO
    FILE *fptr;
#else
    IOStore *store_;
    IOSHandle *handle;
#endif
    off_t current_pos;
    pthread_mutex_t mlock;
public:
    FileWriter();
    ~FileWriter();
    plfs_error_t open_file(const char *filename, class IOStore *store);
    /**
     * Append some data to this file.
     *
     * @param buf The address of the buffer to be written.
     * @param length The length of the buffer.
     * @param physical_offset The start offset of this buffer in the physical
     *    file. It can be NULL if we don't care the offset.
     * @return On success, PLFS_SUCCESS is returned. On error, PLFS_TBD is returned.
     */
    plfs_error_t append(const void *buf, size_t length, off_t *physical_offset);
    plfs_error_t sync();
    plfs_error_t close_file();
    bool is_opened();
};

#endif
