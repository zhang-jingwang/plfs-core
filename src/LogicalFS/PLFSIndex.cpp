#include <stdlib.h>
#include "plfs_private.h"
#include "ThreadPool.h"
#include "mlog_oss.h"
#include "PLFSIndex.h"
#include <mchecksum.h>

// a struct for making reads be multi-threaded
typedef struct {
    IOSHandle *fh;
    size_t length;
    off_t chunk_offset;
    off_t logical_offset;
    char *buf;
    pid_t chunk_id; // in order to stash fd's back into the index
    string path;
    struct plfs_backend *backend;
    Plfs_checksum checksum;
    void *msrc;
    void *mdst;
    size_t mlen;
    bool hole;
    bool partial_read;
} ReadTask;

// a struct to contain the args to pass to the reader threads
typedef struct {
    PLFSIndex *index;   // the index needed to get and stash chunk fds
    list<ReadTask> *tasks;   // the queue of tasks
    list<ReadTask>::iterator titr;
    pthread_mutex_t mux;    // to lock the queue
} ReaderArgs;


// free the shard list produced by build_shard_list
plfs_error_t
free_shard_list(plfs_shard *head, int loc_required)
{
    plfs_shard *shard;
    while(head){
	shard = head;
	head = head->next;
	if(loc_required) free(shard->location);
	delete shard;
    }
    return PLFS_SUCCESS;
}

// a helper routine to convert a list of read tasks into logical valid ranges.
// merge logical contiguous read tasks depending on loc_required.
// if loc_required is setting to 0, then try to merge across different devices.
// Or try to merge within each device.
// return 0 or -err
plfs_error_t
build_shrad_list(list<ReadTask> *tasks, plfs_shard **head,
		 int loc_required)
{
    plfs_error_t ret = PLFS_SUCCESS;
    list<ReadTask>::iterator itr;
    plfs_shard *shard = NULL, *current = NULL;
    struct plfs_backend *backend; //retrieve back backend device by phsical path
    string bpath;

    if( tasks->empty() ) return ret;

    *head = NULL;
    for(itr=tasks->begin(); itr!=tasks->end(); itr++){
	if ( itr->hole ) continue;
	shard = new plfs_shard;
	if( shard == NULL ){
	    ret = PLFS_ENOMEM;
	    break;
	}
	shard->xvec.offset = itr->logical_offset;
	shard->xvec.len = itr->length;
	shard->location = NULL;
	shard->next = NULL;
	// retrieve back the backend device
	ret = plfs_phys_backlookup(itr->path.c_str(), NULL, &backend, &bpath);
	if(ret != 0){
	    mlog(INT_CRIT, "%s backlookup failed for %s\n",
		    __FUNCTION__, itr->path.c_str());
	    break;
	}
	if(loc_required){
	    shard->location = Util::Strdup(backend->bmpoint.c_str());
	}
	if ( *head == NULL ){
	    *head = shard;
	    current = *head;
	} else {
	    // this is how to merge different read tasks, across or within
	    // devices
	    bool can_merge = loc_required?
		(current->xvec.offset+(off_t)current->xvec.len ==
		 shard->xvec.offset
		 && strcmp(current->location, shard->location) == 0):
		(current->xvec.offset+(off_t)current->xvec.len ==
		 shard->xvec.offset);
	    if( can_merge ) {
		current->xvec.len += shard->xvec.len;
		delete shard;
	    } else {
		current->next = shard;
		current = shard;
	    }
	}
    }

    if(ret != PLFS_SUCCESS) free_shard_list(*head, loc_required);

    return ret;
}

// a helper routine for read to allow it to be multi-threaded when a single
// logical read spans multiple chunks
// tasks needs to be a list and not a queue since sometimes it does pop_back
// here in order to consolidate sequential reads (which can happen if the
// index is not buffered on the writes)
// don't associate each range with buf segment if passed buf as NULL
plfs_error_t
find_read_tasks(PLFSIndex *index, list<ReadTask> *tasks, size_t size,
                off_t offset, char *buf)
{
    plfs_error_t ret;
    ssize_t bytes_remaining = size;
    ssize_t bytes_traversed = 0;
    int chunk = 0;
    ReadTask task;
    do {
        // find a read task
        ret = index->globalLookup(&(task.fh),
                                  &(task.chunk_offset),
                                  &(task.length),
                                  task.path,
                                  &task.backend,
                                  &(task.hole),
                                  &(task.chunk_id),
                                  offset+bytes_traversed);
        // make sure it's good
        if ( ret == PLFS_SUCCESS ) {
            task.length = min(bytes_remaining,(ssize_t)task.length);
	    if( buf != NULL ) task.buf = &(buf[bytes_traversed]);
	    task.logical_offset = offset + bytes_traversed;
            bytes_remaining -= task.length;
            bytes_traversed += task.length;
        }
        // then if there is anything to it, add it to the queue
        if ( ret == PLFS_SUCCESS && task.length > 0 ) {
            mss::mlog_oss oss(INT_DCOMMON);
            oss << chunk << ".1) Found index entry offset "
                << task.chunk_offset << " len "
                << task.length << " fh " << task.fh << " path "
                << task.path << endl;
            // check to see if we can combine small sequential reads
            // when merging is off, that breaks things even more.... ?
	    // there seems to be a merging bug now too
	    if ( ! tasks->empty() > 0 ) {
		ReadTask lasttask = tasks->back();
		if ( task.fh != NULL && lasttask.fh == task.fh &&
			lasttask.hole == task.hole &&
			lasttask.chunk_offset + (off_t)lasttask.length ==
			task.chunk_offset &&
			lasttask.logical_offset + (off_t)lasttask.length ==
			task.logical_offset &&
			buf != NULL &&
			lasttask.buf + lasttask.length ==
			task.buf) {
		    // merge last into this and pop last
                    oss << chunk++ << ".1) Merge with last index entry offset "
                        << lasttask.chunk_offset << " len "
                        << lasttask.length << " fh " << lasttask.fh
                        << endl;
                    task.chunk_offset = lasttask.chunk_offset;
                    task.length += lasttask.length;
                    task.buf = lasttask.buf;
		    task.logical_offset = lasttask.logical_offset;
                    tasks->pop_back();
                }
            }
            // remember this task
            oss.commit();
            tasks->push_back(task);
        }
        // when chunk_length is 0, that means EOF
    } while(bytes_remaining && ret == PLFS_SUCCESS && task.length);
    return(ret);
}

/* ret 0 or -err */
plfs_error_t
plfs_shard_builder(PLFSIndex *index, off_t offset, size_t size,
		   int loc_required, plfs_shard **head)
{
    plfs_error_t ret;
    list<ReadTask> tasks;   // a container of read tasks in case the logical

    index->lock(__FUNCTION__); // in case another FUSE thread in here
    // setting the last parameter to NULL means it doesn't associate buf segment
    // with each read task
    ret = find_read_tasks(index,&tasks,size,offset, NULL);
    index->unlock(__FUNCTION__); // in case another FUSE thread in here

    if( ret == PLFS_SUCCESS ){
	// build shard list depending on whether location is required
	ret = build_shrad_list(&tasks, head, loc_required);
    }

    return ret;
}

/* @param ret_readlen returns bytes read */
/* ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t
perform_read_task( ReadTask *task, PLFSIndex *index, ssize_t *ret_readlen )
{
    plfs_error_t err = PLFS_SUCCESS;
    ssize_t readlen;
    if ( task->hole ) {
        memset((void *)task->buf, 0, task->length);
        readlen = task->length;
    } else {
        if ( task->fh == NULL ) {
            // since the task was made, maybe someone else has stashed it
            index->lock(__FUNCTION__);
            task->fh = index->getChunkFh(task->chunk_id);
            index->unlock(__FUNCTION__);
            if ( task->fh == NULL) { // not currently stashed, we have to open
                bool won_race = true;   // assume we will be first stash
                // This is where the data chunk is opened.  We need to
                // create a helper function that does this open and reacts
                // appropriately when it fails due to metalinks
                // this is currently working with metalinks.  We resolve
                // them before we get here
                err = task->backend->store->Open(task->path.c_str(),
                                                 O_RDONLY, &(task->fh));
                if ( err != PLFS_SUCCESS ) {
                    mlog(INT_ERR, "WTF? Open of %s: %s",
                         task->path.c_str(), strplfserr(err) );
                    *ret_readlen = -1;
                    return err;
                }
                // now we got the fd, let's stash it in the index so others
                // might benefit from it later
                // someone else might have stashed one already.  if so,
                // close the one we just opened and use the stashed one
                index->lock(__FUNCTION__);
                IOSHandle *existing;
                existing = index->getChunkFh(task->chunk_id);
                if ( existing != NULL ) {
                    won_race = false;
                } else {
                    index->setChunkFh(task->chunk_id, task->fh);   // stash it
                }
                index->unlock(__FUNCTION__);
                if ( ! won_race ) {
                    task->backend->store->Close(task->fh);
                    task->fh = existing; // already stashed by someone else
                }
                mlog(INT_DCOMMON, "Opened fh %p for %s and %s stash it",
                     task->fh, task->path.c_str(),
                     won_race ? "did" : "did not");
            }
        }
        /* here's where we actually read container data! */
        err = task->fh->Pread(task->buf, task->length, task->chunk_offset,
                              &readlen );
    }
    mss::mlog_oss oss(INT_DCOMMON);
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
        << task->length << " fh " << task->fh << ": ret " << readlen;
    oss.commit();
    *ret_readlen = readlen;
    return(err);
}

// pop the queue, do some work, until none remains
void *
reader_thread( void *va )
{
    ReaderArgs *args = (ReaderArgs *)va;
    ReadTask task;
    ssize_t ret = 0, total = 0;
    bool tasks_remaining = true;
    while( true ) {
        Util::MutexLock(&(args->mux),__FUNCTION__);
	if ( args->titr != args->tasks->end() ) {
	    task = *(args->titr);
	    args->titr++;
        } else {
            tasks_remaining = false;
        }
        Util::MutexUnlock(&(args->mux),__FUNCTION__);
        if ( ! tasks_remaining ) {
            break;
        }
        plfs_error_t err = perform_read_task( &task, args->index, &ret );
        if ( err != PLFS_SUCCESS ) {
            ret = (ssize_t)(-err);
            break;
        } else {
            total += ret;
        }
    }
    if ( ret >= 0 ) {
        ret = total;
    }
    pthread_exit((void *) ret);
}

// using ThreadPool to finish each ReadTask for performance
// returns -err or bytes read
plfs_error_t
parallize_reader(list<ReadTask> &tasks, PLFSIndex *index, ssize_t *bytes_read)
{
    ssize_t total = 0;  // no bytes read so far
    plfs_error_t plfs_error = PLFS_SUCCESS;  // no error seen so far
    ssize_t ret = 0;    // for holding temporary return values
    PlfsConf *pconf = get_plfs_conf();
    if ( tasks.size() > 1 && pconf->threadpool_size > 1 ) {
        ReaderArgs args;
        args.index = index;
        args.tasks = &tasks;
	args.titr = tasks.begin();
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min((size_t)pconf->threadpool_size,tasks.size());
	mlog(INT_DCOMMON, "%s %lu THREADS", __FUNCTION__,
		(unsigned long)num_threads);
        ThreadPool threadpool(num_threads,reader_thread, (void *)&args);
        plfs_error = threadpool.threadError();   // returns PLFS_E*
        if ( plfs_error != PLFS_SUCCESS ) {
            mlog(INT_DRARE, "THREAD pool error %s", strplfserr(plfs_error) );
        } else {
            vector<void *> *stati    = threadpool.getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                mlog(INT_DCOMMON, "Thread %d returned %d", (int)t,int(ret));
                if ( ret < 0 ) {
                    plfs_error = errno_to_plfs_error(-ret);
                } else {
                    total += ret;
                }
            }
        }
        pthread_mutex_destroy(&(args.mux));
    } else {
	for (list<ReadTask>::iterator itr = tasks.begin();
	     itr != tasks.end(); itr++) {
	    ReadTask &task = *itr;
            plfs_error_t plfs_ret = perform_read_task( &task, index, &ret );
            if ( plfs_ret != PLFS_SUCCESS ) {
                plfs_error = plfs_ret;
            } else {
                total += ret;
            }
        }
    }
    *bytes_read = total;
    return plfs_error;
}

// a helper routine for read to allow it to be multi-threaded when a single
// logical read spans multiple chunks
// tasks needs to be a list and not a queue since sometimes it does pop_back
// here in order to consolidate sequential reads (which can happen if the
// index is not buffered on the writes)
plfs_error_t
find_read_tasksc(PLFSIndex *index, list<ReadTask> *tasks, size_t size,
		 off_t offset, char *buf, vector<struct iovec> &ranges)
{
    plfs_error_t ret;
    size_t bytes_remaining = size;
    size_t bytes_traversed = 0;
    ReadTask task;
    do {
	off_t partial_offset, loffset_itr = offset + bytes_traversed;
	off_t chunk_start;
	size_t partial_length;
	// find a read task
	ret = index->newLookup(loffset_itr,
			       &(task.fh),
			       task.path,
			       &task.backend,
			       &(task.chunk_id),
			       &(task.chunk_offset),
			       &(task.length),
			       &chunk_start,
			       &(task.checksum),
			       &partial_offset,
			       &partial_length,
			       &(task.hole));
	// make sure it's good
	if ( ret != PLFS_SUCCESS ) break;
	// Handle read pass EOF.
	if (task.length == 0) break;
	// Handle holes.
        struct iovec range;
	if (task.hole) {
	    size_t read_len = min(task.length, bytes_remaining);
	    task.buf = &(buf[bytes_traversed]);
	    task.length = read_len;
	    task.partial_read = false;
	    bytes_remaining -= read_len;
	    bytes_traversed += read_len;
            range.iov_base = task.buf;
            range.iov_len = task.length;
	    tasks->push_back(task);
	    ranges.push_back(range);
	    continue;
	}
	if (loffset_itr == chunk_start && partial_length == task.length
	    && chunk_start == partial_offset && bytes_remaining >= task.length)
	{
	    task.buf = &(buf[bytes_traversed]);
	    task.logical_offset = loffset_itr;
	    task.partial_read = false;
	    bytes_remaining -= task.length;
	    bytes_traversed += task.length;
            range.iov_base = task.buf;
            range.iov_len = task.length;
	} else {
	    // partial read.
	    size_t read_len = partial_offset + partial_length - loffset_itr;
	    read_len = min(read_len, bytes_remaining);
	    task.buf = (char *)malloc(task.length);
	    task.logical_offset = chunk_start;
	    task.partial_read = true;
	    off_t buffer_shift = loffset_itr - chunk_start;
	    task.msrc = &(task.buf[buffer_shift]);
	    task.mdst = &(buf[bytes_traversed]);
	    task.mlen = read_len;
            range.iov_base = task.msrc;
            range.iov_len = read_len;
	    bytes_remaining -= read_len;
	    bytes_traversed += read_len;
	}
	tasks->push_back(task);
        ranges.push_back(range);
	// when chunk_length is 0, that means EOF
    } while(bytes_remaining);
    return(ret);
}

// @param bytes_read returns bytes read
// returns PLFS_SUCCESS or PLFS_E*
// TODO: rename this to container_reader or something better
plfs_error_t
plfs_reader(void * /* pfd */, char *buf, size_t size, off_t offset,
            PLFSIndex *index, ssize_t *bytes_read)
{
    plfs_error_t ret;    // for holding temporary return values
    list<ReadTask> tasks;   // a container of read tasks in case the logical

    index->lock(__FUNCTION__); // in case another FUSE thread in here
    ret = find_read_tasks(index,&tasks,size,offset,buf);
    index->unlock(__FUNCTION__); // in case another FUSE thread in here
    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF
    if ( ret != PLFS_SUCCESS || tasks.empty() ) {
	*bytes_read = 0;
	return ret;
    }

    return parallize_reader(tasks, index, bytes_read);
}


plfs_error_t
plfs_readerc(void * /* pfd */, char *buf, size_t size, off_t offset,
	     PLFSIndex *index, ssize_t *bytes_read, Plfs_checksum *checksum)
{
    ssize_t total = 0;  // no bytes read so far
    plfs_error_t plfs_error = PLFS_SUCCESS;  // no error seen so far
    list<ReadTask> tasks;   // a container of read tasks in case the logical
    vector<struct iovec> ranges;
    // read spans multiple chunks so we can thread them
    // you might think that this can fail because this call is not in a mutex
    // so you might think it's possible that some other thread in a close is
    // changing ref counts right now but it's OK that the reference count is
    // off here since the only way that it could be off is if someone else
    // removes their handle, but no-one can remove the handle being used here
    // except this thread which can't remove it now since it's using it now
    // plfs_reference_count(pfd);
    assert(size > 0);
    index->lock(__FUNCTION__); // in case another FUSE thread in here
    plfs_error = find_read_tasksc(index,&tasks,size,offset,buf,ranges);
    index->unlock(__FUNCTION__); // in case another FUSE thread in here
    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF
    if ( plfs_error != PLFS_SUCCESS || tasks.empty() ) {
	*bytes_read = 0;
	return(plfs_error);
    }
    plfs_error = parallize_reader(tasks, index, &total);
    if (plfs_error != PLFS_SUCCESS) {
	for (list<ReadTask>::iterator itr = tasks.begin();
	     itr != tasks.end(); itr++) {
	    if (itr->partial_read) free(itr->buf);
	}
	return plfs_error;
    }

    // Read is done, accumulate the checksum now.
    if (tasks.size() == 1) { // Shortcut for a single read task.
	ReadTask &task = tasks.front();
	if (!task.partial_read && !task.hole) {
	    *bytes_read = total;
	    *checksum = task.checksum;
	    return plfs_error;
	}
    }
    total = 0;
    for (size_t i = 0; i < ranges.size(); i++) {
        total += ranges[i].iov_len;
    }
    struct iovec range = {NULL, total};
    plfs_error = plfs_recalc_checksum(&ranges[0], ranges.size(),
                                      &range, checksum, 1, 0);
    // Now verify the buffers read from index entries.
    for (list<ReadTask>::iterator itr = tasks.begin();
	 itr != tasks.end(); itr++) {
	ReadTask &task = *itr;
	if (task.hole) {
	    char *cbuf = (char *)task.buf;
	    for (size_t citr = 0; citr < task.length; citr++)
		if (cbuf[citr] != 0) plfs_error = PLFS_EIO;
	    continue;
	}
	if (task.partial_read) {
	    memcpy(task.mdst, task.msrc, task.mlen);
	}
	if (plfs_checksum_match(task.buf, task.length, task.checksum)) {
            mlog(INT_ERR, "Buffer mismatch %lu@%lx, checksum:0x%llx.",
                 (unsigned long)task.length,(unsigned long)task.logical_offset,
                 (unsigned long long)task.checksum);
	    plfs_error = PLFS_EIO;
	}
	if (task.partial_read) free(task.buf);
    }
    *bytes_read = total;
    return plfs_error;
}

// this is a helper function for reading file contents described by logical
// ranges <xvec, xvcnt> into memory segments described by <iov, iovcnt>. Memory
// segments and logical ranges are processed in array order.
// each logical range may span multiple data chunks, so this function just find
// out all read units first and then use multi-thread for performance.
// returns -err or bytes read
plfs_error_t
plfs_xreader(void *, struct iovec *iov, int iovcnt, plfs_xvec *xvec,
	     int xvcnt, PLFSIndex *index, ssize_t *bytes_read)
{
    int i, j;
    size_t remaining, bytes_remaining;
    plfs_error_t ret;
    off_t pos; // logical file offset reading from
    char *base = NULL; // memory buffer pointer reading to
    size_t size; // length of read operation
    size_t iovLen = 0; // total size of memory segments
    size_t xvecLen = 0; // total size of logical ranges
    list<ReadTask> tasks;

    // calculate the total size of memory segments and logical ranges
    for(i=0; i<iovcnt; i++){
	iovLen += iov[i].iov_len;
    }
    for(j=0; j<xvcnt; j++){
	xvecLen += xvec[j].len;
    }
    if(iovLen == 0 || xvecLen == 0) {
        *bytes_read = 0;
        return PLFS_EINVAL;
    }

    i = j = 0; // indicators of which iovec or plfs_xvec we are processing now
    pos = xvec[0].offset; // initialize to first plfs_xvec's starting offset
    base = (char *)iov[0].iov_base; // points to first iovec's starting address
    size = min(iov[0].iov_len, xvec[0].len);
    bytes_remaining = min(iovLen, xvecLen); // read ends when bytes read meet
					    // iovLen or xvecLen

    index->lock(__FUNCTION__); // in case another FUSE thread in here
    /* build read tasks */
    while(bytes_remaining > 0){
	ret = find_read_tasks(index, &tasks, size, pos, base);

	bytes_remaining -= size;
	if(base + size < (char *)iov[i].iov_base + iov[i].iov_len){
	    /* not done for this iovec yet */
	    j ++;
	    if (j >= xvcnt) break;
	    base += size;
	    pos = xvec[j].offset;
	    remaining = ((char *)iov[i].iov_base + iov[i].iov_len) - base;
	    size = min(remaining, xvec[j].len);
	} else {
	    /* done with current iovec */
	    i ++;
	    if( i >= iovcnt) break;
	    base = (char *)iov[i].iov_base;
	    pos += size;
	    remaining = xvec[j].offset + xvec[j].len - pos;
	    if(remaining == 0){
		/* also reached end of current plfs_xvec */
		j ++;
		if(j >= xvcnt) break;
		pos = xvec[j].offset;
		remaining = xvec[j].len;
	    }
	    size = min(iov[i].iov_len, remaining);
	}
    }
    index->unlock(__FUNCTION__); // in case another FUSE thread in here

    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF
    if ( ret != PLFS_SUCCESS || tasks.empty() ) {
        *bytes_read = 0;
        return ret;
    }

    return parallize_reader(tasks, index, bytes_read);
}

plfs_error_t
plfs_xreaderc(void *, struct iovec *iov, int iovcnt, plfs_xvec *xvec,
              int xvcnt, PLFSIndex *index, ssize_t *bytes_read,
              Plfs_checksum *checksum)
{
    int i, j;
    size_t remaining, bytes_remaining;
    plfs_error_t ret;
    off_t pos; // logical file offset reading from
    char *base = NULL; // memory buffer pointer reading to
    size_t size; // length of read operation
    size_t iovLen = 0; // total size of memory segments
    size_t xvecLen = 0; // total size of logical ranges
    list<ReadTask> tasks;
    vector<struct iovec> ranges;
    ssize_t total;

    // calculate the total size of memory segments and logical ranges
    for(i=0; i<iovcnt; i++){
	iovLen += iov[i].iov_len;
    }
    for(j=0; j<xvcnt; j++){
	xvecLen += xvec[j].len;
    }
    if(iovLen == 0 || xvecLen == 0) {
        *bytes_read = 0;
        return PLFS_EINVAL;
    }

    i = j = 0; // indicators of which iovec or plfs_xvec we are processing now
    pos = xvec[0].offset; // initialize to first plfs_xvec's starting offset
    base = (char *)iov[0].iov_base; // points to first iovec's starting address
    size = min(iov[0].iov_len, xvec[0].len);
    bytes_remaining = min(iovLen, xvecLen); // read ends when bytes read meet
					    // iovLen or xvecLen

    index->lock(__FUNCTION__); // in case another FUSE thread in here
    /* build read tasks */
    while(bytes_remaining > 0){
	ret = find_read_tasksc(index, &tasks, size, pos, base, ranges);

	bytes_remaining -= size;
	if(base + size < (char *)iov[i].iov_base + iov[i].iov_len){
	    /* not done for this iovec yet */
	    j ++;
	    if (j >= xvcnt) break;
	    base += size;
	    pos = xvec[j].offset;
	    remaining = ((char *)iov[i].iov_base + iov[i].iov_len) - base;
	    size = min(remaining, xvec[j].len);
	} else {
	    /* done with current iovec */
	    i ++;
	    if( i >= iovcnt) break;
	    base = (char *)iov[i].iov_base;
	    pos += size;
	    remaining = xvec[j].offset + xvec[j].len - pos;
	    if(remaining == 0){
		/* also reached end of current plfs_xvec */
		j ++;
		if(j >= xvcnt) break;
		pos = xvec[j].offset;
		remaining = xvec[j].len;
	    }
	    size = min(iov[i].iov_len, remaining);
	}
    }
    index->unlock(__FUNCTION__); // in case another FUSE thread in here

    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF
    if ( ret != PLFS_SUCCESS || tasks.empty() ) {
        *bytes_read = 0;
        return ret;
    }

    ret = parallize_reader(tasks, index, &total);
    if (ret != PLFS_SUCCESS) {
	for (list<ReadTask>::iterator itr = tasks.begin();
	     itr != tasks.end(); itr++) {
	    if (itr->partial_read) free(itr->buf);
	}
	return ret;
    }

    // Read is done, accumulate the checksum now.
    if (tasks.size() == 1) { // Shortcut for a single read task.
	ReadTask &task = tasks.front();
	if (!task.partial_read && !task.hole) {
	    *bytes_read = total;
	    *checksum = task.checksum;
	    return ret;
	}
    }
    ret = plfs_recalc_checksum(&ranges[0], ranges.size(),
                               iov, checksum, iovcnt, 0);
    // Now verify the buffers read from index entries.
    for (list<ReadTask>::iterator itr = tasks.begin();
	 itr != tasks.end(); itr++) {
	ReadTask &task = *itr;
	if (task.hole) {
	    char *cbuf = (char *)task.buf;
	    for (size_t citr = 0; citr < task.length; citr++)
		if (cbuf[citr] != 0) ret = PLFS_EIO;
	    continue;
	}
	if (task.partial_read) {
	    memcpy(task.mdst, task.msrc, task.mlen);
	    total -= task.length - task.mlen;
	}
	if (plfs_checksum_match(task.buf, task.length, task.checksum)) {
            mlog(INT_ERR, "Buffer mismatch %lu@%lx, checksum:0x%llx.",
                 (unsigned long)task.length,(unsigned long)task.logical_offset,
                 (unsigned long long)task.checksum);
	    ret = PLFS_EIO;
	}
	if (task.partial_read) free(task.buf);
    }
    *bytes_read = total;
    return ret;
}
