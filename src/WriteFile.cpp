#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
#include "IOStore.h"
#include "WriteFile.h"
#include "Container.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <string>
using namespace std;

// the path here is a physical path.  A WriteFile just uses one hostdir.
// so the path sent to WriteFile should be the physical path to the
// shadow or canonical container (i.e. not relying on symlinks)
// anyway, this should all happen above WriteFile and be transparent to
// WriteFile.  This comment is for educational purposes only.
WriteFile::WriteFile(string logical, string path, string newhostname,
                     mode_t newmode, size_t buffer_mbs,
                     struct plfs_backend *backend) : Metadata::Metadata()
{
    this->logical_path      = logical;
    this->container_path    = path;
    this->subdir_path       = path;     /* not really a subdir */
    this->subdirback        = backend;
    this->hostname          = newhostname;
    this->index             = NULL;
    this->mode              = newmode;
    this->has_been_renamed  = false;
    this->createtime        = Util::getTime();
    this->write_count       = 0;
    this->index_buffer_mbs  = buffer_mbs;
    this->max_writers       = 0;
    pthread_mutex_init( &data_mux, NULL );
    pthread_mutex_init( &index_mux, NULL );
}

void WriteFile::setContainerPath ( string p )
{
    this->container_path    = p;
    this->has_been_renamed = true;
}

/**
 * WriteFile::setSubdirPath: change the subdir path and backend
 *
 * XXXCDC: I think this might need some mutex protection, and we
 * should only allow it if the current number of writers is zero,
 * since all writers on one host currently must use the same
 * hostdir/backend.
 *
 * @param p new subdir (either on shadow or canonical)
 * @param wrback backend to use to access the subdir
 */
void WriteFile::setSubdirPath (string p, struct plfs_backend *wrback)
{
    this->subdir_path     = p;
    this->subdirback      = wrback;
}

WriteFile::~WriteFile()
{
    mlog(WF_DAPI, "Delete self %s", container_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
        index = NULL;
    }
    pthread_mutex_destroy( &data_mux );
    pthread_mutex_destroy( &index_mux );
}

// a helper function to set OpenFh for each WriteType
int WriteFile::setOpenFh(OpenFh *ofh, IOSHandle *fh, WriteType wType)
{
    assert(ofh != NULL && fh != NULL);
    switch( wType ){
       case SINGLE_HOST_WRITE:
          ofh->sh_fh = fh;
          break;
       case SIMPLE_FORMULA_WRITE:
       case SIMPLE_FORMULA_WITHOUT_INDEX:
          ofh->sf_fhs.push_back(fh);
          break;
       default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           return -1;
    }
    return 0;
}

/* ret 0 or -err */
int WriteFile::sync()
{
    int ret = 0;
    // iterate through and sync all open fhs
    Util::MutexLock( &data_mux, __FUNCTION__ );
    map<pid_t, OpenFh >::iterator pids_itr;
    WF_FH_ITR fh_itr;
    OpenFh *ofh;
    for( pids_itr = fhs.begin(); pids_itr != fhs.end() && ret==0; pids_itr++ ) {
        ofh = &pids_itr->second;
        for(fh_itr = ofh->sf_fhs.begin(); fh_itr != ofh->sf_fhs.end();
            fh_itr ++){
            ret = (*fh_itr)->Fsync();
        }
        ret = ofh->sh_fh->Fsync();
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

    // now sync the index
    Util::MutexLock( &index_mux, __FUNCTION__ );
    if ( ret == 0 ) {
        index->flush();
        index->sync();
    }
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

/* ret 0 or -err */
int WriteFile::sync( pid_t pid )
{
    int ret=0;
    OpenFh *ofh = getFh( pid );
    if ( ofh == NULL ) {
        // ugh, sometimes FUSE passes in weird pids, just ignore this
        //ret = -ENOENT;
    } else {
        WF_FH_ITR itr;
        for(itr = ofh->sf_fhs.begin(); itr != ofh->sf_fhs.end(); itr ++ ){
            ret = (*itr)->Fsync();
        }
        ret = ofh->sh_fh->Fsync();
        Util::MutexLock( &index_mux, __FUNCTION__ );
        if ( ret == 0 ) {
            index->flush();
            index->sync();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }
    return ret;
}

// returns -errno or number of writers
int WriteFile::addWriter( pid_t pid, bool child , WriteType wType)
{
    int ret = 0;
    Util::MutexLock(   &data_mux, __FUNCTION__ );
    struct OpenFh *ofh = getFh( pid );
    if (ofh != NULL) {
        ofh->writers++;
    } else {
        if (child==true){
            // child may use different hostdirId with parent, so
            // set it up corrently for child
            ContainerPaths c_paths;
            ret = findContainerPaths(logical_path,c_paths,pid);
            if (ret!=0){
                PLFS_EXIT(ret);
            }
            ret=Container::makeHostDir(c_paths, mode, PARENT_ABSENT,
                                       pid, Util::getTime());
        }
        /* note: this uses subdirback from object to open */
        IOSHandle *fh;
        fh = openDataFile(subdir_path, hostname, pid, DROPPING_MODE, wType, ret);
        if ( fh != NULL ) {
            struct OpenFh xofh;
            xofh.writers = 1;
            setOpenFh(&xofh,fh,wType);
            fhs[pid] = xofh;
        } else {
            ret = -errno;
        }
    }
    int writers = incrementOpens(0);
    if ( ret == 0 && ! child ) {
        writers = incrementOpens(1);
    }
    max_writers++;
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers",
         __FUNCTION__, pid, container_path.c_str(), writers );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

size_t WriteFile::numWriters( )
{
    int writers = incrementOpens(0);
    bool paranoid_about_reference_counting = false;
    if ( paranoid_about_reference_counting ) {
        int check = 0;
        Util::MutexLock(   &data_mux, __FUNCTION__ );
        map<pid_t, OpenFh >::iterator pids_itr;
        for( pids_itr = fhs.begin(); pids_itr != fhs.end(); pids_itr++ ) {
            check += pids_itr->second.writers;
        }
        if ( writers != check ) {
            mlog(WF_DRARE, "%s %d not equal %d", __FUNCTION__,
                 writers, check );
            assert( writers==check );
        }
        Util::MutexUnlock( &data_mux, __FUNCTION__ );
    }
    return writers;
}

// ok, something is broken again.
// it shows up when a makefile does a command like ./foo > bar
// the bar gets 1 open, 2 writers, 1 flush, 1 release
// and then the reference counting is screwed up
// the problem is that a child is using the parents fh
struct OpenFh *WriteFile::getFh( pid_t pid ) {
    map<pid_t,OpenFh>::iterator itr;
    struct OpenFh *ofh = NULL;
    if ( (itr = fhs.find( pid )) != fhs.end() ) {
        /*
            ostringstream oss;
            oss << __FILE__ << ":" << __FUNCTION__ << " found fh "
                << itr->second->fh << " with writers "
                << itr->second->writers
                << " from pid " << pid;
            mlog(WF_DCOMMON, "%s", oss.str().c_str() );
        */
        ofh = &(itr->second);
    } else {
        // here's the code that used to do it so a child could share
        // a parent fh but for some reason I commented it out
        /*
           // I think this code is a mistake.  We were doing it once
           // when a child was writing to a file that the parent opened
           // but shouldn't it be OK to just give the child a new datafile?
        if ( fhs.size() > 0 ) {
            ostringstream oss;
            // ideally instead of just taking a random pid, we'd rather
            // try to find the parent pid and look for it
            // we need this code because we've seen in FUSE that an open
            // is done by a parent but then a write comes through as the child
            mlog(WF_DRARE, "%s WARNING pid %d is not mapped. "
                    "Borrowing fh %d from pid %d",
                    __FILE__, (int)pid, (int)fds.begin()->second->fd,
                    (int)fds.begin()->first );
            ofd = fhs.begin()->second;
        } else {
            mlog(WF_DCOMMON, "%s no fd to give to %d", __FILE__, (int)pid);
            ofd = NULL;
        }
        */
        mlog(WF_DCOMMON, "%s no fh to give to %d", __FILE__, (int)pid);
        ofh = NULL;
    }
    return ofh;
}

IOSHandle* WriteFile::whichFh ( OpenFh *ofh, WriteType wType )
{
    IOSHandle *fh;
    if (ofh == NULL) return NULL;
    switch( wType ){
       case SINGLE_HOST_WRITE:
          fh = ofh->sh_fh;
          break;
       case SIMPLE_FORMULA_WRITE:
       case SIMPLE_FORMULA_WITHOUT_INDEX:
          if ( ofh->sf_fhs.empty() ){
              fh = NULL;
          }else{
              fh = ofh->sf_fhs.back();
          }
          break;
       default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           fh = NULL;
    }
    return fh;
}

/* uses this->subdirback for close */
/* ret 0 or -err */
int WriteFile::closeFh( IOSHandle *fh )
{
    map<IOSHandle *,string>::iterator paths_itr;
    paths_itr = paths.find( fh );
    string path = ( paths_itr == paths.end() ? "ENOENT?" : paths_itr->second );
    // XXX 
    int ret = this->subdirback->store->Close(fh);
    mlog(WF_DAPI, "%s:%s closed fh %p for %s: %d %s",
         __FILE__, __FUNCTION__, fh, path.c_str(), ret,
         ( ret != 0 ? strerror(-ret) : "success" ) );
    paths.erase ( fh );
    return ret;
}

// returns -err or number of writers
int
WriteFile::removeWriter( pid_t pid )
{
    int ret = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    struct OpenFh *ofh = getFh( pid );
    int writers = incrementOpens(-1);
    if ( ofh == NULL ) {
        // if we can't find it, we still decrement the writers count
        // this is strange but sometimes fuse does weird things w/ pids
        // if the writers goes zero, when this struct is freed, everything
        // gets cleaned up
        mlog(WF_CRIT, "%s can't find pid %d", __FUNCTION__, pid );
        assert( 0 );
    } else {
        ofh->writers--;
        if ( ofh->writers <= 0 ) {
            WF_FH_ITR itr;
            for(itr = ofh->sf_fhs.begin(); itr != ofh->sf_fhs.end(); itr ++ ){
                ret = closeFh(*itr);
            }
            ret = closeFh(ofh->sh_fh);
            fhs.erase( pid );
        }
    }
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers: %d",
         __FUNCTION__, pid, container_path.c_str(), writers, ret );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

int
WriteFile::extend( off_t offset )
{
    // make a fake write
    if ( fhs.begin() == fhs.end() ) {
        return -ENOENT;
    }
    pid_t p = fhs.begin()->first;
    index->extend(offset, p, createtime);
    addWrite( offset, 0 );   // maintain metadata
    return 0;
}

// we are currently doing synchronous index writing.
// this is where to change it to buffer if you'd like
// We were thinking about keeping the buffer around the
// entire duration of the write, but that means our appended index will
// have a lot duplicate information. buffer the index and flush on the close
//
// returns bytes written or -err
ssize_t
WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid,
                 WriteType wType)
{
    int ret = 0;
    ssize_t written;
    OpenFh *ofh = getFh( pid );
    if ( ofh == NULL ) {
        // we used to return -ENOENT here but we can get here legitimately
        // when a parent opens a file and a child writes to it.
        // so when we get here, we need to add a child datafile
        ret = addWriter( pid, true , wType);
        if ( ret > 0 ) {
            // however, this screws up the reference count
            // it looks like a new writer but it's multiple writers
            // sharing an fh ...
            ofh = getFh( pid );
        }
    }
    IOSHandle *wfh = whichFh(ofh, wType);
    if ( wfh == NULL ){
        wfh = openNewDataFile(pid, wType);
    }
    if ( wfh != NULL ) {
        // write the data file
        double begin, end;
        begin = Util::getTime();
        ret = written = ( size ? wfh->Write(buf, size ) : 0 );
        end = Util::getTime();
        // then the index
        if ( ret >= 0 ) {
            write_count++;
            Util::MutexLock(   &index_mux , __FUNCTION__);
            if (wType == SINGLE_HOST_WRITE){
                index->addWrite( offset, ret, pid, begin, end );
            }else if (wType == SIMPLE_FORMULA_WRITE){
                index->updateSimpleFormula(begin,end);
            }else if (wType == SIMPLE_FORMULA_WITHOUT_INDEX){
                // do nothing
            }else{
                mlog(WF_DCOMMON, "Unexpected write type %d", wType);
                return -1;
            }
            // TODO: why is 1024 a magic number?
            int flush_count = 1024;
            if (write_count%flush_count==0) {
                ret = index->flush();
                // Check if the index has grown too large stop buffering
                if(index->memoryFootprintMBs() > index_buffer_mbs) {
                    index->stopBuffering();
                    mlog(WF_DCOMMON, "The index grew too large, "
                         "no longer buffering");
                }
            }
            if (ret >= 0) {
                addWrite(offset, size);    // track our own metadata
            }
            Util::MutexUnlock( &index_mux, __FUNCTION__ );
        }
    }
    // return bytes written or error
    return((ret >= 0) ? written : ret);
}

// this assumes that the hostdir exists and is full valid path
// returns 0 or -errno
int WriteFile::openIndex( pid_t pid, WriteType wType) {
    int ret = 0;
    bool new_index = false;
    string index_path;

    /* note: this uses subdirback from obj to open */
    IOSHandle *fh = openIndexFile(subdir_path, hostname, pid, DROPPING_MODE,
                                  &index_path, wType, ret);
    if ( fh == NULL ) {
        ret = -errno;
    } else {
        if ( index == NULL ) {
           Util::MutexLock(&index_mux , __FUNCTION__);
           if ( index == NULL ){
              plfs_backend *iback;
              string bpath;
              ret = plfs_phys_backlookup(container_path.c_str(), NULL,
                                         &iback, &bpath);
              if (ret != 0) {
                 /* this shouldn't ever happen */
                 mlog(WF_CRIT, "openIndex: %s backlookup failed",
                       container_path.c_str());
                 Util::MutexUnlock(&index_mux, __FUNCTION__);
                 return ret;
              }
              index = new Index(container_path,iback);
              new_index = true;
           }
           Util::MutexUnlock(&index_mux, __FUNCTION__);
        }
        mlog(WF_DAPI, "In open Index path is %s",index_path.c_str());
        index->setCurrentFh(fh,index_path);
        if(new_index && index_buffer_mbs) {
            index->startBuffering();
        }
    }
    return ret;
}

int WriteFile::closeIndex( )
{
    int ret = 0;
    vector< IOSHandle * > fh_list;
    vector< IOSHandle * >::iterator itr;
    Util::MutexLock(   &index_mux , __FUNCTION__);
    // we are closing index, so enable flushing pattern index
    index->enablePidxFlush();
    ret = index->flush();
    index->getFh( fh_list );
    for(itr=fh_list.begin(); itr!=fh_list.end(); itr++){
        ret = closeFh( *itr );
    }
    delete( index );
    index = NULL;
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

// returns 0 or -err
IOSHandle* WriteFile::openNewDataFile( pid_t pid, WriteType wType )
{
    int ret;
    Util::MutexLock(   &data_mux, __FUNCTION__ );
    IOSHandle *fh = openDataFile( subdir_path, hostname, pid, DROPPING_MODE, wType, ret);
    if ( fh != NULL ) {
        struct OpenFh *ofh = getFh( pid );
        if (ofh == NULL){
            struct OpenFh xofh;
            xofh.writers = 1;
            setOpenFh(&xofh, fh, wType);
            fhs[pid] = xofh;
        } else {
            setOpenFh(ofh, fh, wType);
        }
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return fh; 
}

// returns 0 or -errno
int WriteFile::Close()
{
    int failures = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    map<pid_t,OpenFh >::iterator itr;
    WF_FH_ITR fh_itr;
    // these should already be closed here
    // from each individual pid's close but just in case
    for( itr = fhs.begin(); itr != fhs.end(); itr++ ) {
        for(fh_itr = itr->second.sf_fhs.begin();
            fh_itr != itr->second.sf_fhs.end();
            fh_itr++){
            if ( closeFh( *fh_itr ) != 0 ) {
                failures++;
            }
        }
        if ( closeFh( itr->second.sh_fh ) != 0 ){
            failures++;
        }
    }
    fhs.clear();
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( failures ? -EIO : 0 );
}

// returns 0 or -err
int WriteFile::truncate( off_t offset )
{
    Metadata::truncate( offset );
    index->truncateHostIndex( offset );
    return 0;
}

/* uses this->subdirback to open */
IOSHandle* WriteFile::openIndexFile(string path, string host, pid_t p, mode_t m,
                             string *index_path, WriteType wType, int &ret)
{
    switch (wType){
        case SINGLE_HOST_WRITE:
           *index_path = Container::getIndexPath(path,host,p,createtime,
                                                 BYTERANGE);
           break;
        case SIMPLE_FORMULA_WRITE:
           if (index->getFh(SIMPLEFORMULA) != NULL ) return NULL;
           // store SimpleFormula index file in canonical container
           // so we can always get the correct metalink locations,
           // even after renaming the file
           *index_path = Container::getIndexPath(container_path,host,p,
                                                 metalinkTime,SIMPLEFORMULA);
           break;
        case SIMPLE_FORMULA_WITHOUT_INDEX:
           // do nothing
           return NULL;
        default:
           mlog(WF_DRARE, "%s, unexpected write type", __FUNCTION__);
           return NULL;
    }
    mlog(WF_DBG, "WF: opening index file %s", (*index_path).c_str());
    return openFile(*index_path,m,ret);
}

/* uses this->subdirback to open */
IOSHandle* WriteFile::openDataFile(string path, string host, pid_t p, mode_t m,
                            WriteType wType, int &ret)
{
    string data_path;
    switch (wType){
        case SINGLE_HOST_WRITE:
           data_path = Container::getDataPath(path,host,p,createtime,
                                              BYTERANGE);
           break;
        case SIMPLE_FORMULA_WRITE:
        case SIMPLE_FORMULA_WITHOUT_INDEX:
           data_path = Container::getDataPath(path,host,p,formulaTime,
                                              SIMPLEFORMULA);
           break;
        default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           return NULL;
    }
    mlog(WF_DBG, "WF: opening data file %s", data_path.c_str());
    return openFile(data_path , m, ret);
}

// returns an fh or null
IOSHandle *WriteFile::openFile(string physicalpath, mode_t xmode, int &ret )
{
    mode_t old_mode=umask(0);
    int flags = O_WRONLY | O_APPEND | O_CREAT;
    IOSHandle *fh;
    fh = this->subdirback->store->Open(physicalpath.c_str(), flags, xmode, ret);
    mlog(WF_DAPI, "%s.%s open %s : %p %s",
         __FILE__, __FUNCTION__,
         physicalpath.c_str(),
         fh, ( fh == NULL ? strerror(-ret) : "SUCCESS" ) );
    if ( fh != NULL ) {
        paths[fh] = physicalpath;    // remember so restore works
    }
    umask(old_mode);
    return(fh);
}

// a helper function to resotre data file fh
IOSHandle* WriteFile::restore_helper(IOSHandle *fh, string *path)
{
    IOSHandle *retfh = NULL;
    int ret;
    map<IOSHandle*,string>::iterator paths_itr;
    paths_itr = paths.find( fh );
    if ( paths_itr == paths.end() ) {
        return retfh;
    }
    *path = paths_itr->second;
    if ( closeFh( fh ) != 0 ) {
        return retfh;
    }
    retfh = openFile( *path, mode ,ret);
    return retfh;
}

// we call this after any calls to f_truncate
// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// what if rename is called and then f_truncate?
// return 0 or -err
int WriteFile::restoreFhs( bool droppings_were_truncd )
{
    map<IOSHandle *,string>::iterator paths_itr;
    map<pid_t, OpenFh >::iterator pids_itr;
    OpenFh *ofh;
    IOSHandle *retfh;
    int ret = -ENOSYS;

    // "has_been_renamed" is set at "addWriter, setPath" executing path.
    // This assertion will be triggered when user open a file with write mode
    // and do truncate. Has nothing to do with upper layer rename so I comment
    // out this assertion but remain previous comments here.
    // if an open WriteFile ever gets truncated after being renamed, that
    // will be really tricky.  Let's hope that never happens, put an assert
    // to guard against it.  I guess it if does happen we just need to do
    // reg ex changes to all the paths
    //assert( ! has_been_renamed );
    mlog(WF_DAPI, "Entering %s",__FUNCTION__);
    // first reset the index fd
    if ( index ) {
        Util::MutexLock( &index_mux, __FUNCTION__ );
        index->flush();
        vector< IOSHandle * > fh_list;
        index->getFh( fh_list );
        vector< IOSHandle * >::iterator itr;
        index->index_paths.clear();
        for(itr=fh_list.begin(); itr!=fh_list.end(); itr++){
            string indexpath;
            if ( (retfh = restore_helper(*itr, &indexpath)) == NULL ){
                Util::MutexUnlock( &index_mux, __FUNCTION__ );
                return ret;
            }
            index->setCurrentFh( retfh, indexpath );
        }
        if (droppings_were_truncd) {
            // this means that they were truncd to 0 offset
            index->resetPhysicalOffsets();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }
    // then the data fhs
    for( pids_itr = fhs.begin(); pids_itr != fhs.end(); pids_itr++ ) {
        ofh = &pids_itr->second;
        string unused;
        // handle ByteRange data file first
        if ( (retfh = restore_helper(ofh->sh_fh, &unused)) == NULL ){
            return ret;
        } 
        ofh->sh_fh = retfh;
        // then SimpleFormula data files
        WF_FH_ITR fh_itr;
        for(fh_itr = ofh->sf_fhs.begin(); fh_itr != ofh->sf_fhs.end();
            fh_itr ++){
            if ( (retfh = restore_helper(*fh_itr, &unused)) == NULL ){
                return ret;
            }
            *fh_itr = retfh;
        }
    }
    // normally we return ret at the bottom of our functions but this
    // function had so much error handling, I just cut out early on any
    // error.  therefore, if we get here, it's happy days!
    mlog(WF_DAPI, "Exiting %s",__FUNCTION__);
    return 0;
}
