#include "COPYRIGHT.h"
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <dirent.h>
#include <math.h>
#include <assert.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

#include <time.h>
#include "plfs.h"
#include "IOStore.h"
#include "Container.h"
#include "Index.h"
#include "plfs_private.h"
#include "mlog_oss.h"


SingleByteRangeEntry::SingleByteRangeEntry()
{
    // valgrind complains about unitialized bytes in this thing
    // this is because there is padding in this object
    // so let's initialize our entire self

    // it inherits a pure class now, don't memset to zero. 
    // memset(this,0,sizeof(*this));
}

SingleByteRangeEntry::SingleByteRangeEntry(off_t o, size_t s, pid_t p)
{
    logical_offset = o;
    length = s;
    id = p;
}

SingleByteRangeEntry::SingleByteRangeEntry(const SingleByteRangeEntry& copy)
{
    // similar to standard constructor, this
    // is used when we do things like push a SingleByteRangeEntry
    // onto a vector.  We can't rely on default constructor bec on the
    // same valgrind complaint as mentioned in the first constructor
    //memset(this,0,sizeof(*this));
    memcpy(this,&copy,sizeof(*this));
}

bool
SingleByteRangeEntry::overlap( const SingleByteRangeEntry& other )
{
    return(contains(other.logical_offset) || other.contains(logical_offset));
}

bool
SingleByteRangeEntry::contains( off_t offset ) const
{
    return(offset >= logical_offset && offset < logical_offset + (off_t)length);
}

// subtly different from contains: excludes the logical offset
// (i.e. > instead of >=
bool
SingleByteRangeEntry::splittable( off_t offset ) const
{
    return(offset > logical_offset && offset < logical_offset + (off_t)length);
}

bool
SingleByteRangeEntry::preceeds( const SingleByteRangeEntry& other )
{
    return    logical_offset  + length == (unsigned int)other.logical_offset
              &&  physical_offset + length==(unsigned int)other.physical_offset
              &&  id == other.id;
}

bool
SingleByteRangeEntry::follows( const SingleByteRangeEntry& other )
{
    return other.logical_offset + other.length == (unsigned int)logical_offset
           && other.physical_offset+other.length==(unsigned int)physical_offset
           && other.id == id;
}

bool
SingleByteRangeEntry::abut( const SingleByteRangeEntry& other )
{
    return (follows(other) || preceeds(other));
}

off_t
SingleByteRangeEntry::logical_tail() const
{
    return logical_offset + (off_t)length - 1;
}

SimpleFormulaEntry::SimpleFormulaEntry()
{
    // valgrind complains about unitialized bytes in this thing
    // this is because there is padding in this object
    // so let's initialize our entire self
    //memset(this,0,sizeof(*this));
}

SimpleFormulaEntry::SimpleFormulaEntry(const SimpleFormulaEntry& copy)
{
    // similar to standard constructor, this
    // is used when we do things like push a SimpleFormulaEntry
    // onto a vector.  We can't rely on default constructor bec on the
    // same valgrind complaint as mentioned in the first constructor
    //memset(this,0,sizeof(*this));
    memcpy(this,&copy,sizeof(*this));
}

// because SimpleFormulaEntries are just listed in memory and
// never split or merge, so there is no need to implement overlap()
// for different entries
bool
SimpleFormulaEntry::overlap( const SimpleFormulaEntry& other )
{
    // not implemented yet
    return false;
}

// return the first overlapped range with <offset, length> pair, the overlapped
// range is represented with <o,s>
// lo is short for logical offset and po physical offset
// <lo, s> represents the logical overlapped range and po is
// the start offset in physical data file
bool
SimpleFormulaEntry::overlap( off_t offset, size_t length, off_t &lo,
                             off_t &po, size_t &s , pid_t &pid)
{
    pid_t rank;
    mlog(IDX_DAPI, "%s: logical_start_offset %ld, logical_end_offset %ld, "
          "offset %ld, length %ld", __FUNCTION__, (long)logical_start_offset,
          (long)logical_end_offset, (long)offset, length);

    // check if offset falls in in range of simpleFormulaEntry
    if( offset + (off_t)length <= logical_start_offset ||
        offset >= last_offset ){
        return false;
    }

    // offset falls in a gap, find the first block beyond offset
    // the first block beyond offset must be writen by rank0
    // overlap at first block
    if (offset <= logical_start_offset){
        lo = logical_start_offset;
        s = min((off_t)(offset + length - logical_start_offset),
                (off_t)write_size);
        s = min( (off_t)s, last_offset - logical_start_offset);
        po = 0;
        pid = 0;
        return true;
    }
    int numGap = 0, whichblock;
    off_t superblock_size,subblock_size,range;
    off_t superblock_off, block_off;
    range = logical_end_offset - logical_start_offset;
    if (strided == STRIDED ){
        // strided IO
        subblock_size = nw*write_size;
        if (numWrites > 1){
            superblock_size = (range - subblock_size) / (numWrites - 1);
        } else {
            assert(numWrites == 1);
            superblock_size = range;
        }
    } else {
        // segmented IO
        assert(strided == SEGMENTED);
        subblock_size = numWrites * write_size;
        superblock_size = (range - subblock_size) / (nw - 1);
    }
    // numGap is better called which_super
    numGap = (offset - logical_start_offset) / superblock_size;
    if ( offset >= (numGap * superblock_size + subblock_size
         + logical_start_offset) ){
        // this case can only happen when there are holes in formula
        // and offset falls in one of the holes
        if ( strided == STRIDED ){
            superblock_off = (numGap + 1) * superblock_size
                            + logical_start_offset;
            block_off = superblock_off;
            rank = 0;
        } else {
            superblock_off = numGap*superblock_size + logical_start_offset;
            block_off = superblock_off + superblock_size;
            rank = numGap;
        }
    } else {
        // subblock contains offset
        superblock_off = numGap * superblock_size + logical_start_offset;
        whichblock = (offset - superblock_off) / write_size;
        block_off = superblock_off + whichblock*write_size;
        if (strided == STRIDED){
            rank = whichblock;
        } else {
            rank = numGap;
        }
    }
    // find the first block beyond offset;
    if ( offset + (off_t)length <= block_off){
        // the whole range falls in a gap
        return false;
    }
    lo = max(block_off, offset);
    s = min((off_t)(offset + length) ,block_off + (off_t)write_size) - lo;
    if (lo > last_offset){
        return false;
    }
    s = min((off_t)s, last_offset - lo);
    if ( strided == STRIDED ){
        po = numGap*write_size + (offset - block_off);
    } else {
        po = whichblock*write_size + (offset - block_off);
    }
    pid = rank;
    return true;
}

// no need to implement for now. Use overlap to get
// the first overlappend range for given <offset,length> pair
// contains() and overlap() will share same logic
// so it could be implemented by overlap(offset,1,...) while
// introducing some overhead
bool
SimpleFormulaEntry::contains( off_t offset ) const
{
    // not implemented yet
    return false;
}

bool
SimpleFormulaEntry::splittable( off_t offset ) const
{
    // never split a simpleformula entry
    return false;
}

bool
SimpleFormulaEntry::preceeds( const SimpleFormulaEntry& other )
{
    // we only allow a simpleformula entry to grow larger
    return false;
}

bool
SimpleFormulaEntry::follows( const SimpleFormulaEntry& other )
{
    // quick path to check
    if (nw != other.nw || write_size != other.write_size
        || strided != other.strided
        || (long)other.logical_start_offset >= (long)logical_start_offset){
        return false;
    }
    // if other has been truncated, return false
    if ((long)other.last_offset < (long)other.logical_end_offset){
        return false;
    }
    assert(other.last_offset == other.logical_end_offset);

    if ( strided == SEGMENTED &&
         ((long)(other.logical_start_offset + other.numWrites*write_size) ==
          (long)logical_start_offset) ){
        return true;
    }
    if ( strided == STRIDED ){
        if ( (long)other.logical_end_offset > (long)logical_start_offset ){
            return false;
        }
        if ( numWrites == 1 ) {
            return true;
        }
        assert(numWrites > 1);
        size_t gap_sz = logical_start_offset - other.logical_end_offset;
        size_t block_sz = nw*write_size;
        if ( (long)(other.logical_start_offset + numWrites*(gap_sz+block_sz)) ==
             (long)logical_start_offset ){
             return true;
        }
    }
    return false;
}

bool
SimpleFormulaEntry::abut( const SimpleFormulaEntry& other )
{
    return (follows(other) || preceeds(other));
}

off_t
SimpleFormulaEntry::logical_tail() const
{
    return logical_end_offset;
}

// a helper routine for global_to_stream: copies to a pointer and advances it
char *
memcpy_helper(char *dst, void *src, size_t len) {
    char *ret = (char *)memcpy((void *)dst,src,len);
    ret += len;
    return ret;
}

// Addedd these next set of function for par index read
// might want to use the constructor for something useful
IndexFileInfo::IndexFileInfo()
{
}

/**
 * IndexFileInfo::listToStream: convert list of IndexFileInfos to byte stream
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param list the list to covert
 * @param bytes the number bytes allocated in returned buffer
 * @return the newly malloc'd buffer with the byte stream in it
 */
void *
IndexFileInfo::listToStream(vector<IndexFileInfo> &list,int *bytes)
{
    char *buffer;
    char *buf_pos;
    int size;
    vector<IndexFileInfo>::iterator itr;
    (*bytes) = 0;
    for(itr=list.begin(); itr!=list.end(); itr++) {
        (*bytes)+=sizeof(double);
        (*bytes)+=sizeof(pid_t);
        // IndexEntryType
        (*bytes)+=sizeof(IndexEntryType);
        (*bytes)+=sizeof(int);
        // Null terminating char
        (*bytes)+=(*itr).hostname.size()+1;
        (*bytes)+=sizeof(int);
        (*bytes)+=(*itr).path.size()+1;
    }
    // Make room for number of Index File Info
    (*bytes)+=sizeof(int);
    // This has to be freed somewhere
    buffer=(char *)calloc(1, *bytes);
    if(!buffer) {
        *bytes=-1;
        return (void *)buffer;
    }
    buf_pos=buffer;
    size=list.size();
    buf_pos=memcpy_helper(buf_pos,&size,sizeof(int));
    for(itr=list.begin(); itr!=list.end(); itr++) {
        double xtimestamp = (*itr).timestamp;
        pid_t  xid = (*itr).id;
        IndexEntryType xtype = (*itr).type;
        // Putting the plus one for the null terminating  char
        // Try using the strcpy function
        int len =(*itr).hostname.size()+1;
        mlog(IDX_DCOMMON, "Size of hostname is %d",len);
        int path_len = (*itr).path.size()+1;
        mlog(IDX_DCOMMON, "Size of path is %d",path_len);
        char *xhostname = strdup((*itr).hostname.c_str());
        char *xpath = strdup((*itr).path.c_str());
        buf_pos=memcpy_helper(buf_pos,&xtimestamp,sizeof(double));
        buf_pos=memcpy_helper(buf_pos,&xid,sizeof(pid_t));
        buf_pos=memcpy_helper(buf_pos,&xtype,sizeof(IndexEntryType));
        buf_pos=memcpy_helper(buf_pos,&len,sizeof(int));
        buf_pos=memcpy_helper(buf_pos,(void *)xhostname,len);
        buf_pos=memcpy_helper(buf_pos,&path_len,sizeof(int));
        buf_pos=memcpy_helper(buf_pos,(void *)xpath,path_len);
        free(xhostname);
        free(xpath);
    }
    return (void *)buffer;
}

/**
 * IndexFileInfo::streamToList: convert byte-stream to IndexFileInfo list
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param addr byte stream input (sized by number of entries)
 * @return the decoded IndexFileInfo
 */
vector<IndexFileInfo>
IndexFileInfo::streamToList(void *addr)
{
    vector<IndexFileInfo> list;
    int *sz_ptr;
    int size,count;
    sz_ptr = (int *)addr;
    size = sz_ptr[0];
    // Skip past the count
    addr = (void *)&sz_ptr[1];
    for(count=0; count<size; count++) {
        int hn_sz,path_sz;
        double *ts_ptr;
        pid_t *id_ptr;
        int *hnamesz_ptr,*pathsz_ptr;
        IndexEntryType *type_ptr;
        char *hname_ptr, *path_ptr;
        string xhostname,xpath;
        IndexFileInfo index_dropping;
        ts_ptr=(double *)addr;
        index_dropping.timestamp=ts_ptr[0];
        addr = (void *)&ts_ptr[1];
        id_ptr=(pid_t *)addr;
        index_dropping.id=id_ptr[0];
        addr = (void *)&id_ptr[1];
        type_ptr = (IndexEntryType *)addr;
        index_dropping.type = type_ptr[0];
        addr = (void *)&type_ptr[1];
        hnamesz_ptr=(int *)addr;
        hn_sz=hnamesz_ptr[0];
        addr= (void *)&hnamesz_ptr[1];
        hname_ptr=(char *)addr;
        xhostname.append(hname_ptr);
        index_dropping.hostname=xhostname;
        addr=(void *)&hname_ptr[hn_sz];
        pathsz_ptr=(int *)addr;
        path_sz=pathsz_ptr[0];
        addr= (void *)&pathsz_ptr[1];
        path_ptr=(char *)addr;
        xpath.append(path_ptr);
        index_dropping.path=xpath;
        addr=(void *)&path_ptr[path_sz];
        list.push_back(index_dropping);
        /*if(count==0 || count ==1){
            printf("stream to list size:%d \n",size);
            printf("TS :%f |",index_dropping.getTimeStamp());
            printf(" ID: %d |\n",index_dropping.getId());
            printf("HOSTNAME: %s\n",index_dropping.getHostname().c_str());
        }
        */
    }
    return list;
}

// for dealing with partial overwrites, we split entries in half on split
// points.  copy *this into new entry and adjust new entry and *this
// accordingly.  new entry gets the front part, and this is the back.
// return new entry
SingleByteRangeInMemEntry
SingleByteRangeInMemEntry::split(off_t offset)
{
    assert(contains(offset));   // the caller should ensure this
    SingleByteRangeInMemEntry front = *this;
    off_t split_offset = offset - this->logical_offset;
    front.length = split_offset;
    this->length -= split_offset;
    this->logical_offset += split_offset;
    this->physical_offset += split_offset;
    return front;
}

bool
SingleByteRangeInMemEntry::preceeds( const SingleByteRangeInMemEntry& other )
{
    if (!SingleByteRangeEntry::preceeds(other)) {
        return false;
    }
    return (physical_offset + (off_t)length == other.physical_offset);
}

bool
SingleByteRangeInMemEntry::follows( const SingleByteRangeInMemEntry& other )
{
    if (!SingleByteRangeEntry::follows(other)) {
        return false;
    }
    return (other.physical_offset + (off_t)other.length == physical_offset);
}

bool
SingleByteRangeInMemEntry::abut( const SingleByteRangeInMemEntry& other )
{
    return (preceeds(other) || follows(other));
}

bool
SingleByteRangeInMemEntry::mergable( const SingleByteRangeInMemEntry& other )
{
    return ( id == other.id && abut(other) );
}

ostream& operator <<(ostream& os,const SingleByteRangeInMemEntry& entry)
{
    double begin_timestamp = 0, end_timestamp = 0;
    begin_timestamp = entry.begin_timestamp;
    end_timestamp  = entry.end_timestamp;
    os  << setw(5)
        << entry.id             << " w "
        << setw(16)
        << entry.logical_offset << " "
        << setw(8) << entry.length << " "
        << setw(16) << fixed << setprecision(16)
        << begin_timestamp << " "
        << setw(16) << fixed << setprecision(16)
        << end_timestamp   << " "
        << setw(16)
        << entry.logical_tail() << " "
        << " [" << entry.id << "." << setw(10) << entry.physical_offset << "]";
    return os;
}

ostream& operator <<(ostream& os,const SimpleFormulaInMemEntry& entry)
{
    double begin_timestamp = 0, end_timestamp = 0;
    begin_timestamp = entry.begin_timestamp;
    end_timestamp  = entry.end_timestamp;
    os  << setw(5)
        << entry.nw << " " << entry.write_size << " "
        << entry.numWrites << " "
        << setw(16)
        << entry.logical_start_offset << " "
        << entry.logical_end_offset << " "
        << entry.last_offset << " "
        << setw(16) << fixed << setprecision(16)
        << begin_timestamp << " "
        << setw(16) << fixed << setprecision(16)
        << end_timestamp   << " "
        << setw(16) << fixed << setprecision(16)
        << entry.formulaTime << " ";
    switch ( entry.strided ){
       case STRIDED:
          os << "STRIDED";
          break;
       case SEGMENTED:
          os << "SEGMENTED";
          break;
       default:
          os << "UNKNOWN";
    }
    return os;
}

ostream& operator <<(ostream& os,const Index& ndx )
{
    os << "# Index of " << ndx.physical_path << endl;
    os << "# Data Droppings" << endl;
    os << "# SimpleFormula data droppings are not listed" << endl;
    for(unsigned i = 0; i < ndx.chunk_map.size(); i++ ) {
        /* XXX: maybe print backend prefix too? */
        if(ndx.chunk_map[i].bpath.find(DATAPREFIX) != string::npos){
            os << "# " << i << " " << ndx.chunk_map[i].backend->prefix <<
                ndx.chunk_map[i].bpath << endl;
        }
    }
    map<off_t,SingleByteRangeInMemEntry>::const_iterator itr;
    os << "# SingleByteRangeEntry Count: "
       << ndx.global_byteRange_index.size()
       << " consuming "
       << ndx.global_byteRange_index.size()*sizeof(SingleByteRangeEntry)
       << " bytes "<< endl;
    if ( ndx.global_byteRange_index.size() > 0 ){
        os << "# ID Logical_offset Length Begin_timestamp End_timestamp "
           << " Logical_tail ID.Chunk_offset " << endl;
        for(itr = ndx.global_byteRange_index.begin();
            itr != ndx.global_byteRange_index.end();
            itr++)
        {
            os << itr->second << endl;
        }
    }

    map< double, SimpleFormulaInMemEntry >::const_iterator sf_itr;
    os << "# SimpleFormulaEntry Count: "
       << ndx.global_simpleFormula_index.size()
       << " consuming "
       << ndx.global_simpleFormula_index.size()*sizeof(SimpleFormulaInMemEntry)
       << " bytes "<< endl;
    if ( ndx.global_simpleFormula_index.size() > 0 ){
        os << "# nw size numWrites Logical_start_offset Logical_end_offset "
           << " Last_offset Begin_timestamp End_timestamp FormulaTime type "
           << endl;
        for(sf_itr = ndx.global_simpleFormula_index.begin();
            sf_itr != ndx.global_simpleFormula_index.end(); sf_itr++) {
            os << sf_itr->second << endl;
        }
    }

    return os;
}

/*
 * XXX: code is not consistant about what it puts in "physical", sometimes
 * it is a top-level container directory, other times it is a specific
 * index file inside a container hostdir.
 */
void
Index::init( string physical, struct plfs_backend *ibackend )
{
    physical_path    = physical;
    this->iobjback = ibackend;   /* mainly for flush() ? */
    populated       = false;
    buffering       = false;
    buffer_filled   = false;
    compress_contiguous = true;
    chunk_id        = 0;
    last_offset     = 0;
    total_bytes     = 0;
    pidx_flush      = false; // disable pattern index flushing by default
    byteRangeIndex.clear();
    global_byteRange_index.clear();
    chunk_map.clear();
    pthread_mutex_init( &fh_mux, NULL );
}

Index::Index( string logical, struct plfs_backend *iback, IOSHandle *newfh )
    : Metadata::Metadata()
{
    init( logical, iback );
    this->fh = newfh;
    mlog(IDX_DAPI, "%s: created index on %s, fh=%p", __FUNCTION__,
         physical_path.c_str(), newfh);
}

void
Index::lock( const char *function )
{
    Util::MutexLock( &fh_mux, function );
}

void
Index::unlock( const char *function )
{
    Util::MutexUnlock( &fh_mux, function );
}

int
Index::resetPhysicalOffsets()
{
    map<pid_t,off_t>::iterator itr;
    for(itr=physical_offsets.begin(); itr!=physical_offsets.end(); itr++) {
        itr->second = 0;
        //physical_offsets[itr.first] = 0;
    }
    return 0;
}

Index::Index( string logical, struct plfs_backend *iback )
    : Metadata::Metadata()
{
    init( logical, iback );
    mlog(IDX_DAPI, "%s: created index on %s, %lu chunks", __FUNCTION__,
         physical_path.c_str(), (unsigned long)chunk_map.size());
}

void
Index::setPath( string p )  /* XXX: when do we use this? */
{
    this->physical_path = p;
}

int
Index::getFh( vector<IOSHandle *> &list) {
   assert(list.size() == 0);

   map< IOSHandle*, string>::iterator itr;
   for (itr = index_paths.begin(); itr != index_paths.end(); itr ++){
       list.push_back(itr->first);
   }
   return 0;
}

void
Index::setCurrentFh(IOSHandle *ifh , string indexpath )
{
   if (indexpath.find(FORMULAINDEXPREFIX) != string::npos){
      current_fh[SIMPLEFORMULA] = ifh;
   } else if (indexpath.find(BYTERANGEINDEXPREFIX) != string::npos){
      current_fh[BYTERANGE] = ifh;
   }
   index_paths[ifh] = indexpath;
}

Index::~Index()
{
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this
       << " removing index on " << physical_path << ", "
       << chunk_map.size() << " chunks";
    mlog(IDX_DAPI, "%s", os.str().c_str() );
    mlog(IDX_DCOMMON, "There are %lu chunks to close fhs for",
         (unsigned long)chunk_map.size());
    /*
     * XXX: things are currently setup so that this does not close
     * this->fd.   worth noting and keeping an eye on it.
     */
    for( unsigned i = 0; i < chunk_map.size(); i++ ) {
        if ( chunk_map[i].fh != NULL) {
            mlog(IDX_DCOMMON, "Closing fh %p for %s",
                 chunk_map[i].fh, chunk_map[i].bpath.c_str() );
            chunk_map[i].backend->store->Close(chunk_map[i].fh);
        }
    }
    pthread_mutex_destroy( &fh_mux );
    // I think these just go away, no need to clear them
    /*
    byteRangeIndex.clear();
    global_byteRange_index.clear();
    chunk_map.clear();
    */
}

void
Index::startBuffering()
{
    this->buffering=true;
    this->buffer_filled=false;
}

void
Index::stopBuffering()
{
    this->buffering=false;
    this->buffer_filled=true;
    global_byteRange_index.clear();
    global_simpleFormula_index.clear();
}

bool
Index::isBuffering()
{
    return this->buffering;
}

// this function makes a copy of the index
// and then clears the existing one
// walks the copy and merges where possible
// and then inserts into the existing one
void
Index::compress()
{
    return;
    /*
        this whole function is deprecated now that
        we successfully compress at the time that we
        build the index for both writes and reads.
        It was just a bandaid for after the fact compression
        which is now no longer neeeded.
        Furthermore, it is buggy since it merges entries which
        abut backwards whereas it should only merge those which
        abut forwards (i.e. yes when b follows a but no conversely)
    */
    /*
    if ( global_byteRange_index.size() <= 1 ) return;
    map<off_t,SingleByteRangeInMemEntry> old_global = global_byteRange_index;
    map<off_t,SingleByteRangeInMemEntry>::const_iterator itr=old_global.begin();
    global_byteRange_index.clear();
    SingleByteRangeInMemEntry pEntry = itr->second;
    while( ++itr != old_global.end() ) {
        if ( pEntry.mergable( itr->second ) ) {
            pEntry.length += itr->second.length;
        } else {
            insertGlobal( &pEntry );
            pEntry = itr->second;
        }
    }
    // need to put in the last one(s)
    insertGlobal( &pEntry );
    */
}

// merge another index into this one
// we're not looking for errors here probably we should....
void
Index::merge(Index *other)
{
    // the other has it's own chunk_map and the SingleByteRangeInMemEntry have
    // an index into that chunk_map
    // copy over the other's chunk_map and remember how many chunks
    // we had originally
    size_t chunk_map_shift = chunk_map.size();
    vector<ChunkFile>::iterator itr;
    for(itr = other->chunk_map.begin(); itr != other->chunk_map.end(); itr++) {
        chunk_map.push_back(*itr);
    }
    // copy over the other's container entries but shift the index
    // so they index into the new larger chunk_map
    map<off_t,SingleByteRangeInMemEntry>::const_iterator ce_itr;
    map<off_t,SingleByteRangeInMemEntry> *og = &(other->global_byteRange_index);
    for( ce_itr = og->begin(); ce_itr != og->end(); ce_itr++ ) {
        SingleByteRangeInMemEntry entry = ce_itr->second;
        // Don't need to shift in the case of flatten on close
        entry.id += chunk_map_shift;
        insertGlobal(&entry);
    }

    // handling simpleFormula index
    // maintain indexing between index entries and chunk_map
    map<double,SimpleFormulaInMemEntry>::const_iterator sf_itr;
    for ( sf_itr = other->global_simpleFormula_index.begin();
          sf_itr != other->global_simpleFormula_index.end(); sf_itr ++)
    {
        SimpleFormulaInMemEntry sf_entry = sf_itr->second;
        sf_entry.id += chunk_map_shift;
        // SimpleFormula index entries will never merge
        // so just do insert operation here
        global_simpleFormula_index[sf_entry.end_timestamp] = sf_entry;
    }
    last_offset = max( other->last_offset, last_offset );
    total_bytes += other->total_bytes;
}

off_t
Index::lastOffset()
{
    return last_offset;
}

size_t
Index::totalBytes()
{
    return total_bytes;
}

bool
Index::ispopulated( )
{
    return populated;
}

// returns 0 or -err
// this dumps the local index
// and then clears it
int
Index::flush()
{
    // ok, vectors are guaranteed to be contiguous
    // so just dump it in one fell swoop
    size_t  len = byteRangeIndex.size() * sizeof(SingleByteRangeEntry);
    int ret;
    void *start;
    mlog(IDX_DAPI, "%s flushing %lu bytes", __FUNCTION__, (unsigned long)len);
    if ( len != 0 ) {
        // valgrind complains about writing uninitialized bytes here....
        // but it's fine as far as I can tell.
        start = &(byteRangeIndex.front());
        ret     = Util::Writen(start, len, getFh(BYTERANGE) );
        if ( (size_t)ret != (size_t)len ) {
            mlog(IDX_DRARE, "%s failed write to fh %p: %s",
                __FUNCTION__, getFh(BYTERANGE), strerror(errno));
        }
        byteRangeIndex.clear();
    }

    // flush simpleformulaIndex
    // for simpleFormula index, only rank0 opens index dropping
    // so it's ok to fail to get index fh for other ranks
    IOSHandle *ifh = getFh(SIMPLEFORMULA);
    len = simpleFormulaIndex.size() *sizeof(SimpleFormulaEntry);
    if ( pidx_flush == false || ifh == NULL || len == 0 ) {
        return 0;
    }
    start = &(simpleFormulaIndex.front());
    ret = Util::Writen( start, len, ifh);
    if ( (size_t)ret != (size_t)len ) {
        mlog(IDX_DRARE, "%s failed write to fh %p: %s",
             __FUNCTION__, ifh, strerror(errno));
    }
    simpleFormulaIndex.clear();
    return ( ret < 0 ? -errno : 0 );
}

int
Index::sync()
{
    int ret;
    map<IOSHandle*, string>::iterator itr;
    for(itr=index_paths.begin(); itr!=index_paths.end(); itr++){
        ret = itr->first->Fsync();
    }
    return ret;
}

// takes a path and returns a ptr to the mmap of the file
// also computes the length of the file
// Update: seems to work with metalink
// this is for reading an index file
// only called by Index::readIndex.  must call cleanupReadIndex to
// close and unmap
// return 0 or -err
int
Index::mapIndex( void **ibufp, string hostindex, IOSHandle **xfh,
                 off_t *length, struct plfs_backend *hback)
{
    int ret;
    *xfh = hback->store->Open(hostindex.c_str(), O_RDONLY, ret);
    if ( *xfh == NULL ) {
        mlog(IDX_DRARE, "%s WTF open: %s", __FUNCTION__, strerror(-ret));
        /* play it safe in case store doesn't set ret properly */
        *ibufp = NULL;
        *length = 0;
        return(ret);
    }

    // lseek doesn't always see latest data if panfs hasn't flushed
    // could be a zero length chunk although not clear why that gets
    // created.
    *length = (*xfh)->Size();
    if ( *length == 0 ) {
        /* this can happen if index !flushed, or after a truncate */
        mlog(IDX_DRARE, "%s is a zero length index file", hostindex.c_str());
        *ibufp = NULL;
        return(0);
    }
    if (*length < 0) {
        mlog(IDX_DRARE, "%s WTF lseek: %s", __FUNCTION__,
             strerror(-(*length)));
        return(*length);
    }
    
    ret = (*xfh)->GetDataBuf(ibufp, *length);
    return(ret);
}


// this builds a global in-memory index from a physical host index dropping
// return 0 for sucess, -err for failure
int Index::readIndex( string hostindex, struct plfs_backend *hback )
{
    int rv;
    off_t length = (off_t)-1;
    IOSHandle *rfh = NULL;
    void  *maddr = NULL;
    populated = true;
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this << " reading index on " <<
       physical_path;
    mlog(IDX_DAPI, "%s", os.str().c_str() );

    rv = mapIndex(&maddr, hostindex, &rfh, &length, hback);

    if( rv < 0) {
        return cleanupReadIndex( rfh, maddr, length, rv, "mapIndex",
                                 hostindex.c_str(), hback );
    }
    if (hostindex.find(FORMULAINDEXPREFIX) != string::npos){
        // handling with simpleFormula index file
        mlog(IDX_DCOMMON,"%s: handling a simpleFormulaIndex file",__FUNCTION__);
        SimpleFormulaEntry *s_index = (SimpleFormulaEntry *)maddr;
        // shouldn't be partials
        size_t entries     = length / sizeof(SimpleFormulaEntry);
        // hostindex looks like,container/HOSTDIRPREFIX.ID/FORMULAINDEXPREFIX.TS
        string timestamp = hostindex.substr(
              hostindex.find(FORMULAINDEXPREFIX) + strlen(FORMULAINDEXPREFIX));
        double metalinkTime = atof(timestamp.c_str());
        for( size_t i = 0; i < entries; i++ ) {
            SimpleFormulaInMemEntry m_entry;
            SimpleFormulaEntry entry = s_index[i];
            ChunkFile cf;
            // One SimpleFormulaEntry can map to multiple data files resides
            // in different backends.
            // when reconstructing index from a flattened in-memory stream,
            // chunk_map will be regenerated by slashing a long paths stream
            // So for each cf, stash container path into it temporarily.
            // when reading acctually happens for a data file, the full path
            // will be filled in
            cf.bpath = hostindex.substr(0,hostindex.find(HOSTDIRPREFIX));
            cf.fh = NULL;
            cf.backend = NULL; 
            m_entry.nw                   = entry.nw;
            m_entry.write_size           = entry.write_size;
            m_entry.numWrites            = entry.numWrites;
            m_entry.logical_start_offset = entry.logical_start_offset;
            m_entry.logical_end_offset   = entry.logical_end_offset;
            m_entry.last_offset          = entry.last_offset;
            m_entry.formulaTime          = entry.formulaTime;
            m_entry.strided              = entry.strided;
            m_entry.begin_timestamp      = entry.begin_timestamp;
            m_entry.end_timestamp        = entry.end_timestamp;
            m_entry.id                   = chunk_id;
            m_entry.metalinkTime         = metalinkTime;

            // maintain consistency
            chunk_id += m_entry.nw;
            chunk_map.insert(chunk_map.end(), m_entry.nw, cf);

            global_simpleFormula_index[m_entry.end_timestamp] = m_entry;
            last_offset = max( (off_t)m_entry.last_offset, last_offset );
            total_bytes += m_entry.nw * m_entry.write_size * m_entry.numWrites;
        }
        mlog(IDX_DCOMMON, "%s: found %ld simpleFormulaEntries",
              __FUNCTION__, global_simpleFormula_index.size());
        return cleanupReadIndex( rfh, maddr, length, 0, "mapIndex",
                                 hostindex.c_str(), hback );
    }
    // ok, there's a bunch of data structures in here
    // some temporary some more permanent
    // each entry in the Container index has a chunk id (id)
    // which is a number from 0 to N where N is the number of chunks
    // the chunk_map is an instance variable within the Index which
    // persists for the lifetime of the Index which maps a chunk id
    // to a ChunkFile which is just a path and an fh.
    // now, this function gets called once for each hostdir
    // within each hostdir is a set of chunk files.  The host entry
    // has a pid in it.  We can use that pid to find the corresponding
    // chunk path.  Then we remember, just while we're reading the hostdir,
    // which chunk id we've assigned to each chunk path.  we could use
    // our permanent chunk_map to look this up but it'd be a backwards
    // lookup so that might be slow for large N's.
    // we do however remember the original pid so that we can rewrite
    // the index correctly for the cases where we do the reverse thing
    // and recreate a host index dropping (we do this for truncating to
    // the middle and for flattening an index)
    // since the order of the entries for each pid in a host index corresponds
    // to the order of the writes within that pid's chunk file, we also
    // remember the current offset for each chunk file (but we only need
    // to remember that for the duration of this function bec we stash the
    // important stuff that needs to be more permanent into the container index)
    // need to remember a chunk id for each distinct chunk file
    // we used to key this with the path but we can just key it with the id
    map<pid_t,pid_t> known_chunks;
    map<pid_t,pid_t>::iterator known_chunks_itr;
    // so we have an index mapped in, let's read it and create
    // mappings to chunk files in our chunk map
    SingleByteRangeEntry *h_index = (SingleByteRangeEntry *)maddr;
    // shouldn't be partials
    size_t entries     = length / sizeof(SingleByteRangeEntry);
    // but any will be ignored
    mlog(IDX_DCOMMON, "There are %lu in %s",
         (unsigned long)entries, hostindex.c_str() );
    for( size_t i = 0; i < entries; i++ ) {
        SingleByteRangeInMemEntry c_entry;
        SingleByteRangeEntry      h_entry = h_index[i];
        //  too verbose
        //mlog(IDX_DCOMMON, "Checking chunk %s", chunkpath.c_str());
        // remember the mapping of a chunkpath to a chunkid
        // and set the initial offset
        if( known_chunks.find(h_entry.id) == known_chunks.end() ) {
            ChunkFile cf;
            cf.bpath = Container::chunkPathFromIndexPath(hostindex,h_entry.id);
            cf.backend = hback;
            cf.fh   = NULL;
            chunk_map.push_back( cf );
            known_chunks[h_entry.id]  = chunk_id++;
            // chunk_map is indexed by chunk_id so these need to be the same
            assert( (size_t)chunk_id == chunk_map.size() );
            mlog(IDX_DCOMMON, "Inserting chunk %s (%lu)", cf.bpath.c_str(),
                 (unsigned long)chunk_map.size());
        }
        // copy all info from the host entry to the global and advance
        // the chunk offset
        // we need to remember the original chunk so we can reverse
        // this process and rewrite an index dropping from an index
        // in-memory data structure
        c_entry.logical_offset    = h_entry.logical_offset;
        c_entry.length            = h_entry.length;
        c_entry.id                = known_chunks[h_entry.id];
        c_entry.original_chunk    = h_entry.id;
        c_entry.physical_offset   = h_entry.physical_offset;
        c_entry.begin_timestamp   = h_entry.begin_timestamp;
        c_entry.end_timestamp     = h_entry.end_timestamp;
        int ret = insertGlobal( &c_entry );
        if ( ret != 0 ) {
            /* caller should prob discard index if we fail here */
            return cleanupReadIndex( rfh, maddr, length, ret, "insertGlobal",
                                     hostindex.c_str(), hback );
        }
    }
    mlog(IDX_DAPI, "After %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)chunk_map.size());
    return cleanupReadIndex(rfh, maddr, length, 0, "DONE",hostindex.c_str(),
                            hback);
}

// constructs a global index from a "stream" (i.e. a chunk of memory)
// returns 0 or -err
// format:
//    <quant> [ContainerEntry list] [chunk paths]
int Index::global_from_stream(void *addr)
{
    // first read the header to know how many entries there are
    size_t quant = 0, sf_quant = 0;
    size_t *sarray = (size_t *)addr;
    quant = sarray[0];
    mlog(IDX_DAPI, "%s for %s has %ld entries",
         __FUNCTION__,physical_path.c_str(),(long)quant);
    // then skip past the header
    addr = (void *)&(sarray[1]);
    // then read in all the entries
    SingleByteRangeInMemEntry *entries = (SingleByteRangeInMemEntry *)addr;
    for(size_t i=0; i<quant; i++) {
        SingleByteRangeInMemEntry e = entries[i];
        // just put it right into place. no need to worry about overlap
        // since the global index on disk was already pruned
        // UPDATE : The global index may never touch the disk
        // this happens on our broadcast on close optimization
        // Something fishy here we insert the address of the entry
        // in the insertGlobal code
        //global_byteRange_index[e.logical_offset] = e;
        insertGlobalEntry(&e);
    }
    // then skip past the entries
    addr = (void *)&(entries[quant]);

    // then read in all simpleFormula entries
    sarray = (size_t *)addr;
    sf_quant = sarray[0];
    if ( sf_quant < 0 ){
        return -EBADF;
    }
    mlog(IDX_DAPI, "%s for %s has %ld simpleFormula entries",
         __FUNCTION__,physical_path.c_str(),(long)sf_quant);
    addr = (void *)&(sarray[1]);
    SimpleFormulaInMemEntry *sfentries = (SimpleFormulaInMemEntry *)addr;
    for (size_t i=0; i<sf_quant; i++) {
        SimpleFormulaInMemEntry sf = sfentries[i];
        global_simpleFormula_index[sf.end_timestamp] = sf;

        last_offset = max( (off_t)sf.logical_end_offset,last_offset );
        total_bytes += sf.nw * sf.write_size * sf.numWrites;
    }
    addr = (void *)&(sfentries[sf_quant]);

    mlog(IDX_DCOMMON, "%s of %s now parsing data chunk paths",
         __FUNCTION__,physical_path.c_str());
    vector<string> chunk_paths;
    Util::tokenize((char *)addr,"\n",chunk_paths); // might be inefficient...
    for( size_t i = 0; i < chunk_paths.size(); i++ ) {
#if 0
        if(chunk_paths[i].size()<7) {
            continue;    // WTF does <7 mean???
            /*
             * XXXCDC: chunk path has a minimum length, 7 may not be
             * correct?  maybe change it to complain loudly if this
             * ever fires? (what is the min chunk size?)
             */
        }
#endif
        int ret;
        ChunkFile cf;
        // we used to strip the physical path off in global_to_stream
        // and add it back here.  See comment in global_to_stream for why
        // we don't do that anymore
        //cf.path = physical_path + "/" + chunk_paths[i];

        ret = plfs_phys_backlookup(chunk_paths[i].c_str(), NULL,
                                   &cf.backend, &cf.bpath);
        if (ret != 0) {
            //XXXCDC: NOW WHAT?   not going to be able to read the
            //data log files without a valid backend, so we are sunk
            //if we try to read
        }
        cf.fh = NULL;
        chunk_map.push_back(cf);
    }
    return 0;
}

// Helper function to debug global_to_stream
int Index::debug_from_stream(void *addr)
{
    // first read the header to know how many entries there are
    size_t quant = 0, sf_quant = 0;
    size_t *sarray = (size_t *)addr;
    quant = sarray[0];
    mlog(IDX_DAPI, "%s for %s has %ld entries",
         __FUNCTION__,physical_path.c_str(),(long)quant);
    // then skip past the entries
    SingleByteRangeInMemEntry *entries = (SingleByteRangeInMemEntry *)addr;
    addr = (void *)&(entries[quant]);
    // read in simpleFormula entries
    sarray = (size_t *)addr;
    sf_quant = sarray[0];
    if (sf_quant < 0){
        mlog(IDX_DRARE, "WTF the size simpleFormula entry of your "
              "stream index is less than 0");
        return -1;
    }
    // then skip simpleFormula entries
    SimpleFormulaInMemEntry *sfentries = (SimpleFormulaInMemEntry *)addr;
    addr = (void *)&(sfentries[sf_quant]);

    // now read in the vector of chunk files
    mlog(IDX_DCOMMON, "%s of %s now parsing data chunk paths",
         __FUNCTION__,physical_path.c_str());
    vector<string> chunk_paths;
    Util::tokenize((char *)addr,"\n",chunk_paths); // might be inefficient...
    for( size_t i = 0; i < chunk_paths.size(); i++ ) {
        mlog(IDX_DCOMMON, "Chunk path:%lu is :%s",
             (unsigned long)i,chunk_paths[i].c_str());
    }
    return 0;
}

// this writes a flattened in-memory global index to a physical file
// returns 0 or -err
int Index::global_to_file(IOSHandle *xfh, struct plfs_backend *canback)
{
    void *buffer;
    size_t length;
    int ret = global_to_stream(&buffer,&length);
    if (ret==0) {
        ret = Util::Writen(buffer,length,xfh);
        if (ret >= 0) {   /* let -err pass up to caller */
            ret = 0;
        }
        free(buffer);
    }
    return ret;
}

// this writes a flattened in-memory global index to a memory address
// it allocates the memory.  The caller must free it.
// returns 0 or -err
int Index::global_to_stream(void **buffer,size_t *length)
{
    int ret = 0;
    // Global ?? or this
    size_t quant = global_byteRange_index.size();
    size_t sf_quant = global_simpleFormula_index.size();
    //Check if we stopped buffering, if so return -1 and length of -1
    if(!buffering && buffer_filled) {
        *length=(size_t)-1;
        return -1;
    }
    // first build the vector of chunk paths, trim them to relative
    // to the container so they're smaller and still valid after rename
    // this gets written last but compute it first to compute length
    // the problem is that some of them might contain symlinks!
    // maybe we can handle this in one single place.  If we can find
    // the one single place where we open the data chunk we can handle
    // an error there possibly and reconstruct the full path.  Oh but
    // we also have to find the place where we open the index chunks
    // as well.
    ostringstream chunks;
    for(unsigned i = 0; i < chunk_map.size(); i++ ) {
        /*
           // we used to optimize a bit by stripping the part of the path
           // up to the hostdir.  But now this is problematic due to backends
           // if backends have different lengths then this strip isn't correct
           // the physical path is to canonical but some of the chunks might be
           // shadows.  If the length of the canonical_backend isn't the same
           // as the length of the shadow, then this code won't work.
           // additionally, we saw that sometimes we had extra slashes '///' in
           // the paths.  That also breaks this.
           // so just put full path in.  Makes it a bit larger though ....
        string chunk_path = chunk_map[i].path.substr(physical_path.length());
        mlog(IDX_DCOMMON, "%s: constructed %s from %s", __FUNCTION__,
                chunk_path.c_str(), chunk_map[i].path.c_str());
        chunks << chunk_path << endl;
        */
        chunks << chunk_map[i].bpath << endl;
    }
    chunks << '\0'; // null term the file
    size_t chunks_length = chunks.str().length();
    // compute the length
    *length = sizeof(quant);    // the header
    *length += quant*sizeof(SingleByteRangeInMemEntry);
    *length += sizeof(sf_quant);
    *length += sf_quant*sizeof(SimpleFormulaInMemEntry);
    *length += chunks_length;
    // allocate the buffer
    *buffer = calloc(1, *length);
    // Let's check this malloc and make sure it succeeds
    if(!buffer) {
        mlog(IDX_DRARE, "%s, Malloc of stream buffer failed",__FUNCTION__);
        return -1;
    }
    char *ptr = (char *)*buffer;
    if ( ! *buffer ) {
        return -ENOMEM;
    }
    // copy in the header
    ptr = memcpy_helper(ptr,&quant,sizeof(quant));
    mlog(IDX_DCOMMON, "%s: Copied header for global index of %s",
         __FUNCTION__, physical_path.c_str());
    // copy in each container entry
    size_t  centry_length = sizeof(SingleByteRangeInMemEntry);
    map<off_t,SingleByteRangeInMemEntry>::iterator itr;
    for( itr = global_byteRange_index.begin();
         itr != global_byteRange_index.end(); itr++ )
    {
        void *start = &(itr->second);
        ptr = memcpy_helper(ptr,start,centry_length);
    }
    mlog(IDX_DCOMMON, "%s: Copied %ld entries for global index of %s",
         __FUNCTION__, (long)quant,physical_path.c_str());

    // copy sf_quant of simpleFormula entries
    ptr = memcpy_helper(ptr,&sf_quant,sizeof(size_t));

    // copy simpleFormula entries
    size_t sf_length = sizeof(SimpleFormulaInMemEntry);
    map< double, SimpleFormulaInMemEntry >::iterator sitr;
    for ( sitr = global_simpleFormula_index.begin();
          sitr != global_simpleFormula_index.end(); sitr ++){
         void *start = &(sitr->second);
         ptr = memcpy_helper(ptr,start,sf_length);
    }
    mlog(IDX_DCOMMON, "%s: Copied %ld simpleFormula entries for "
          "global index of %s",__FUNCTION__, (long)sf_quant,
          physical_path.c_str());

    // put chunk paths last
    // copy the chunk paths,
    ptr = memcpy_helper(ptr,(void *)chunks.str().c_str(),chunks_length);
    mlog(IDX_DCOMMON, "%s: Copied the chunk map for global index of %s",
         __FUNCTION__, physical_path.c_str());

    assert(ptr==(char *)*buffer+*length);
    return ret;
}

size_t Index::splitEntry( SingleByteRangeInMemEntry *entry,
                          set<off_t> &splits,
                          multimap<off_t,SingleByteRangeInMemEntry> &entries)
{
    set<off_t>::iterator itr;
    size_t num_splits = 0;
    for(itr=splits.begin(); itr!=splits.end(); itr++) {
        // break it up as needed, and insert every broken off piece
        if ( entry->splittable(*itr) ) {
            /*
            ostringstream oss;
            oss << "Need to split " << endl << *entry << " at " << *itr;
            mlog(IDX_DCOMMON,"%s",oss.str().c_str());
            */
            SingleByteRangeInMemEntry trimmed = entry->split(*itr);
            entries.insert(make_pair(trimmed.logical_offset,trimmed));
            num_splits++;
        }
    }
    // insert whatever is left
    entries.insert(make_pair(entry->logical_offset,*entry));
    return num_splits;
}

void Index::findSplits(SingleByteRangeInMemEntry& e,set<off_t> &s)
{
    s.insert(e.logical_offset);
    s.insert(e.logical_offset+e.length);
}


// to deal with overlapped write records
// we split them into multiple writes where each one is either unique
// or perfectly colliding with another (i.e. same logical offset and length)
// then we keep all the unique ones and the more recent of the colliding ones
//
// adam says this is most complex code in plfs.  Here's longer explanation:
// A) we tried to insert an entry, incoming,  and discovered that it overlaps w/
// other entries already in global_byteRange_index
// the attempted insertion was either:
// 1) successful bec offset didn't collide but the range overlapped with others
// 2) error bec the offset collided with existing
// B) take the insert iterator and move it backward and forward until we find
// all entries that overlap with incoming.  As we do this, also create a set
// of all offsets, splits, at beginning and end of each entry
// C) remove those entries from global_byteRange_index and insert into temporary
// container, overlaps.
// if incoming was successfully insert originally, it's already in this range
// else we also need to explicity insert it
// D) iterate through overlaps and split each entry at split points, insert
// into yet another temporary multimap container, chunks
// all of the entries in chunks will either be:
// 1) unique, they don't overlap with any other chunk
// 2) or a perfect collision with another chunk (i.e. off_t and len are same)
// E) iterate through chunks, insert into temporary map container, winners
// on collision (i.e. insert failure) only retain entry with higher timestamp
// F) finally copy all of winners back into global_byteRange_index
int Index::handleOverlap(SingleByteRangeInMemEntry& incoming,
                      pair<map<off_t,SingleByteRangeInMemEntry>::iterator, bool>
                      &insert_ret )
{
    // all the stuff we use
    // place holders
    map<off_t,SingleByteRangeInMemEntry>::iterator first, last, cur;
    cur = first = last = insert_ret.first;
    // offsets to use to split into chunks
    set<off_t> splits;
    // all offending entries
    multimap<off_t,SingleByteRangeInMemEntry> overlaps;
    // offending entries nicely split
    multimap<off_t,SingleByteRangeInMemEntry> chunks;
    // the set to keep
    map<off_t,SingleByteRangeInMemEntry> winners;
    mss::mlog_oss oss(IDX_DCOMMON);
    // OLD: this function is easier if incoming is not already in
    // global_byteRange_index
    // NEW: this used to be true but for overlap2 it was breaking things.  we
    // wanted this here so we wouldn't insert it twice.  But now we don't insert
    // it twice so now we don't need to remove incoming here
    // find all existing entries that overlap
    // and the set of offsets on which to split
    // I feel like cur should be at most one away from the first overlap
    // but I've seen empirically that it's not, so move backwards while we
    // find overlaps, and then forwards the same
    for(first=insert_ret.first;; first--) {
        if (!first->second.overlap(incoming)) {  // went too far
            mlog(IDX_DCOMMON, "Moving first %lu forward, "
                 "no longer overlaps with incoming %lu",
                 (unsigned long)first->first,
                 (unsigned long)incoming.logical_offset);
            first++;
            break;
        }
        findSplits(first->second,splits);
        if ( first == global_byteRange_index.begin() ) {
            break;
        }
    }
    for(;
        (last!=global_byteRange_index.end())&&(last->second.overlap(incoming));
        last++)
    {
        findSplits(last->second,splits);
    }
    findSplits(incoming,splits);  // get split points from incoming as well
    // now that we've found the range of overlaps,
    // 1) put them in a temporary multimap with the incoming,
    // 2) remove them from the global index,
    // 3) then clean them up and reinsert them into global
    // insert the incoming if it wasn't already inserted into global (1)
    if ( insert_ret.second == false ) {
        overlaps.insert(make_pair(incoming.logical_offset,incoming));
    }
    overlaps.insert(first,last);    // insert the remainder (1)
    global_byteRange_index.erase(first,last); // remove from global (2)
    /*
    // spit out debug info about our temporary multimap and our split points
    oss << "Examing the following overlapped entries: " << endl;
    for(cur=overlaps.begin();cur!=overlaps.end();cur++) oss<<cur->second<< endl;
    oss << "List of split points";
    for(set<off_t>::iterator i=splits.begin();i!=splits.end();i++)oss<<" "<< *i;
    oss << endl;
    */
    // now split each entry on split points and put split entries into another
    // temporary container chunks (3)
    for(cur=overlaps.begin(); cur!=overlaps.end(); cur++) {
        splitEntry(&cur->second,splits,chunks);
    }
    // now iterate over chunks and insert into 3rd temporary container, winners
    // on collision, possibly swap depending on timestamps
    multimap<off_t,SingleByteRangeInMemEntry>::iterator chunks_itr;
    pair<map<off_t,SingleByteRangeInMemEntry>::iterator,bool> ret;
    oss << "Entries have now been split:" << endl;
    for(chunks_itr=chunks.begin(); chunks_itr!=chunks.end(); chunks_itr++) {
        oss << chunks_itr->second << endl;
        // insert all of them optimistically
        ret = winners.insert(make_pair(chunks_itr->first,chunks_itr->second));
        if ( ! ret.second ) { // collision
            // check timestamps, if one already inserted
            // is older, remove it and insert this one
            if ( ret.first->second.end_timestamp
                    < chunks_itr->second.end_timestamp ) {
                winners.erase(ret.first);
                winners.insert(make_pair(chunks_itr->first,chunks_itr->second));
            }
        }
    }
    oss << "Entries have now been trimmed:" << endl;
    for(cur=winners.begin(); cur!=winners.end(); cur++) {
        oss << cur->second;
    }
    mlog(IDX_DCOMMON, "%s",oss.str().c_str());
    // I've seen weird cases where when a file is continuously overwritten
    // slightly (like config.log), that it makes a huge mess of small little
    // chunks.  It'd be nice to compress winners before inserting into global
    // now put the winners back into the global index
    global_byteRange_index.insert(winners.begin(),winners.end());
    return 0;
}


map<off_t,SingleByteRangeInMemEntry>::iterator
Index::insertGlobalEntryHint(
    SingleByteRangeInMemEntry *g_entry ,
    map<off_t,SingleByteRangeInMemEntry>::iterator hint)
{
    return global_byteRange_index.insert(hint,
                               pair<off_t,SingleByteRangeInMemEntry>(
                                   g_entry->logical_offset,
                                   *g_entry ) );
}

pair<map<off_t,SingleByteRangeInMemEntry>::iterator,bool>
Index::insertGlobalEntry( SingleByteRangeInMemEntry *g_entry)
{
    last_offset = max( (off_t)(g_entry->logical_offset+g_entry->length),
                       last_offset );
    total_bytes += g_entry->length;
    return global_byteRange_index.insert(
               pair<off_t,SingleByteRangeInMemEntry>( g_entry->logical_offset,
                                           *g_entry ) );
}

int
Index::insertGlobal( SingleByteRangeInMemEntry *g_entry )
{
    pair<map<off_t,SingleByteRangeInMemEntry>::iterator,bool> ret;
    bool overlap  = false;
    mss::mlog_oss ioss(IDX_DAPI);
    mlog(IDX_DAPI, "Inserting offset %ld into index of %s (%d)",
         (long)g_entry->logical_offset, physical_path.c_str(),g_entry->id);
    ret = insertGlobalEntry( g_entry );
    if ( ret.second == false ) {
        ioss << "overlap1" <<endl<< *g_entry <<endl << 
                ret.first->second << endl;
        mlog(IDX_DCOMMON, "%s", ioss.str().c_str() );
        overlap  = true;
    }
    // also, need to check against prev and next for overlap
    map<off_t,SingleByteRangeInMemEntry>::iterator next, prev;
    next = ret.first;
    next++;
    prev = ret.first;
    prev--;
    if ( next != global_byteRange_index.end() &&
         g_entry->overlap( next->second ) )
    {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << "overlap2 " << endl << *g_entry << endl <<next->second;
        mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        overlap = true;
    }
    if (ret.first!=global_byteRange_index.begin() &&
        prev->second.overlap(*g_entry) )
    {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << "overlap3 " << endl << *g_entry << endl <<prev->second;
        mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        overlap = true;
    }
    if ( overlap ) {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << __FUNCTION__ << " of " << physical_path << " trying to insert "
            << "overlap at " << g_entry->logical_offset;
        mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        //handleOverlap with entry length 0 is broken
        //not exactly sure why
        if (g_entry->length != 0){
            handleOverlap( *g_entry, ret );
        }
    } else if (compress_contiguous) {
        // does it abuts with the one before it
        if (ret.first!=global_byteRange_index.begin() &&
            g_entry->follows(prev->second))
        {
            ioss << "Merging index for " << *g_entry << " and " << prev->second
                << endl;
            mlog(IDX_DCOMMON, "%s", ioss.str().c_str());
            prev->second.length += g_entry->length;
            global_byteRange_index.erase( ret.first );
        }
        /*
        // does it abuts with the one after it.  This code hasn't been tested.
        // also, not even sure this would be possible.  Even if it is logically
        // contiguous with the one after, it wouldn't be physically so.
        if ( next != global_byteRange_index.end() &&
             g_entry->abut(next->second) )
        {
            oss << "Merging index for " << *g_entry << " and " << next->second
                 << endl;
            mlog(IDX_DCOMMON, "%s", oss.str().c_str());
            g_entry->length += next->second.length;
            global_byteRange_index.erase( next );
        }
        */
    }
    return 0;
}

// just a little helper to print an error message and make sure the fh is
// closed and the data buffers released
// ret 0 or -err
int
Index::cleanupReadIndex( IOSHandle *xfh, void *maddr, off_t length, int ret,
                         const char *last_func, const char *indexfile,
                         struct plfs_backend *hback)
{
    int ret2 = 0, ret3 = 0;
    if ( ret < 0 ) {
        mlog(IDX_DRARE, "WTF.  readIndex failed during %s on %s: %s",
             last_func, indexfile, strerror( -ret ) );
    }
    if ( maddr != NULL ) {
        ret2 = xfh->ReleaseDataBuf(maddr, length);
        if ( ret2 < 0 ) {
            mlog(IDX_DRARE,
                 "WTF.  readIndex failed during release of %s (%lu): %s",
                 indexfile, (unsigned long)length, strerror(-ret2));
            ret = ret2; // set to error
        }
    }
    if ( maddr == NULL ) {
        mlog(IDX_DRARE, "get/map failed on %s: %s",indexfile,strerror(-ret));
    }
    if ( xfh != NULL ) {
        ret3 = hback->store->Close(xfh);
        if ( ret3 < 0 ) {
            mlog(IDX_DRARE,
                 "WTF. readIndex failed during close of %s: %s",
                 indexfile, strerror(-ret3) );
            ret = ret3; // set to error
        }
    }
    return(ret);
}

// returns any fh that has been stashed for a data chunk
// if an fh has not yet been stashed, it returns the initial
// value of -1
IOSHandle *
Index::getChunkFh( pid_t chunkid )
{
    return chunk_map[chunkid].fh;
}

// stashes an fh for a data chunk
// the index no longer opens them itself so that
// they might be opened in parallel when a single logical read
// spans multiple data chunks
int
Index::setChunkFh( pid_t chunkid, IOSHandle *newfh )
{
    chunk_map[chunkid].fh = newfh;
    return 0;
}

// this is a helper function to globalLookup which returns information
// identifying the physical location of some piece of data
// we found a chunk containing an offset, return necessary stuff
// this does not open the fh to the chunk however
int
Index::chunkFound( IOSHandle **xfh, off_t *chunk_off, size_t *chunk_len,
                   off_t shift, string& path,
                   struct plfs_backend **backp, pid_t *chunkid,
                   double *ts, SingleByteRangeInMemEntry *entry )
{
    ChunkFile *cf_ptr = &(chunk_map[entry->id]); // typing shortcut
    *chunk_off  = entry->physical_offset + shift;
    *chunk_len  = entry->length       - shift;
    *chunkid   = entry->id;
    *ts         = entry->end_timestamp;
    if( cf_ptr->fh == NULL ) {
        // I'm not sure why we used to open the chunk file here and
        // now we don't.  If you figure it out, pls explain it here.
        // we must have done the open elsewhere.  But where and why not here?
        // ok, I figured it out (johnbent 8/27/2011):
        // this function is a helper to globalLookup which returns information
        // about in which physical data chunk some logical data resides
        // we stash that location info here but don't do the open.  we changed
        // this when we made reads be multi-threaded.  since we postpone the
        // opens and do them in the threads we give them a better chance to
        // be parallelized.  However, it turns out that the opens are then
        // later mutex'ed so they're actually parallized.  But we've done the
        // best that we can here at least.
        mlog(IDX_DRARE, "Not opening chunkfile %s yet", cf_ptr->bpath.c_str());
    }
    mlog(IDX_DCOMMON, "Will read from chunk %s at off %ld (shift %ld)",
         cf_ptr->bpath.c_str(), (long)*chunk_off, (long)shift );
    *xfh = cf_ptr->fh;
    path = cf_ptr->bpath;
    *backp = cf_ptr->backend;
    return 0;
}

// returns the fh for the chunk and the offset within the chunk
// and the size of the chunk beyond the offset
// if the chunk does not currently have an fh, it is created here
// if the lookup finds a hole, it returns -1 for the fh and
// chunk_len for the size of the hole beyond the logical offset
// returns 0 or -err
int Index::globalLookup( IOSHandle **xfh, off_t *chunk_off, size_t *chunk_len,
                         string& path, struct plfs_backend **backp,
                         bool *hole, pid_t *chunkid,
                         off_t logical )
{
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this << " using index.";
    mlog(IDX_DAPI, "%s", os.str().c_str() );
    double ts = 0;
    *hole = false;
    *chunkid = (pid_t)-1;
    //mlog(IDX_DCOMMON, "Look up %ld in %s",
    //        (long)logical, physical_path.c_str() );
    
    // beyond EOF
    if ( logical > last_offset ){
        *chunk_len = 0;
        return 0;
    }

    // lookup in ByteRange index first
    // no ByteRange index entries, nothing to see here, move along
    if ( global_byteRange_index.size() == 0 ) {
        // set length to EOF
        *xfh = NULL;
        *chunk_len = last_offset;
        *hole = true;
    } else {
        SingleByteRangeInMemEntry entry, previous;
        CON_ENTRY_ITR itr;
        CON_ENTRY_ITR prev = (CON_ENTRY_ITR)NULL;
        // Finds the first element whose key is not less than k.
        // four possibilities:
        // 1) direct hit
        // 2) within a chunk
        // 3) off the end of global_byteRange_index
        // 4) in a hole
        
        itr = global_byteRange_index.lower_bound( logical );
        // back up if we went off the end
        if ( itr == global_byteRange_index.end() ) {
            // this is safe because we know the size is >= 1
            // so the worst that can happen is we back up to begin()
            itr--;
        }
        if ( itr != global_byteRange_index.begin() ) {
            prev = itr;
            prev--;
        }
        entry = itr->second;
        if ( prev != (CON_ENTRY_ITR)NULL ){
            previous = prev->second;
        }
        //ostringstream oss;
        //oss << "Considering whether chunk " << entry
        //     << " contains " << logical;
        //mlog(IDX_DCOMMON, "%s\n", oss.str().c_str() );
        if ( entry.contains( logical ) ) {
            // case 1 or 2
            //ostringstream oss;
            //oss << "FOUND(1): " << entry << " contains " << logical;
            //mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
            chunkFound( xfh, chunk_off, chunk_len,
                        logical - entry.logical_offset, path,
                        backp, chunkid, &ts, &entry );
        } else if ( prev != (CON_ENTRY_ITR)NULL &&
                    previous.contains( logical )) {
            // case 1 or 2
            //ostringstream oss;
            //oss << "FOUND(2): "<< previous << " contains "
            //    << logical << endl;
            //mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
            chunkFound( xfh, chunk_off, chunk_len,
                        logical - previous.logical_offset, path,
                        backp, chunkid, &ts, &previous );
        } else if ( logical < entry.logical_offset ) {
            // now it's before entry and in a hole
            // case 4: within a hole
            off_t remaining_hole_size = entry.logical_offset - logical;
            *chunk_len = remaining_hole_size;
            *hole = true;
            ts = 0;
            mss::mlog_oss oss(IDX_DCOMMON);
            oss << "FOUND(4): " << logical << " is in a hole";
            mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        } else {
            // after entry and off the end of ByteRange index
            // case 3: off the end of the global index
            *chunk_len = last_offset - logical;
            *hole = true;
            ts = 0;
            mss::mlog_oss oss(IDX_DCOMMON);
            oss << "FOUND(3): " << logical
                << " is off the end of global_byteRange_index";
            mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        }
    }

    // lookup in SimpleFormula index
    // Here we have got <chunk_off, chunk_len, path, chunkid, ts, path, hole>
    // tuple from ByteRange Index
    // if hole is set, then chunk_off, path, chunkid is not initilized
    map< double, SimpleFormulaInMemEntry >::reverse_iterator ritr;
    off_t l_off, p_off;
    size_t len;
    pid_t pid;
    bool found = false;
    double metalinkTime = 0;

    if (global_simpleFormula_index.size() == 0) {
        mlog(IDX_INFO, "%s: simpleFormulaIndex size 0", __FUNCTION__);
        return 0;
    }
    for(ritr = global_simpleFormula_index.rbegin();
        ritr != global_simpleFormula_index.rend();
        ritr ++ ){
        if ( (*ritr).first > ts ) {
            if((*ritr).second.overlap(logical,*chunk_len,l_off,p_off,len,pid)){
                if ( l_off > logical ){
                    *chunk_len = l_off - logical;
                    continue;
                }
                found        = true;
                *chunkid    = (*ritr).second.id + pid;
                ts           = (*ritr).second.formulaTime;
                metalinkTime = (*ritr).second.metalinkTime;
                mlog(IDX_DCOMMON, "%s find one overlapped formulaEntry "
                      "at off %ld len %ld", __FUNCTION__,
                      (long)l_off, (long)len);
                break;
            }
        } else {
            mlog(IDX_INFO, "%s: Can't find overlop at off %ld len %ld",
                  __FUNCTION__, (long)logical, (long)chunk_len);
            // following simpleFormulaEntries have smaller timestamp
            break;
        }
    }
    if (found){
        assert(l_off == logical);
        if(*hole==true) *hole=false;
        ChunkFile *cf_ptr = &(chunk_map[*chunkid]); // typing shortcut
        *xfh         = cf_ptr->fh;
        *chunk_off   = p_off;
        *chunk_len   = len;
        struct plfs_backend *backout;
        string bpath;
        int rv;
        if (cf_ptr->bpath.find(FORMULADATAPREFIX) == string::npos){
            // The data file locates in shadow container or canonical container
            string mlink = Container::getMetalinkPath(cf_ptr->bpath,pid,
                                                      metalinkTime);
            string resolved;
            plfs_backend *metaback;
            // find out the metalink backend
            rv = plfs_phys_backlookup(mlink.c_str(), NULL, &backout, &bpath);
            if( rv != 0 ){
               /* this shouldn't ever happen */
               mlog(IDX_CRIT, "globalLookup: %s backlookup failed", path.c_str());
               return(rv);
            }
            if( Container::resolveMetalink(mlink,backout,NULL,resolved,&metaback) == 0 ){
                ostringstream tmp;
                tmp.setf(ios::fixed,ios::floatfield);
                tmp << resolved << "/" << FORMULADATAPREFIX << ts << "." << pid;
                path = tmp.str();
            }else{
                path = Container::getDataPath(cf_ptr->bpath,Util::hostname(),
                                              pid,ts,SIMPLEFORMULA);
            }
            // save the full path to the data file
            cf_ptr->bpath = path;
            // save the backend info
            rv = plfs_phys_backlookup(path.c_str(), NULL, &backout, &bpath);
            if( rv != 0 ){
               /* this shouldn't ever happen */
               mlog(IDX_CRIT, "globalLookup: %s backlookup failed", path.c_str());
               return(rv);
            }
            cf_ptr->backend = backout;
        }else{
            path = cf_ptr->bpath;
        }
        *backp = cf_ptr->backend;
    }

    return 0;
}

// we're just estimating the area of these stl containers which ignores overhead
size_t
Index::memoryFootprintMBs()
{
    double KBs = 0;
    KBs += (byteRangeIndex.size() * sizeof(SingleByteRangeEntry))/1024.0;
    KBs += (global_byteRange_index.size()*
           (sizeof(off_t)+sizeof(SingleByteRangeInMemEntry)))/1024.0;
    KBs += (chunk_map.size() * sizeof(ChunkFile))/1024.0;
    KBs += (physical_offsets.size() * (sizeof(pid_t)+sizeof(off_t)))/1024.0;
    KBs += (simpleFormulaIndex.size() * sizeof(SimpleFormulaEntry))/1024.0;
    KBs += (global_simpleFormula_index.size() *
           (sizeof(double)+sizeof(SimpleFormulaInMemEntry)))/1024.0;
    mlog(IDX_DCOMMON, "%s: Index contains %ld ByteRange entries, "
          "%ld simpleFormula entries",__FUNCTION__,
          global_byteRange_index.size(), global_simpleFormula_index.size());
    return size_t(KBs/1024);
}

void
Index::addWrite( off_t offset, size_t length, pid_t pid,
                 double begin_timestamp, double end_timestamp )
{
    Metadata::addWrite( offset, length );
    // check whether incoming abuts with last and we want to compress
    if ( compress_contiguous && !byteRangeIndex.empty() &&
            byteRangeIndex.back().id == pid  &&
            byteRangeIndex.back().logical_offset +
            (off_t)byteRangeIndex.back().length == offset) {
        mlog(IDX_DCOMMON, "Merged new write with last at offset %ld."
             " New length is %d.\n",
             (long)byteRangeIndex.back().logical_offset,
             (int)byteRangeIndex.back().length );
        byteRangeIndex.back().end_timestamp = end_timestamp;
        byteRangeIndex.back().length += length;
        physical_offsets[pid] += length;
    } else {
        // create a new index entry for this write
        SingleByteRangeEntry entry;
        // suppress valgrind complaint
        //memset(&entry,0,sizeof(SingleByteRangeEntry));
        entry.logical_offset = offset;
        entry.length         = length;
        entry.id             = pid;
        entry.begin_timestamp = begin_timestamp;
        // valgrind complains about this line as well:
        // Address 0x97373bc is 20 bytes inside a block of size 40 alloc'd
        entry.end_timestamp   = end_timestamp;
        // lookup the physical offset
        map<pid_t,off_t>::iterator itr = physical_offsets.find(pid);
        if ( itr == physical_offsets.end() ) {
            physical_offsets[pid] = 0;
        }
        entry.physical_offset = physical_offsets[pid];
        physical_offsets[pid] += length;
        byteRangeIndex.push_back( entry );
        // Needed for our index stream function
        // It seems that we can store this pid for the global entry
    }
    if (buffering && !buffer_filled) {
        // ok this code is confusing
        // there are two types of indexes that we create in this same class:
        // SingleByteRangeEntry are used for writes 
        // (specific to a hostdir (subdir))
        // SingleByteRangeInMemEntry are used for reading
        // (global across container)
        // this buffering code is currently only used in ad_plfs
        // we buffer the index so we can send it on close to rank 0 who
        // collects from everyone, compresses, and writes a global index.
        // What's confusing is that in order to buffer, we create a read type
        // index.  This is because we have code to serialize a read index into
        // a byte stream and we don't have code to serialize a write index.
        // restore the original physical offset before we incremented above
        off_t poff = physical_offsets[pid] - length;
        // create a container entry from the SingleByteRangeEntry
        SingleByteRangeInMemEntry c_entry;
        c_entry.logical_offset    = offset;
        c_entry.length            = length;
        c_entry.id                = pid;
        c_entry.original_chunk    = pid;
        c_entry.physical_offset   = poff;
        c_entry.begin_timestamp   = begin_timestamp;
        c_entry.end_timestamp     = end_timestamp;
        insertGlobal(&c_entry);  // push it into the read index structure
        // Make sure we have the chunk path
        // chunk map only used for read index.  We need to maintain it here
        // so that rank 0 can collect all the local chunk maps to create a
        // global one
        if(chunk_map.size()==0) {
            ChunkFile cf;
            cf.fh = NULL;
            IOSHandle *ifh = current_fh[BYTERANGE];
            string path = index_paths[ifh];
            cf.bpath = Container::chunkPathFromIndexPath(path,pid);
            // No good we need the Index Path please be stashed somewhere
            mlog(IDX_DCOMMON, "Use chunk path from index path: %s",
                 cf.bpath.c_str());
            chunk_map.push_back( cf );
        }
    }
}

// this function add a simple formula write, before adding a new
// simple formula entry, firstly check if this write extends 
// the head of simple formula entry list.
//
// return code:
// bool extended is set to true when this entry extends head of
// simple formula entry list.
// Or it will be set to false when this entry is inserted to head of
// simple formula entry list.
// the simpleFormula entry is listed by time-ascending order.
int
Index::addSimpleFormulaWrite(int nw, int size, plfs_io_pattern strided,
                             off_t start,off_t end, off_t last,
                             double ts, bool& extended)
{
    SimpleFormulaEntry entry;
    entry.nw = nw;
    entry.write_size = size;
    entry.logical_start_offset = start;
    entry.logical_end_offset = end;
    entry.last_offset = last;
    entry.strided = strided;
    entry.formulaTime = ts;
    entry.numWrites = 1;

    // check if this entry grows the head
    if ( simpleFormulaIndex.size() != 0 &&
         entry.follows( simpleFormulaIndex.back() ) )
    {
        simpleFormulaIndex.back().logical_end_offset = end;
        simpleFormulaIndex.back().last_offset = last;
        simpleFormulaIndex.back().numWrites ++;
        extended = true;
        return 0;
    }
    simpleFormulaIndex.push_back(entry);
    extended = false;
    return 0;
}

int
Index::updateSimpleFormula(double begin, double end)
{
    // get latest simpleFormulaEntry
    simpleFormulaIndex.back().begin_timestamp = begin;
    simpleFormulaIndex.back().end_timestamp = end;
    return 0;
}

void
Index::truncate( off_t offset )
{
    map<off_t,SingleByteRangeInMemEntry>::iterator itr, prev;
    map< double, SimpleFormulaInMemEntry >::iterator sitr,tmp;
    bool first = false;
    // in the case that truncate a zero length logical file.
    if ( global_simpleFormula_index.size() > 0){
        // it's a simpleFormula index file
        for(sitr = global_simpleFormula_index.begin();
            sitr != global_simpleFormula_index.end();){
            if( offset < (*sitr).second.logical_start_offset ){
                // the whole formulaEntry should be abandoned
                global_simpleFormula_index.erase(sitr++);
                continue;
            } else if (offset < (*sitr).second.last_offset){
                (*sitr).second.last_offset = offset;
            }
            sitr++;
        }
    }
    if ( global_byteRange_index.size() == 0 ) {
        mlog(IDX_DAPI, "%s in %p, global_byteRange_index.size == 0.\n",
             __FUNCTION__, this);
        return;
    }
    mlog(IDX_DAPI, "Before %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)global_byteRange_index.size());
    // Finds the first element whose offset >= offset.
    itr = global_byteRange_index.lower_bound( offset );
    if ( itr == global_byteRange_index.begin() ) {
        first = true;
    }
    prev = itr;
    prev--;
    // remove everything whose offset >= offset
    global_byteRange_index.erase( itr, global_byteRange_index.end() );
    // check whether the previous needs to be
    // internally truncated
    if ( ! first ) {
        if ((off_t)(prev->second.logical_offset + prev->second.length)
                > offset) {
            // say entry is 5.5 that means that ten
            // is a valid offset, so truncate to 7
            // would mean the new length would be 3
            prev->second.length = offset - prev->second.logical_offset ;//+ 1 ?
            mlog(IDX_DCOMMON, "%s Modified a global index record to length %u",
                 __FUNCTION__, (uint)prev->second.length);
            if (prev->second.length==0) {
                mlog(IDX_DCOMMON, "Just truncated index entry to 0 length" );
            }
        }
    }
    mlog(IDX_DAPI, "After %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)global_byteRange_index.size());
}

// operates on a host entry which is not sorted
void
Index::truncateHostIndex( off_t offset )
{
    last_offset = offset;
    vector< SingleByteRangeEntry > new_entries;
    vector< SingleByteRangeEntry >::iterator itr;
    for( itr = byteRangeIndex.begin(); itr != byteRangeIndex.end(); itr++ ) {
        SingleByteRangeEntry entry = *itr;
        if ( entry.logical_offset < offset ) {
            // adjust if necessary and save this one
            if ( (off_t)(entry.logical_offset + entry.length) > offset ) {
                entry.length = offset - entry.logical_offset + 1;
            }
            new_entries.push_back( entry );
        }
    }
    byteRangeIndex = new_entries;
}

void
Index::extend(off_t offset, pid_t p, double ts)
{
    addWrite( offset, 0, p, ts, ts );
    // if there is no simpleFormula in memory, we are safe to just
    // add a new ByteRange entry to represent this truncate
    if ( simpleFormulaIndex.size() != 0 ) {
        // in case we have simpleFormula index in memory, need to
        // push a specialized simpleFormulaEntry in vector to stop
        // possible following write grow head of simpleFormula vecotr
        // this is for data correctness
        bool unused;
        addSimpleFormulaWrite(0,0,RANDOM,offset,offset,offset,ts,unused);
    }
}
// ok, someone is truncating a file, so we reread a local index,
// created a partial global index, and truncated that global
// index, so now we need to dump the modified global index into
// a new local index
// XXX: fh's backend is stored in Index::iback, use it to flush()
int
Index::rewriteIndex( IOSHandle *rfh )
{
    int ret;
    enablePidxFlush();
    string indexfile = index_paths[rfh];
    map< double, SimpleFormulaInMemEntry >::iterator sitr;
    if (indexfile.find(FORMULAINDEXPREFIX) != string::npos){
        for (sitr = global_simpleFormula_index.begin();
             sitr != global_simpleFormula_index.end(); sitr ++){
           SimpleFormulaEntry entry;
           entry.nw                   = sitr->second.nw;
           entry.write_size           = sitr->second.write_size;
           entry.numWrites            = sitr->second.numWrites;
           entry.logical_start_offset = sitr->second.logical_start_offset;
           entry.logical_end_offset   = sitr->second.logical_end_offset;
           entry.last_offset          = sitr->second.last_offset;
           entry.formulaTime          = sitr->second.formulaTime;
           entry.strided              = sitr->second.strided;
           entry.begin_timestamp      = sitr->second.begin_timestamp;
           entry.end_timestamp        = sitr->second.end_timestamp;

           simpleFormulaIndex.push_back(entry);
        }
        ret = flush();
        disablePidxFlush();
        return ret;
    }
    map<off_t,SingleByteRangeInMemEntry>::iterator itr;
    map<double,SingleByteRangeInMemEntry> global_byteRange_index_timesort;
    map<double,SingleByteRangeInMemEntry>::iterator itrd;
    // so this is confusing.  before we dump the global_byteRange_index back into
    // a physical index entry, we have to resort it by timestamp instead
    // of leaving it sorted by offset.
    // this is because we have a small optimization in that we don't
    // actually write the physical offsets in the physical index entries.
    // we don't need to since the writes are log-structured so the order
    // of the index entries matches the order of the writes to the data
    // chunk.  Therefore when we read in the index entries, we know that
    // the first one to a physical data dropping is to the 0 offset at the
    // physical data dropping and the next one is follows that, etc.
    //
    // update, we know include physical offsets in the index entries so
    // we don't have to order them by timestamps anymore.  However, I'm
    // reluctant to change this code so near a release date and it doesn't
    // hurt them to be sorted so just leave this for now even though it
    // is technically unnecessary
    for( itr = global_byteRange_index.begin();
         itr != global_byteRange_index.end(); itr++ )
    {
        global_byteRange_index_timesort.insert(
            make_pair(itr->second.begin_timestamp,itr->second));
    }
    for( itrd = global_byteRange_index_timesort.begin(); itrd !=
            global_byteRange_index_timesort.end(); itrd++ )
    {
        double begin_timestamp = 0, end_timestamp = 0;
        begin_timestamp = itrd->second.begin_timestamp;
        end_timestamp   = itrd->second.end_timestamp;
        addWrite( itrd->second.logical_offset,itrd->second.length,
                  itrd->second.original_chunk, begin_timestamp, end_timestamp );
        /*
        ostringstream os;
        os << __FUNCTION__ << " added : " << itr->second;
        mlog(IDX_DCOMMON, "%s", os.str().c_str() );
        */
    }
    ret = flush();
    disablePidxFlush();
    return ret;
}
