#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"

// THREAD SAFETY
// we use a mutex when writers are added or removed
// - the data_mux protects the fhs map
// we do lookups into the fhs map on writes
// but we assume that either FUSE is using us and it won't remove any writers
// until the release which means that everyone has done a close
// or that ADIO is using us and there won't be concurrency

// if we get multiple writers, we create an index mutex to protect it

// Each writeType has its own naming conventions for data files.
// SINGLE_HOST_WRITE is used for tranditional byteRange writing.
// SIMPLE_FORMULA_WRITE is used for ADIO patterned write.
// SIMPLE_FORMULA_WITHOUT_INDEX is similar to SIMPLE_FORMULA_WRITE except
// not writing index file, e.g. for ranks other than zero in MPI job
enum WriteType {
    SINGLE_HOST_WRITE,
    SIMPLE_FORMULA_WRITE,
    SIMPLE_FORMULA_WITHOUT_INDEX,
};

// Every proc may own multiple data files, one ByteRange data file and
// multiple SimpleFormula data files.
struct
OpenFh {
    IOSHandle *sh_fh;           // ByteRange data file fh
    vector< IOSHandle* > sf_fhs;// SimpleFormula data files fh
                                // one proc may open multiple SimpleFormula
                                // data files, push back and stash them here
                                // the fh at back() is the one currently
                                // being used
    int writers;
};

class WriteFile : public Metadata
{
    public:
        WriteFile(string, string, string, mode_t, size_t index_buffer_mbs,
                  struct plfs_backend *);
        ~WriteFile();

        int openIndex( pid_t, WriteType );
        int closeIndex();
        IOSHandle* openNewDataFile( pid_t pid , WriteType wType);

        int addWriter( pid_t, bool child, WriteType );
        int removeWriter( pid_t );
        size_t numWriters();
        size_t maxWriters() {
            return max_writers;
        }

        int truncate( off_t offset );
        int extend( off_t offset );

        ssize_t write( const char *, size_t, off_t, pid_t, WriteType );

        int sync( );
        int sync( pid_t pid );

        void setContainerPath(string path);
        void setSubdirPath (string path, struct plfs_backend *wrback);

        // get current operating data file fh
        IOSHandle* whichFh ( OpenFh *ofh, WriteType wType );
        int restoreFhs(bool droppings_were_truncd);
        Index *getIndex() {
            return index;
        }

        double createTime() {
            return createtime;
        }

        void setFormulaTime(double ts){
            formulaTime = ts;
        }
        void setMetalinkTime(double ts){
            metalinkTime = ts;
        }

    private:
        IOSHandle *openIndexFile( string path, string host, pid_t, mode_t,
                                  string *index_path,WriteType wType,int &ret);
        IOSHandle *openDataFile(string path,string host,pid_t,mode_t,
                                WriteType,int &ret );
        IOSHandle *openFile( string, mode_t mode, int &ret );
        int Close( );
        int closeFh( IOSHandle *fh );
        struct OpenFh *getFh( pid_t pid );
        int setOpenFh(OpenFh *ofh, IOSHandle *, WriteType wType);
        IOSHandle *restore_helper(IOSHandle *, string *path);

        string container_path;
        string subdir_path;
        string logical_path;
        struct plfs_backend *subdirback;
        string hostname;
        map< pid_t, OpenFh  > fhs;
        // need to remember fh paths to restore
        map< IOSHandle *, string > paths;
        pthread_mutex_t    index_mux;  // to use the shared index
        pthread_mutex_t    data_mux;   // to access our map of fhs
        bool has_been_renamed; // use this to guard against a truncate following
        // a rename
        size_t index_buffer_mbs;
        Index *index;
        mode_t mode;
        double createtime;
        double formulaTime;  // used for MPIIO only,
                             // keeps rank0's formula creation time
        double metalinkTime; // use this timestamp to create metalink
        size_t max_writers;
        // Keeps track of writes for flush of index
        int write_count;
};

#define WF_FH_ITR vector< IOSHandle* >::iterator

#endif
