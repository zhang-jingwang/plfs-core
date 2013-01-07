/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_write.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 */

#include "ad_plfs.h"
#include "adio_extern.h"

#ifdef ROMIO_CRAY
#include "../ad_cray/ad_cray.h"
#endif /* ROMIO_CRAY */

// calculate number of elements in PatternInfo structure
#define ELMS_NUM (sizeof(PatternInfo)/sizeof(double))

extern int enable_pidx;

// be careful of the byte alignment so ELEM_NUM can always
// get the correct number of elements in it,
// may waste some bytes though
typedef struct {
    ADIO_Offset start; // ADIO_Offset is a long long
    ADIO_Offset end;
    double      timestamp;
    int         strided;
}__attribute__((aligned(8))) PatternInfo;

// buf stores all ranks <offset,length> pair
// return code 1 means a pattern has been extracted while -1 not
int pattern_detector(void *buf,int size, int len, plfs_io_pattern *strided)
{
    plfs_debug( "pattern_detector: size %d, len %d\n", size, len);
    int index, off_index, ret = -1;
    long block_size;
    long *pointer = (long *)buf;

    // check if this is strided pattern
    for( index = 0; index < size-1; index ++){
        // skip length in buf
        off_index = index*2;
        if ( *(pointer+off_index) + len != *(pointer+off_index+2) ){
            break;
        }
    }
    if ( index == size - 1 ){
        // if we got here, means we found a strided IO
        *strided = STRIDED;
        ret = 1;
        plfs_debug( "pattern_detector: found a STRIDED IO\n" );
    } else if ( index == 0 ){
        // check if this is segmented pattern
        // skip the first length in buf
        block_size = *(pointer+2) - *pointer;
        if (block_size < len) {
           goto out;
        }
        for ( index = 0; index < size - 1; index ++){
            // skip length in buf
            off_index = index*2;
            if (*(pointer+off_index) + block_size !=
                  *(pointer+off_index+2)){
                goto out;
            }
        }
        // if we got here, means we found a segmented IO
        *strided = SEGMENTED;
        ret = 1;
        plfs_debug( "pattern_detector: found a SEGMENTED IO" );
    }
out:
    plfs_debug( "pattern_detector: strided %d ret %d \n", *strided, ret);
    return ret;
}

// Collect IO pattern for every write. A pattern should have more one
// procs involved, have same write size and can be represented by a formula
// Index compression is only used for CONTAINER mode
// Do quick check for extracting a pattern
// return 1 means it passed the quick check
// return 0 means it failed the quick check
int canBePattern(int comm_size, const char *path)
{
    if (comm_size == 1 ||
        plfs_get_filetype(path) != CONTAINER )
    {
        // not a pattern
        return 0;
    }
    return 1;
}

void ADIOI_PLFS_WriteContig(ADIO_File fd, void *buf, int count,
                            MPI_Datatype datatype, int file_ptr_type,
                            ADIO_Offset offset, ADIO_Status *status,
                            int *error_code)
{
    /* --BEGIN CRAY MODIFICATION-- */
    int err=-1, datatype_size, rank, comm_size, pattern = 0;
    ADIO_Offset len;
    /* --END CRAY MODIFICATION-- */
    ADIO_Offset myoff, lastoff;
    plfs_io_pattern strided;
    static char myname[] = "ADIOI_PLFS_WRITECONTIG";
    long *sbuf = NULL, *rbuf = NULL;
#ifdef ROMIO_CRAY
MPIIO_TIMER_START(WSYSIO);
#endif /* ROMIO_CRAY */
    MPI_Type_size(datatype, &datatype_size);
    /* --BEGIN CRAY MODIFICATION-- */
    len = (ADIO_Offset)datatype_size * (ADIO_Offset)count;
    /* --END CRAY MODIFICATION-- */
    MPI_Comm_rank( fd->comm, &rank );
    MPI_Comm_size(fd->comm,&comm_size);
    // for the romio/test/large_file we always get an offset of 0
    // maybe we need to increment fd->fp_ind ourselves?
    if (file_ptr_type == ADIO_EXPLICIT_OFFSET) {
        myoff = offset;
    } else {
        // for ADIO_INDIVIDUAL
        myoff = fd->fp_ind;
    }
    plfs_debug( "%s: offset %ld len %ld rank %d\n",
                myname, (long)myoff, (long)len, rank );

    // formula quick check
    if( enable_pidx == 1 && canBePattern(comm_size, fd->filename) == 1 ){
        // rank 0 to collect write pattern
        // and malloc buf for <offset, length> pair
        if ( rank == 0 ){
            // malloc buf to store all ranks' <offset,length>
            rbuf = (long *)malloc(comm_size*sizeof(MPI_LONG)*2);
            malloc_check(rbuf,rank);
            memset(rbuf,'0',comm_size*sizeof(MPI_LONG)*2);
        }
        // all rank malloc sbuf for its own <offset,length> pair
        sbuf = (long*)malloc(sizeof(MPI_LONG)*2);
        malloc_check(sbuf,rank);
        sbuf[0] = myoff;
        sbuf[1] = len;

        // Gather <offset, len> pair in one call
        MPI_Gather(sbuf, 2, MPI_LONG, rbuf, 2, MPI_LONG, 0, fd->comm);
        // Now, rbuf should be filled with <O0,L0,O1,L1,O2,L2...>

        if ( rank == 0 ) {
            int current;
            for(current = 0; current != comm_size; current ++){
                // skip offset in rbuf
                if ( len != *(rbuf+(current*2+1)) ){
                    pattern = -1;
                    break;
                }
            }
            // for now, all procs are with same write size
            if ( pattern == 0 && len != 0) {
                pattern = pattern_detector(rbuf,comm_size,len,&strided);
            }
            lastoff = *(rbuf+(comm_size-1)*2);
            free(rbuf);
        }
        free(sbuf);

        MPI_Bcast(&pattern,1,MPI_INT,0,fd->comm);
    }

    plfs_debug( "%s: rank %d, pattern %d \n", myname, rank, pattern);

    if ( pattern == 1 ){
        PatternInfo pInfo;
        if (rank == 0){
            pInfo.start = myoff;
            pInfo.end = lastoff + len;
            pInfo.timestamp = plfs_wtime();
            pInfo.strided = strided;
        }

        // create a detrived datatype, so we can just send all info in
        // one message instead of four

        MPI_Datatype MPI_pInfo_t;
        int blocklen[ELMS_NUM] = {1,1,1,1};
        MPI_Datatype type[ELMS_NUM] = {MPI_LONG, MPI_LONG, MPI_DOUBLE, MPI_INT};
        MPI_Aint disp[ELMS_NUM];
        MPI_Aint PatternInfo_addr;
        MPI_Aint Pattern_start_addr;
        MPI_Aint Pattern_end_addr;
        MPI_Aint Pattern_ts_addr;
        MPI_Aint Pattern_strided_addr;

        MPI_Get_address(&pInfo, &PatternInfo_addr);
        MPI_Get_address(&pInfo.start, &Pattern_start_addr);
        MPI_Get_address(&pInfo.end, &Pattern_end_addr);
        MPI_Get_address(&pInfo.timestamp, &Pattern_ts_addr);
        MPI_Get_address(&pInfo.strided, &Pattern_strided_addr);
        disp[0] = Pattern_start_addr - PatternInfo_addr;
        disp[1] = Pattern_end_addr - PatternInfo_addr;
        disp[2] = Pattern_ts_addr - PatternInfo_addr;
        disp[3] = Pattern_strided_addr - PatternInfo_addr;

        MPI_Type_create_struct(ELMS_NUM, blocklen, disp, type, &MPI_pInfo_t);
        MPI_Type_commit(&MPI_pInfo_t);

        MPI_Bcast(&pInfo,1,MPI_pInfo_t,0,fd->comm);

        Plfs_write_opt write_opt;
        write_opt.pattern_type                            = pInfo.strided;
        write_opt.writeArgs.patternArgs.pid               = rank;
        write_opt.writeArgs.patternArgs.number_of_writers = comm_size;
        write_opt.writeArgs.patternArgs.size              = len;
        write_opt.writeArgs.patternArgs.start             = pInfo.start;
        write_opt.writeArgs.patternArgs.end               = pInfo.end;
        write_opt.writeArgs.patternArgs.timestamp         = pInfo.timestamp;
        write_opt.writeArgs.patternArgs.strided           = pInfo.strided;
        if (rank == 0){
            write_opt.writeArgs.patternArgs.write_index = 1;
        }else{
            write_opt.writeArgs.patternArgs.write_index = 0;
        }

        MPI_Type_free(&MPI_pInfo_t);

        err = plfs_write(fd->fs_ptr, buf, len, myoff, rank, &write_opt);
    } else {
        err = plfs_write( fd->fs_ptr, buf, len, myoff, rank, NULL );
    }

#ifdef HAVE_STATUS_SET_BYTES
    if (err >= 0 ) {
        MPIR_Status_set_bytes(status, datatype, err);
    }
#endif
    if (err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
    } else {
        if (file_ptr_type == ADIO_INDIVIDUAL) {
            fd->fp_ind += err;
        }
        *error_code = MPI_SUCCESS;
    }
#ifdef ROMIO_CRAY
MPIIO_TIMER_END(WSYSIO);
#endif /* ROMIO_CRAY */
}

