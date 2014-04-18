#include "plfsunit.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

CPPUNIT_TEST_SUITE_REGISTRATION(PlfsUnit);
CPPUNIT_TEST_SUITE_REGISTRATION(PlfsFileUnit);

extern string plfsmountpoint;

#define PLFSUNIT_DEFAULT_DIR_MODE 0777

void
PlfsUnit::setUp() {
    pid = getpid();
    uid = getuid();
    gid = getgid();
    mountpoint = plfsmountpoint;
    return ;
}

void
PlfsUnit::tearDown() {
    return ;
}

void
PlfsUnit::createTest() {
    plfs_error_t ret;
    string path = mountpoint + "/createtest1";
    const char *pathname1 = path.c_str();
    string path2 = mountpoint + "/createtest2";
    const char *pathname2 = path2.c_str();
    ret = plfs_create(pathname1, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_create(pathname2, 0666, 0, 0);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    // ret = plfs_create(pathname1, 0666, O_EXCL, pid);
    // CPPUNIT_ASSERT_EQUAL(0, (int)ret); // O_EXCL is not supported.
    // ret = plfs_create(pathname1, 0666, O_EXCL, 0);
    // CPPUNIT_ASSERT_EQUAL(0, (int)ret); // O_EXCL is not supported.
    ret = plfs_unlink(pathname1);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_access(pathname1, F_OK);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
    ret = plfs_access(pathname2, F_OK);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
}

int ref_count;

void
PlfsUnit::openCloseTest() {
    plfs_error_t ret;
    string path = mountpoint + "/openclosetest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/nonexist";
    const char *nonexist = path2.c_str();
    Plfs_fd *fd = NULL;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_open(&fd, pathname, 0, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_open(&fd, pathname, 0, 0, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(1, ref_count);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, ref_count);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_access(pathname, F_OK);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);

    fd = NULL;
    ret = plfs_open(&fd, nonexist, 0, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
    CPPUNIT_ASSERT(fd == NULL);
    ret = plfs_open(&fd, pathname, O_CREAT, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_access(pathname, F_OK);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::readWriteTest() {
    string path = mountpoint + "/readwrite1";
    const char *pathname = path.c_str();
    Plfs_fd *fd = NULL;
    plfs_error_t ret;
    ssize_t written;

#ifdef NEGATIVE_TEST_CASES
    ret = plfs_open(&fd, pathname, O_CREAT | O_RDONLY, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_write(fd, "HELLO WORLD.", 13, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(PLFS_EBADF, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    fd = NULL;
#endif
    ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(fd);
    for (int i = 0; i < 10; i++) {
	char rbuf[13];
	for (pid_t fpid = 0; fpid < 10; fpid ++) {
	    off_t offset=rand();
	    ret = plfs_write(fd, "HELLO WORLD.", 13, offset, fpid, &written);
	    CPPUNIT_ASSERT_EQUAL(13, (int)written);
	    ret = plfs_sync(fd);
	    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	    ret = plfs_read(fd, rbuf, 13, offset, &written);
            CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	    CPPUNIT_ASSERT_EQUAL(13, (int)written);
	    CPPUNIT_ASSERT(strcmp(rbuf, "HELLO WORLD.") == 0);
	}
    }
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    fd = NULL;
    for (int i = 0; i < 10; i++) {
	char rbuf[13];
	for (pid_t fpid = 0; fpid < 10; fpid ++) {
	    off_t offset=rand();
            ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, fpid, 0666, NULL);
            CPPUNIT_ASSERT_EQUAL(0, (int)ret);
            CPPUNIT_ASSERT(fd);
	    ret = plfs_write(fd, "HELLO WORLD.", 13, offset, fpid, &written);
	    CPPUNIT_ASSERT_EQUAL(13, (int)written);
	    ret = plfs_sync(fd);
	    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	    ret = plfs_read(fd, rbuf, 13, offset, &written);
	    CPPUNIT_ASSERT_EQUAL(13, (int)written);
	    CPPUNIT_ASSERT(strcmp(rbuf, "HELLO WORLD.") == 0);
            ret = plfs_close(fd, fpid, uid, 0, NULL, &ref_count);
            CPPUNIT_ASSERT_EQUAL(0, (int)ret);
            fd = NULL;
	}
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::chmodTest() {
    string path = mountpoint + "/chmodetest1";
    const char *pathname = path.c_str();
    plfs_error_t ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (mode_t mode = 0; mode<01000; mode++) {
        mode_t result;
        ret = plfs_chmod(pathname, mode);
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        ret = plfs_mode(pathname, &result);
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        mode_t expected_mode = mode;
        if (plfs_get_filetype(pathname) == CONTAINER) {
            if (mode & S_IRGRP || mode & S_IWGRP) expected_mode |= S_IXGRP;
            if (mode & S_IROTH || mode & S_IWOTH) expected_mode |= S_IXOTH;
            expected_mode |= S_IRUSR | S_IXUSR | S_IWUSR;
        }
        CPPUNIT_ASSERT_EQUAL(expected_mode, (mode_t)(result & 0777));
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::chmodDirTest() {
    string path = mountpoint + "/chmoddirtest1";
    const char *pathname = path.c_str();
    plfs_error_t ret;

    ret = plfs_mkdir(pathname, PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (mode_t mode = 0; mode<01000; mode++) {
        mode_t result;
        ret = plfs_chmod(pathname, mode);
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        ret = plfs_mode(pathname, &result);
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        CPPUNIT_ASSERT_EQUAL(mode, (mode_t)(result & 0777));
    }
    ret = plfs_rmdir(pathname, 0);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::linkTest() {
    string path = mountpoint + "/linktest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/linktest2";
    const char *pathname2 = path2.c_str();
    plfs_error_t ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_link(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOSYS, ret);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::chownTest() {
    string path = mountpoint + "/chowntest1";
    const char *pathname = path.c_str();
    plfs_error_t ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (uid_t luid = 0; luid < 65536; luid += 1000) {
	for (gid_t lgid = 0; lgid < 65536; lgid += 1000) {
	    struct stat stbuf;
	    ret = plfs_chown(pathname, luid, lgid);
	    if (uid != 0) { // not the root user.
		CPPUNIT_ASSERT_EQUAL(PLFS_EPERM, ret);
		goto unlinkout;
	    }
	    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	    ret = plfs_getattr(NULL, pathname, &stbuf, 0);
	    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	    CPPUNIT_ASSERT_EQUAL(stbuf.st_uid, luid);
	    CPPUNIT_ASSERT_EQUAL(stbuf.st_gid, lgid);
	}
    }
    ret = plfs_chown(pathname, uid, gid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
unlinkout:
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::symlinkTest() {
    string path = mountpoint + "/symlinktest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/symlinktest2";
    const char *pathname2 = path2.c_str();
    plfs_error_t ret;
    Plfs_fd *fd = NULL;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_symlink(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_open(&fd, pathname2, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_symlink("./nonexist.ne", pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    fd = NULL;
    ret = plfs_open(&fd, pathname2, O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (int i = 0; i < 100; i++) {
	string linkcontent = "./noexist.ne";
	char buf[256];
	sprintf(buf, "%d", i);
	linkcontent += buf;
	ret = plfs_symlink(linkcontent.c_str(), pathname2);
	CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	plfs_readlink(pathname2, buf, 256, (int *)&ret);
	CPPUNIT_ASSERT((int)linkcontent.length() == (int)ret);
	CPPUNIT_ASSERT(strcmp(linkcontent.c_str(), buf) == 0);
	ret = plfs_unlink(pathname2);
	CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

static const char *
generateRandomString() {
    static char buf[256];
    sprintf(buf, "/%d%d%d%d", rand(), rand(), rand(), rand());
    return buf;
}

static void
renameSeveralTimes(string &from, int nTimes, const string mpt) {
    plfs_error_t ret;

    for (int i = 0; i < nTimes; i++) {
        string to = mpt + generateRandomString();
        ret = plfs_rename(from.c_str(), to.c_str());
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        ret = plfs_access(to.c_str(), F_OK);
        CPPUNIT_ASSERT_EQUAL(0, (int)ret);
        from = to;
    }
}

void
PlfsUnit::renameTest() {
    string path = mountpoint + "/renametest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/renametest2";
    const char *pathname2 = path2.c_str();
    string path3 = mountpoint + "/dirtoberenamed";
    plfs_error_t ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    renameSeveralTimes(path, 30, mountpoint);
    ret = plfs_symlink(path.c_str(), pathname2);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    renameSeveralTimes(path2, 30, mountpoint);
    ret = plfs_mkdir(path3.c_str(), PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    renameSeveralTimes(path3, 30, mountpoint);
#ifdef NEGATIVE_TEST_CASES
    ret = plfs_rename(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOENT, ret);
#endif
    ret = plfs_unlink(path.c_str());
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_unlink(path2.c_str());
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_rmdir(path3.c_str(), 0);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::dirTest() {
    string path = mountpoint + "/dirtest1";
    const char *pathname = path.c_str();
    set<string> dents;
    set<string> readres;
    set<string>::iterator it;
    plfs_error_t ret;

    ret = plfs_mkdir(pathname, PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (int i = 0; i < 100; i++) {
	string subdir = generateRandomString();
	pair<set<string>::iterator, bool> result;
	result = dents.insert(subdir);
	if (result.second) {
	    string fullpath = path + subdir;
	    ret = plfs_mkdir(fullpath.c_str(), PLFSUNIT_DEFAULT_DIR_MODE);
	    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	}
    }
    ret = plfs_rmdir(pathname, 0);
    CPPUNIT_ASSERT_EQUAL(PLFS_ENOTEMPTY, ret);
    ret = plfs_readdir(pathname, &readres);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    for (it=readres.begin() ; it != readres.end(); it++) {
	if (*it == "." || *it == "..") continue;
	string fullpath = path + "/" + *it;
	ret = plfs_rmdir(fullpath.c_str(), 0);
	CPPUNIT_ASSERT_EQUAL(0, (int)ret);
	dents.erase("/" + *it);
    }
    CPPUNIT_ASSERT(dents.empty());
    ret = plfs_rmdir(pathname, 0);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::truncateTest() {
    string path = mountpoint + "/trunctest1";
    const char *pathname = path.c_str();
    Plfs_fd *fd = NULL;
    plfs_error_t ret;
    struct stat stbuf;
    ssize_t written;

    ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_write(fd, "SIMPLE_TRUNCATE_TEST", 21, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(21, (int)written);
    ret = plfs_trunc(fd, pathname, 15, true);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_getattr(fd, pathname, &stbuf, 1);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(stbuf.st_size == 15);
    ret = plfs_close(fd, pid, uid, 0, NULL, &ref_count);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_trunc(NULL, pathname, 5, true);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    ret = plfs_getattr(NULL, pathname, &stbuf, 1);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    CPPUNIT_ASSERT(stbuf.st_size == 5);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
}

void
PlfsUnit::GetShadowsTest() {
    plfs_error_t ret;
    int          nshadow, myshadow;
    char         **shadows;
    ret = plfs_get_shadows(plfsmountpoint.c_str(), &nshadow, &myshadow,
                           &shadows);
    CPPUNIT_ASSERT_EQUAL(0, (int)ret);
    printf("\nList all shadow backends:\n");
    for (int i = 0; i < nshadow; i++) {
        printf("Shadow backend %d: %s %s\n", i, shadows[i],
               (myshadow == i) ? "(Mine)" : "");
        free(shadows[i]);
    }
    free(shadows);
}

void
PlfsFileUnit::setUp() {
    std::string filename = plfsmountpoint + "/fileunitfile.tst";
    Plfs_fd *fd = NULL;
    plfs_error_t ret;
    ret = plfs_open(&fd, filename.c_str(), O_CREAT | O_RDWR, pid,
                    0666 | S_ISVTX, NULL);
    CPPUNIT_ASSERT(ret == PLFS_SUCCESS);
    filedes = fd;
    pid = getpid();
    return ;
}

void
PlfsFileUnit::tearDown()
{
    plfs_error_t ret;
    uid_t uid = getuid();
    ret = plfs_close(filedes, pid, uid, 0, NULL, &ref_count);
    std::string filename = plfsmountpoint + "/fileunitfile.tst";
    ret = plfs_unlink(filename.c_str());
    return ;
}

void
PlfsFileUnit::RWSliceTest()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[40];

    ret = plfs_write(filedes, "SIMPLE_TRUNCATE_TEST", 21, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    ret = plfs_read(filedes, rbuf, 13, 0, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf, "SIMPLE_TRUNCATE_TEST", 13) == 0);
}

void
PlfsFileUnit::RWMergeTest()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[40];

    ret = plfs_write(filedes, "Merge", 5, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_write(filedes, "Test", 4, 5, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    ret = plfs_read(filedes, rbuf, 9, 0, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf, "MergeTest", 9) == 0);
}

void
PlfsFileUnit::RWXTest()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[2][20];
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    char *wbuf[2] = {(char *)"Hello", (char *)"World"};
    struct iovec memranges[2] = {{wbuf[0], 5}, {wbuf[1], 5}};
    struct iovec rdranges[2] = {{rbuf[0], 5}, {rbuf[1], 5}};

    ret = plfs_writex(filedes, memranges, 2, ioranges, 2, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    ret = plfs_readx(filedes, rdranges, 2, ioranges, 2, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], wbuf[0], 5) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], wbuf[1], 5) == 0);
}

void
PlfsFileUnit::RWXSliceTestW()
{
    plfs_error_t ret;
    ssize_t written;
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    char *wbuf[2] = {(char *)"Hello", (char *)"World"};
    struct iovec memranges[2] = {{wbuf[0], 5}, {wbuf[1], 5}};
    char rrbuf[20];

    ret = plfs_writex(filedes, memranges, 2, ioranges, 2, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    ret = plfs_read(filedes, rrbuf, 10, 0, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rrbuf, "HelloWorld", 10) == 0);
}

void
PlfsFileUnit::RWXSliceTestR()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[2][20];
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    struct iovec rdranges[2] = {{rbuf[0], 5}, {rbuf[1], 5}};

    ret = plfs_write(filedes, "HelloWorld", 10, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    ret = plfs_readx(filedes, rdranges, 2, ioranges, 2, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], "Hello", 5) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], "World", 5) == 0);
}

void
PlfsFileUnit::RWXMergeTest()
{
    plfs_error_t ret;
    ssize_t written;
    plfs_xvec ioranges[3] = {{0, 5}, {5, 5}, {10, 5}};
    char *wbuf[3] = {(char *)"Hello", (char *)"World", (char *)"Check"};
    struct iovec memranges[3] = {{wbuf[0], 5}, {wbuf[1], 5}, {wbuf[2], 5}};

    ret = plfs_writex(filedes, memranges, 3, ioranges, 3, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    char rbuf[2][20];
    plfs_xvec rioranges[2] = {{3, 4}, {8, 4}};
    struct iovec rdranges[2] = {{rbuf[0], 4}, {rbuf[1], 4}};
    ret = plfs_readx(filedes, rdranges, 2, rioranges, 2, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], "loWo", 4) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], "ldCh", 4) == 0);
}

void
PlfsFileUnit::RWXChecksum()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[2][20];
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    char *wbuf[2] = {(char *)"Hello", (char *)"World"};
    struct iovec memranges[2] = {{wbuf[0], 5}, {wbuf[1], 5}};
    struct iovec rdranges[2] = {{rbuf[0], 5}, {rbuf[1], 5}};
    Plfs_checksum checksums[2];
    ret = plfs_get_checksum(wbuf[0], 5, &checksums[0]);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_get_checksum(wbuf[1], 5, &checksums[1]);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_writexc(filedes, memranges, 2, ioranges, 2, pid, &written,
                      checksums);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    Plfs_checksum rchecksums[2];
    ret = plfs_readxc(filedes, rdranges, 2, ioranges, 2, &written, rchecksums);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], wbuf[0], 5) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], wbuf[1], 5) == 0);
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[0], 5, rchecksums[0]));
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[1], 5, rchecksums[1]));
}

void
PlfsFileUnit::RWXWrongChecksum()
{
    plfs_error_t ret;
    ssize_t written;
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    char *wbuf[2] = {(char *)"Hello", (char *)"World"};
    struct iovec memranges[2] = {{wbuf[0], 5}, {wbuf[1], 5}};
    Plfs_checksum checksums[2];
    ret = plfs_get_checksum(wbuf[0], 5, &checksums[0]);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_get_checksum(wbuf[1], 5, &checksums[1]);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    checksums[1] -= 1;
    ret = plfs_writexc(filedes, memranges, 2, ioranges, 2, pid, &written,
                      checksums);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_EIO);
}

void
PlfsFileUnit::RWXSliceChecksum()
{
    plfs_error_t ret;
    ssize_t written;
    char rbuf[2][20];
    plfs_xvec ioranges[2] = {{0, 5}, {5, 5}};
    struct iovec rdranges[2] = {{rbuf[0], 5}, {rbuf[1], 5}};

    ret = plfs_write(filedes, "HelloWorld", 10, 0, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    Plfs_checksum rchecksums[2];
    ret = plfs_readxc(filedes, rdranges, 2, ioranges, 2, &written, rchecksums);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], "Hello", 5) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], "World", 5) == 0);
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[0], 5, rchecksums[0]));
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[1], 5, rchecksums[1]));
}

void
PlfsFileUnit::RWXSliceMergeChecksum()
{
    plfs_error_t ret;
    ssize_t written;
    plfs_xvec ioranges[3] = {{0, 5}, {5, 5}, {10, 5}};
    char *wbuf[3] = {(char *)"Hello", (char *)"World", (char *)"Check"};
    struct iovec memranges[3] = {{wbuf[0], 5}, {wbuf[1], 5}, {wbuf[2], 5}};

    ret = plfs_writex(filedes, memranges, 3, ioranges, 3, pid, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    ret = plfs_sync(filedes);
    char rbuf[2][20];
    plfs_xvec rioranges[2] = {{3, 4}, {8, 4}};
    struct iovec rdranges[2] = {{rbuf[0], 4}, {rbuf[1], 4}};
    Plfs_checksum rchecksums[2];
    ret = plfs_readx(filedes, rdranges, 2, rioranges, 2, &written);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], "loWo", 4) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], "ldCh", 4) == 0);
    ret = plfs_readxc(filedes, rdranges, 2, rioranges, 2, &written,rchecksums);
    CPPUNIT_ASSERT_EQUAL(ret, PLFS_SUCCESS);
    CPPUNIT_ASSERT(memcmp(rbuf[0], "loWo", 4) == 0);
    CPPUNIT_ASSERT(memcmp(rbuf[1], "ldCh", 4) == 0);
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[0], 4, rchecksums[0]));
    CPPUNIT_ASSERT(!plfs_checksum_match(rbuf[1], 4, rchecksums[1]));
}
