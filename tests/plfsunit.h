#ifndef __PLFSCREATE_H__
#define __PLFSCREATE_H__

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <plfs.h>

using namespace std;

class PlfsUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (PlfsUnit);
        CPPUNIT_TEST (GetShadowsTest);
	CPPUNIT_TEST (createTest);
	CPPUNIT_TEST (openCloseTest);
	CPPUNIT_TEST (readWriteTest);
	CPPUNIT_TEST (chmodTest);
	CPPUNIT_TEST (chmodDirTest);
	CPPUNIT_TEST (linkTest);
	CPPUNIT_TEST (chownTest);
	CPPUNIT_TEST (symlinkTest);
	CPPUNIT_TEST (renameTest);
	CPPUNIT_TEST (dirTest);
	CPPUNIT_TEST (truncateTest);
	CPPUNIT_TEST_SUITE_END ();

public:
        void setUp (void);
        void tearDown (void);

protected:
	void createTest();
	void openCloseTest();
	void readWriteTest();
	void chmodTest();
	void chmodDirTest();
	void linkTest();
	void chownTest();
	void symlinkTest();
	void renameTest();
	void dirTest();
	void truncateTest();
        void GetShadowsTest();
private:
	string mountpoint;
	pid_t pid;
	uid_t uid;
	gid_t gid;
};

class PlfsFileUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (PlfsFileUnit);
	CPPUNIT_TEST (RWSliceTest);
	CPPUNIT_TEST (RWMergeTest);
        CPPUNIT_TEST (RWXTest);
        CPPUNIT_TEST (RWXSliceTestW);
        CPPUNIT_TEST (RWXSliceTestR);
        CPPUNIT_TEST (RWXMergeTest);
        CPPUNIT_TEST (RWXChecksum);
        CPPUNIT_TEST (RWXWrongChecksum);
        CPPUNIT_TEST (RWXSliceChecksum);
        CPPUNIT_TEST (RWXSliceMergeChecksum);
	CPPUNIT_TEST_SUITE_END ();
public:
        void setUp (void);
        void tearDown (void);

protected:
        void RWSliceTest();
        void RWMergeTest();
        void RWXTest();
        void RWXSliceTestW();
        void RWXSliceTestR();
        void RWXMergeTest();
        void RWXChecksum();
        void RWXWrongChecksum();
        void RWXSliceChecksum();
        void RWXSliceMergeChecksum();
private:
        Plfs_fd *filedes;
        pid_t pid;
};

#endif
