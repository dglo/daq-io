package icecube.daq.io;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.Poolable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class AdjustablePayload
    implements IWriteablePayload
{
    private int len;
    private int value;

    public AdjustablePayload(int len)
    {
        this.len = len;
    }

    @Override
    public Object deepCopy()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void dispose()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public int getPayloadType()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public long getUTCTime()
    {
        return 0L;
    }

    int getValue()
    {
        return value;
    }

    @Override
    public int length()
    {
        return len;
    }

    @Override
    public void recycle()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void setCache(IByteBufferCache cache)
    {
        throw new Error("Unimplemented");
    }

    void setValue(int val)
    {
        value = val;
    }

    @Override
    public int writePayload(boolean b0, int offset, ByteBuffer buf)
        throws IOException
    {
        if (buf.capacity() - offset >= 4) {
            buf.putInt(offset, value);
        }
        return len;
    }
}

public class FileDispatcherTest
    extends LoggingCase
{
    private File testDirectory;

    /**
     * Constructs an instance of this test.
     *
     * @param name the name of the test.
     */
    public FileDispatcherTest(String name)
    {
        super(name);
    }

    private void checkDataDir(File destDir, int numDataFiles,
                              File tempFile, boolean expTempFile)
    {
        final int expNumFiles = numDataFiles + (expTempFile ? 1 : 0);

        String[] files = destDir.list();
        assertEquals("Unexpected number of files",
                     expNumFiles, files.length);

        boolean foundTemp = false;
        int numData = 0;
        for (int i = 0; i < files.length; i++) {
            if (files[i].equals(tempFile.getName())) {
                if (!expTempFile) {
                    fail("Found unexpected temp file");
                }

                foundTemp = true;
            } else if (files[i].startsWith("physics") &&
                       files[i].endsWith(".dat"))
            {
                numData++;
            } else {
                fail("Unknown file " + files[i]);
            }
        }

        if (expTempFile) {
            assertTrue("Unexpected temp file", foundTemp);
        }

        assertEquals("Bad number of files", numDataFiles, numData);
    }

    private static boolean clearDirectory(File dir)
    {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDirectory(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        return true;
    }

    public static File createTempDirectory()
        throws IOException
    {
        final File temp;

        temp = File.createTempFile("fdtest", "dir");

        if (!(temp.delete()))
        {
            throw new IOException("Could not delete temp file: " +
                                  temp.getAbsolutePath());
        }

        if (!(temp.mkdir()))
        {
            throw new IOException("Could not create temp directory: " +
                                  temp.getAbsolutePath());
        }

        return temp;
    }

    private static boolean deleteDirectory(File dir)
    {
        if (!clearDirectory(dir)) {
            return false;
        }

        return dir.delete();
    }

    @Override
    protected void setUp()
        throws Exception
    {
        super.setUp();

        File tempFile = new File(FileDispatcher.TEMP_PREFIX + "physics");
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Override
    protected void tearDown()
        throws Exception
    {
        File tempFile = new File(FileDispatcher.TEMP_PREFIX + "physics");
        if (tempFile.exists()) {
            tempFile.delete();
        }

        if (testDirectory != null) {
            if (!deleteDirectory(testDirectory)) {
                System.err.println("Couldn't tear down test directory");
            }
        }

        super.tearDown();
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(FileDispatcherTest.class);
    }

    public void testNullBase()
    {
        FileDispatcher fd;
        try {
            fd = new FileDispatcher(null);
            fail("Should not succeed for null base file name");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }
    }

    public void testNullDest()
    {
        FileDispatcher fd = new FileDispatcher(".", "physics");
        assertEquals("Unexpected destination directory",
                     ".", fd.getDispatchDestStorage().getPath());

        try {
            fd.setDispatchDestStorage(null);
            fail("Should not be able to set null destination directory");
        } catch (IllegalArgumentException iae) {
            // expect failure
        }
    }

    public void testBadDest()
    {
        final String badDir = "/bad/dir/path";

        FileDispatcher fd = new FileDispatcher(badDir, "physics");
        assertEquals("Unexpected destination directory",
                     ".", fd.getDispatchDestStorage().getPath());

        assertLogMessage(badDir + " does not exist or is not writable!" +
                         "  Using current directory.");
        assertNoLogMessages();

        try {
            fd.setDispatchDestStorage(badDir);
            fail("Should not be able to set bogus destination directory");
        } catch (IllegalArgumentException iae) {
            // expect failure
        }
    }

    public void testBadBase()
    {
        try {
            testDirectory = createTempDirectory();
        } catch (IOException ioe) {
            fail("Cannot create temporary directory");
        }

        final String baseName = "foo";

        FileDispatcher fd = new FileDispatcher(baseName);
        assertNoLogMessages();

        try {
            fd.dataBoundary(FileDispatcher.START_PREFIX + "12345");
        } catch (DispatchException de) {
            fail("Unexpected exception: " + de);
        }
        assertLogMessage("Dispatching to unusual base name " + baseName);

        final File destDir = new File(FileDispatcher.DISPATCH_DEST_STORAGE);
        if (!destDir.isDirectory() || !destDir.canWrite()) {
            assertLogMessage(FileDispatcher.DISPATCH_DEST_STORAGE +
                             " does not exist or is not writable!" +
                             "  Using current directory.");
        }
    }

    public void testGoodDest()
    {
        final String goodDir = "subdir";

        File subdirFile = new File(goodDir);

        final boolean preexist = subdirFile.isDirectory();

        if (!preexist) {
            subdirFile.mkdir();
        }

        try {
            FileDispatcher fd = new FileDispatcher(goodDir, "physics");
            assertEquals("Unexpected destination directory",
                         goodDir, fd.getDispatchDestStorage().getPath());

            FileDispatcher fd2 = new FileDispatcher(".", "physics");
            assertEquals("Unexpected destination directory",
                         ".", fd2.getDispatchDestStorage().getPath());

            fd2.setDispatchDestStorage(goodDir);
            assertEquals("Unexpected destination directory",
                         goodDir, fd2.getDispatchDestStorage().getPath());
        } finally {
            if (!preexist) {
                deleteDirectory(subdirFile);
            }
        }
    }

    public void testDispatchEvent()
        throws DispatchException
    {
        try {
            testDirectory = createTempDirectory();
        } catch (IOException ioe) {
            fail("Cannot create temporary directory");
        }

        IByteBufferCache bufCache = new MockBufferCache("DispEvt");

        FileDispatcher fd = new FileDispatcher(testDirectory.getAbsolutePath(),
                                               "physics", bufCache);
        assertNoLogMessages();

        assertNotNull("ByteBuffer was null", fd.getByteBufferCache());

        assertEquals("Total dispatched events is not zero",
                     0, fd.getTotalDispatchedEvents());

        fd.dispatchEvent(new AdjustablePayload(8));

        assertEquals("Total dispatched events was not incremented",
                     1, fd.getTotalDispatchedEvents());
    }

    public void testDispatchEventWithUnsetCache()
        throws DispatchException
    {
        FileDispatcher fd = new FileDispatcher("physics");

        try {
            fd.dispatchEvent(new AdjustablePayload(8));
            fail("Shouldn't be able to dspatch without setting buffer cache");
        } catch (DispatchException de) {
            // expect this to fail
        }
    }

    public void testReadOnlyDir()
        throws DispatchException
    {
        try {
            testDirectory = createTempDirectory();
        } catch (IOException ioe) {
            fail("Cannot create temporary directory");
        }
        testDirectory.setReadOnly();

        FileDispatcher fd =
            new FileDispatcher(testDirectory.getAbsolutePath(), "physics");
        assertEquals("Unexpected destination directory",
                     ".", fd.getDispatchDestStorage().getPath());
        assertLogMessage(testDirectory + " does not exist or is not" +
                         " writable!  Using current directory.");
    }

    public void testMaxFileSize()
        throws DispatchException
    {
        FileDispatcher fd = new FileDispatcher("physics");

        try {
            fd.setMaxFileSize(-1000);
            fail("Shouldn't be able to set negative file size");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }
        assertNoLogMessages();

        try {
            fd.setMaxFileSize(0);
            fail("Shouldn't be able to set file size to zero");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }
        assertNoLogMessages();

        fd.setMaxFileSize(100);
    }

    public void testBogusDataBoundary()
    {
        FileDispatcher fd = new FileDispatcher("physics");

        try {
            fd.dataBoundary();
            fail("Bogus dataBoundary() should not succeed");
        } catch (DispatchException de) {
            // expect this to fail
        }
    }

    public void testDataBoundary()
        throws DispatchException
    {
        try {
            testDirectory = createTempDirectory();
        } catch (IOException ioe) {
            fail("Cannot create temporary directory");
        }

        IByteBufferCache bufCache = new MockBufferCache("DispEvt");

        FileDispatcher fd = new FileDispatcher(testDirectory.getAbsolutePath(),
                                               "physics", bufCache);
        assertNoLogMessages();

        try {
            fd.dataBoundary(null);
            fail("Should not be able to specify null data boundary");
        } catch (DispatchException de) {
            // expect this to fail
        }

        try {
            fd.dataBoundary("bogus");
            fail("Should not be able to specify bogus data boundary");
        } catch (DispatchException de) {
            // expect this to fail
        }

        try {
            fd.dataBoundary(Dispatcher.STOP_PREFIX);
            fail("Should not be able to stop unstarted run");
        } catch (DispatchException de) {
            // expect this to fail
        }

        assertEquals("Bad initial run number", 0, fd.getRunNumber());

        try {
            fd.dataBoundary(Dispatcher.START_PREFIX + "ABC");
        } catch (DispatchException de) {
            // expect this to fail
        }

        final int firstNum = 123;
        fd.dataBoundary(Dispatcher.START_PREFIX + firstNum);
        assertEquals("Incorrect run number", firstNum, fd.getRunNumber());

        final int runNum = 456;
        fd.dataBoundary(Dispatcher.START_PREFIX + runNum);
        assertEquals("Incorrect run number", runNum, fd.getRunNumber());

        assertLogMessage("Run " + runNum +
                         " started without stopping run " + firstNum);

        fd.dispatchEvent(new AdjustablePayload(8));

        fd.dataBoundary(Dispatcher.CLOSE_PREFIX);

        fd.dispatchEvent(new AdjustablePayload(8));

        fd.dataBoundary(Dispatcher.SUBRUN_START_PREFIX);

        fd.dispatchEvent(new AdjustablePayload(8));

        fd.dataBoundary(Dispatcher.SWITCH_PREFIX + "789");

        fd.dispatchEvent(new AdjustablePayload(8));

        fd.dataBoundary(Dispatcher.STOP_PREFIX);

        try {
            fd.dataBoundary(Dispatcher.STOP_PREFIX);
            fail("Shouldn't be able to stop more times than we started");
        } catch (DispatchException de) {
            // expect this to fail
        }
    }

    public void testFull()
        throws DispatchException
    {
        try {
            testDirectory = createTempDirectory();
        } catch (IOException ioe) {
            fail("Cannot create temporary directory");
        }

        IByteBufferCache bufCache = new MockBufferCache("Full");

        FileDispatcher fd = new FileDispatcher(testDirectory.getAbsolutePath(),
                                               "physics", bufCache);
        assertNotNull("ByteBuffer was null", fd.getByteBufferCache());

        final int maxFileSize = 100;
        fd.setMaxFileSize(maxFileSize);

        File tempFile = fd.getTempFile(testDirectory, "physics");

        for (int i = 10; i < 15; i++) {
            fd.dataBoundary(Dispatcher.START_PREFIX + i);
            assertEquals("Incorrect run number", i, fd.getRunNumber());

            IWriteablePayload payload = new AdjustablePayload(i);

            assertEquals("Number of dispatched events is not zero",
                         0, fd.getNumDispatchedEvents());
            assertEquals("Unexpected total number of dispatched events",
                         (i - 10) * 20, fd.getTotalDispatchedEvents());

            assertFalse("Temp file should not exist", tempFile.exists());

            int dataFiles = 0;
            int fileLen = 0;
            for (int j = 0; j < 20; j++) {
                fd.dispatchEvent(payload);
                fileLen += i;

                assertEquals("Number of dispatched events was not incremented",
                             j + 1, fd.getNumDispatchedEvents());
                assertEquals("Number of dispatched events was not incremented",
                             (i - 10) * 20 + j + 1,
                             fd.getTotalDispatchedEvents());

                if (fileLen <= maxFileSize) {
                    assertTrue("Temp file should exist",
                               tempFile.exists());
                } else {
                    dataFiles++;
                    fileLen = 0;
                    assertFalse("Temp file should not exist",
                                tempFile.exists());
                }
            }

            final boolean expTempFile = (fileLen > 0);

            checkDataDir(testDirectory, dataFiles, tempFile, expTempFile);

            fd.dataBoundary(Dispatcher.STOP_PREFIX);

            if (expTempFile) {
                checkDataDir(testDirectory, dataFiles + 1, tempFile, false);
            }

            clearDirectory(testDirectory);
        }
    }

    /**
     * Main routine which runs text test in standalone mode.
     *
     * @param args the arguments with which to execute this method.
     */
    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
