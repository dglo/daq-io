package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.test.LoggingCase;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.PayloadDestination;
import icecube.daq.payload.VitreousBufferCache;

import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.splicer.PayloadFactory;

import icecube.util.Poolable;

import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.zip.DataFormatException;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

class AdjustablePayload
    extends Payload
{
    private int len;

    AdjustablePayload(int len)
    {
        this.len = len;
    }

    public int compareTo(Object x0)
    {
        throw new Error("Unimplemented");
    }

    public Object deepCopy()
    {
        throw new Error("Unimplemented");
    }

    public void dispose()
    {
        throw new Error("Unimplemented");
    }

    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadInterfaceType()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadLength()
    {
        return len;
    }

    public int getPayloadOffset()
    {
        throw new Error("Unimplemented");
    }

    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadType()
    {
        throw new Error("Unimplemented");
    }

    public Poolable getPoolable()
    {
        throw new Error("Unimplemented");
    }

    public boolean hasBeenDisposed()
    {
        throw new Error("Unimplemented");
    }

    public void hasBeenDisposed(boolean b0)
    {
        throw new Error("Unimplemented");
    }

    public boolean hasBeenRecycled()
    {
        throw new Error("Unimplemented");
    }

    public void hasBeenRecycled(boolean b0)
    {
        throw new Error("Unimplemented");
    }

    public void initialize(int i0, ByteBuffer x1, PayloadFactory x2)
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    protected void loadEnvelope()
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    public void loadPayload()
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    public void loadSpliceablePayload()
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    public int readSpliceableLength(int i0, ByteBuffer x1)
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    public void recycle()
    {
        throw new Error("Unimplemented");
    }

    public void setPayloadBuffer(int i0, ByteBuffer x1)
        throws IOException, DataFormatException
    {
        throw new IOException("Unimplemented");
    }

    public void setPayloadTimeUTC(IUTCTime x0)
    {
        throw new Error("Unimplemented");
    }

    public void shiftOffset(int i0)
    {
        throw new Error("Unimplemented");
    }

    public int writePayload(PayloadDestination x0)
        throws IOException
    {
        throw new IOException("Unimplemented");
    }

    public int writePayload(boolean b0, PayloadDestination x1)
        throws IOException
    {
        throw new IOException("Unimplemented");
    }

    public int writePayload(boolean writeLoaded, int offset, ByteBuffer buf)
        throws IOException
    {
        buf.position(offset);
        buf.putInt(len);

        for (int i = 4; i < len - 3; i += 4) {
            buf.putInt(i / 4);
        }

        byte val = 0;
        while (buf.position() < offset + len) {
            buf.put(val++);
        }

        buf.flip();

        return len;
    }

    public int writePayload(int i0, ByteBuffer x1)
        throws IOException
    {
        throw new IOException("Unimplemented");
    }
}

public class FileDispatcherTest
    extends LoggingCase
{
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

    private static boolean deleteDirectory(File dir)
    {
        if (!clearDirectory(dir)) {
            return false;
        }

        return dir.delete();
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        File tempFile = FileDispatcher.getTempFile(".", "physics");
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    protected void tearDown()
        throws Exception
    {
        File tempFile = FileDispatcher.getTempFile(".", "physics");
        if (tempFile.exists()) {
            tempFile.delete();
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

    public void testBadBase()
    {
        FileDispatcher fd;
        try {
            fd = new FileDispatcher("foo");
            fail("Should not succeed for bogus base file name");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }
    }

    public void testNullDest()
    {
        try {
            new FileDispatcher(null, "physics");
            fail("Should not be able to specify null destination directory");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }

        FileDispatcher fd = new FileDispatcher(".", "physics");
        assertEquals("Unexpected destination directory",
                     ".", fd.getDispatchDestinationDirectory());

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
                     ".", fd.getDispatchDestinationDirectory());

        assertEquals("Bad number of log messages",
                     1, getNumberOfMessages());
        assertEquals("Unexpected log message 0",
                     badDir + " does not exist!  Using current directory.",
                     getMessage(0));
        clearMessages();

        try {
            fd.setDispatchDestStorage(badDir);
            fail("Should not be able to set bogus destination directory");
        } catch (IllegalArgumentException iae) {
            // expect failure
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
                         goodDir, fd.getDispatchDestinationDirectory());

            FileDispatcher fd2 = new FileDispatcher(".", "physics");
            assertEquals("Unexpected destination directory",
                         ".", fd2.getDispatchDestinationDirectory());

            fd2.setDispatchDestStorage(goodDir);
            assertEquals("Unexpected destination directory",
                         goodDir, fd2.getDispatchDestinationDirectory());
        } finally {
            if (!preexist) {
                deleteDirectory(subdirFile);
            }
        }
    }

    public void testDispatchEvent()
        throws DispatchException
    {
        IByteBufferCache bufCache = new VitreousBufferCache();

        FileDispatcher fd = new FileDispatcher("physics", bufCache);
        assertNotNull("ByteBuffer was null", fd.getByteBufferCache());

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

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

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

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
        final String destDirName = "readOnlyDir";

        File destDir = new File(destDirName);

        final boolean preexist = destDir.isDirectory();

        if (preexist) {
            fail("Read-only subdirectory exists");
        }

        destDir.mkdir();
        destDir.setReadOnly();

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            FileDispatcher fd =
                new FileDispatcher(destDirName, "physics");
            assertEquals("Unexpected destination directory",
                         ".", fd.getDispatchDestinationDirectory());
        } finally {
            deleteDirectory(destDir);
        }

        assertEquals("Bad number of log messages",
                     2, getNumberOfMessages());
        assertEquals("Unexpected log message 0",
                     "Cannot write to " + destDirName + "!", getMessage(0));
        assertEquals("Unexpected log message 1",
                     destDirName + " does not exist!  Using current directory.",
                     getMessage(1));
        clearMessages();

    }

    public void testUnimplemented()
        throws DispatchException
    {
        FileDispatcher fd = new FileDispatcher("physics");

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

        ByteBuffer bb = ByteBuffer.allocate(12);
        int[] indices = new int[] { 0, 4, 8 };

        final String unimplemented = "Unimplemented";

        try {
            fd.dispatchEvents(bb, indices);
            fail("Expected dispatchEvents(ByteBuffer, int[])" +
                 " to be unimplemented!");
        } catch (UnsupportedOperationException uoe) {
            if (!unimplemented.equals(uoe.getMessage())) {
                fail("Expected dispatchEvents(ByteBuffer, int[])" +
                     " to be unimplemented!");
            }
        }

        try {
            fd.dispatchEvents(bb, indices, 123);
            fail("Expected dispatchEvents(ByteBuffer, int[])" +
                 " to be unimplemented!");
        } catch (UnsupportedOperationException uoe) {
            if (!unimplemented.equals(uoe.getMessage())) {
                fail("Expected dispatchEvents(ByteBuffer, int[])" +
                     " to be unimplemented!");
            }
        }
    }

    public void testMaxFileSize()
        throws DispatchException
    {
        FileDispatcher fd = new FileDispatcher("physics");

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

        try {
            fd.setMaxFileSize(-1000);
            fail("Shouldn't be able to set negative file size");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }

        try {
            fd.setMaxFileSize(0);
            fail("Shouldn't be able to set file size to zero");
        } catch (IllegalArgumentException iae) {
            // expect this to fail
        }

        fd.setMaxFileSize(100);
    }

    public void testBogusDataBoundary()
    {
        FileDispatcher fd = new FileDispatcher("physics");

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

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
        FileDispatcher fd = new FileDispatcher("physics");

        if (!new File(FileDispatcher.DISPATCH_DEST_STORAGE).isDirectory()) {
            assertEquals("Bad number of log messages",
                         1, getNumberOfMessages());
            assertEquals("Unexpected log message 0",
                         FileDispatcher.DISPATCH_DEST_STORAGE +
                         " does not exist!  Using current directory.",
                         getMessage(0));
            clearMessages();
        }

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
            fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG);
            fail("Should not be able to stop unstarted run");
        } catch (DispatchException de) {
            // expect this to fail
        }

        assertEquals("Bad initial run number", 0, fd.getRunNumber());

        fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG + "123");
        assertEquals("Incorrect run number", 123, fd.getRunNumber());

        fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG + "456");
        assertEquals("Incorrect run number", 456, fd.getRunNumber());

        fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG);
        fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG);

        try {
            fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG);
            fail("Shouldn't be able to stop more times than we started");
        } catch (DispatchException de) {
            // expect this to fail
        }
    }

    public void testFull()
        throws DispatchException
    {
        final String destDirName = "renameDir";

        File destDir = new File(destDirName);

        final boolean preexist = destDir.isDirectory();

        if (!preexist) {
            destDir.mkdir();
        }

        try {
            IByteBufferCache bufCache = new VitreousBufferCache();

            FileDispatcher fd =
                new FileDispatcher(destDirName, "physics", bufCache);
            assertNotNull("ByteBuffer was null", fd.getByteBufferCache());

            final int maxFileSize = 100;
            fd.setMaxFileSize(maxFileSize);

            File tempFile = fd.getTempFile(destDirName, "physics");

            for (int i = 10; i < 15; i++) {
                fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTART_FLAG + i);
                assertEquals("Incorrect run number", i, fd.getRunNumber());

                Payload payload = new AdjustablePayload(i);

                assertEquals("Total dispatched events is not zero",
                             0, fd.getTotalDispatchedEvents());

                assertFalse("Temp file should not exist", tempFile.exists());

                int dataFiles = 0;
                int fileLen = 0;
                for (int j = 0; j < 20; j++) {
                    fd.dispatchEvent(payload);
                    fileLen += i;

                    assertEquals("Total dispatched events was not incremented",
                                 j + 1, fd.getTotalDispatchedEvents());

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

                checkDataDir(destDir, dataFiles, tempFile, expTempFile);

                fd.dataBoundary(DAQCmdInterface.DAQ_ONLINE_RUNSTOP_FLAG);

                if (expTempFile) {
                    checkDataDir(destDir, dataFiles + 1, tempFile, false);
                }

                clearDirectory(destDir);
            }
        } finally {
            if (!preexist) {
                deleteDirectory(destDir);
            }
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
