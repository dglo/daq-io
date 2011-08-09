package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockObserver;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class MockPushSimpleRdr
    extends PushSimpleReader
{
    private IByteBufferCache bufMgr;
    private int recvCnt;
    private boolean gotStop;

    MockPushSimpleRdr(String name, IByteBufferCache bufMgr)
        throws IOException
    {
        super(name);

        this.bufMgr = bufMgr;
    }

    int getReceiveCount()
    {
        return recvCnt;
    }

    public void pushBuffer(ByteBuffer bb)
        throws IOException
    {
        bufMgr.returnBuffer(bb);
        recvCnt++;
    }

    public void sendStop()
    {
        gotStop = true;
    }
}

public class PushSimpleReaderTest
    extends LoggingCase
{
    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private MockPushSimpleRdr tstRdr;

    /**
     * Construct an instance of this test.
     *
     * @param name the name of the test.
     */
    public PushSimpleReaderTest(String name)
    {
        super(name);
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        tstRdr = null;
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(PushSimpleReaderTest.class);
    }

    protected void tearDown()
        throws Exception
    {
        if (tstRdr != null) {
            tstRdr.destroyProcessor();
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        IByteBufferCache bufMgr = new MockBufferCache("StartStop");

        tstRdr = new MockPushSimpleRdr("StartStop", bufMgr);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(tstRdr, "forced stop");

        tstRdr.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(tstRdr);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            tstRdr.startProcessing();
            fail("Reader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    public void testStartDispose()
        throws Exception
    {
        IByteBufferCache bufMgr = new MockBufferCache("StartDisp");

        tstRdr = new MockPushSimpleRdr("StartDisp", bufMgr);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        tstRdr.startDisposing();
        IOTestUtil.waitUntilDisposing(tstRdr);
    }

    public void testOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("OutIn");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new MockPushSimpleRdr("OutIn", bufMgr);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int loopCnt = 0;
        while (tstRdr.getReceiveCount() < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                final int acquireLen = bufLen;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt++;
            } else {
                try {
                    Thread.sleep(100);
                } catch (Exception ex) {
                    // ignore interrupts
                }
            }

            loopCnt++;
            if (loopCnt > INPUT_OUTPUT_LOOP_CNT * 2) {
                fail("Received " + tstRdr.getReceiveCount() +
                     " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop.", observer.gotSinkStop());
	assertNotNull("number of messages received", tstRdr.getDequeuedMessages());
	assertNotNull("total number of stop messages", tstRdr.getStopMessagesPropagated());
	assertNotNull("total number of stop messages", tstRdr.getTotalStopsReceived());
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr = new MockBufferCache("MultiOutIn");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        MockObserver observer = new MockObserver();

        tstRdr = new MockPushSimpleRdr("MultiOutIn", bufMgr);
        tstRdr.registerComponentObserver(observer);

        tstRdr.start();
        IOTestUtil.waitUntilStopped(tstRdr, "creation");

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        Thread.sleep(100);

        tstRdr.startProcessing();
        IOTestUtil.waitUntilRunning(tstRdr);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;
        final int groupSize = 3;

        final int numToSend = INPUT_OUTPUT_LOOP_CNT * groupSize;

        int id = 1;
        int recvId = 0;

        int xmitCnt = 0;
        int loopCnt = 0;
        while (tstRdr.getReceiveCount() < numToSend) {
            if (xmitCnt < numToSend) {
                final int acquireLen = bufLen * groupSize;
                testBuf = bufMgr.acquireBuffer(acquireLen);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                for (int i = 0; i < groupSize; i++) {
                    final int start = bufLen * i;
                    testBuf.putInt(start, bufLen);
                    testBuf.putInt(start + 4, id++);
                }
                testBuf.limit(acquireLen);
                testBuf.position(0);
                sinkChannel.write(testBuf);

                bufMgr.returnBuffer(testBuf);

                xmitCnt += groupSize;
            } else {
                try {
                    Thread.sleep(100);
                } catch (Exception ex) {
                    // ignore interrupts
                }
            }

            loopCnt++;
            if (loopCnt == numToSend * 2) {
                fail("Received " + tstRdr.getReceiveCount() +
                     " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        IOTestUtil.sendStopMsg(sinkChannel);
        IOTestUtil.waitUntilStopped(tstRdr, "stop msg");
        assertTrue("Observer didn't see sinkStop.", observer.gotSinkStop());
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
