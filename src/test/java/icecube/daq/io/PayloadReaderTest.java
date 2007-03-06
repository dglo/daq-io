package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.ErrorState;
import icecube.daq.common.NormalState;

import icecube.daq.io.test.MockAppender;

import icecube.daq.payload.ByteBufferCache;
import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.BasicConfigurator;

import org.apache.log4j.Level;

class SimpleReader
    extends PayloadReader
{
    private ArrayList<ByteBuffer> inputData = new ArrayList<ByteBuffer>();

    class TestChannel
        extends InputChannel
    {
        TestChannel(InputChannelParent parent, SelectableChannel channel,
                    IByteBufferCache bufMgr, int bufSize)
            throws IOException
        {
            super(parent, channel, bufMgr, bufSize);
        }

        public void pushPayload(ByteBuffer buf)
        {
            synchronized (inputData) {
                inputData.add(buf);
            }
        }
    }

    SimpleReader(String name)
        throws IOException
    {
        super(name);
    }

    public InputChannel createChannel(SelectableChannel channel,
                               IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        return new TestChannel(this, channel, bufMgr, bufSize);
    }

    boolean hasPayloads()
    {
        return inputData.size() > 0;
    }

    List<ByteBuffer> getPayloads()
    {
        ArrayList list = new ArrayList();

        synchronized (inputData) {
            list.addAll(inputData);
            inputData.clear();
        }

        return list;
    }
}

public class PayloadReaderTest
    extends TestCase
    implements DAQComponentObserver
{
    private static final Log LOG = LogFactory.getLog(PayloadReaderTest.class);

    private static final int BUFFER_LEN = 5000;
    private static final int INPUT_OUTPUT_LOOP_CNT = 5;

    private static final String SRC_NOTE_ID = "SourceID";
    private static final String ERR_NOTE_ID = "ErrorID";

    private static Level logLevel = Level.INFO;

    private static ByteBuffer stopMsg;

    private boolean sinkStopNotificationCalled;
    private boolean sinkErrorNotificationCalled;

    private SimpleReader tstRdr;

    public PayloadReaderTest(String name)
    {
        super(name);
    }

    private SocketChannel acceptChannel(Selector sel)
        throws IOException
    {
        SocketChannel chan = null;

        while (chan == null) {
            int numSel = sel.select(500);
            if (numSel == 0) {
                continue;
            }

            Iterator iter = sel.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey selKey = (SelectionKey) iter.next();
                iter.remove();

                if (!selKey.isAcceptable()) {
                    selKey.cancel();
                    continue;
                }

                ServerSocketChannel ssChan =
                    (ServerSocketChannel) selKey.channel();
                if (chan != null) {
                    System.err.println("Got multiple socket connections");
                    continue;
                }

                try {
                    chan = ssChan.accept();
                } catch (IOException ioe) {
                    System.err.println("Couldn't accept client socket");
                    ioe.printStackTrace();
                    chan = null;
                }
            }
        }

        return chan;
    }

    private static final void checkGetters(PayloadReader rdr,
                                           int numReceiveChans,
                                           long bufsAcquired,
                                           long bytesAcquired,
                                           long bytesRcvd, long recsRcvd,
                                           long stopsRcvd)
    {
        Boolean[] allocStopped = rdr.getAllocationStopped();
        assertNotNull("Got null allocationStopped array", allocStopped);
        assertEquals("Bad allocationStopped length",
                     numReceiveChans, allocStopped.length);
        if (numReceiveChans > 0) {
            assertFalse("allocationStopped[0] was not false",
                        allocStopped[0].booleanValue());
        }

        for (int i = 0; i < 9; i++) {
            Long[] data;
            String name;
            long val;

            switch (i) {
            case 0:
                name = "curAcqBuf";
                data = rdr.getBufferCurrentAcquiredBuffers();
                val = bufsAcquired;
            break;
            case 1:
                name = "curAcqByt";
                data = rdr.getBufferCurrentAcquiredBytes();
                val = bytesAcquired;
                break;
            case 2:
                name = "bytesRcvd";
                data = rdr.getBytesReceived();
                val = bytesRcvd;
                break;
            case 3:
                name = "lim2Rest";
                data = rdr.getLimitToRestartAllocation();
                val = 100000;
                break;
            case 4:
                name = "lim2Stop";
                data = rdr.getLimitToStopAllocation();
                val = 140000;
                break;
            case 5:
                name = "maxRest";
                data = rdr.getPercentMaxRestartAllocation();
                val = 50;
                break;
            case 6:
                name = "maxStop";
                data = rdr.getPercentMaxStopAllocation();
                val = 70;
                break;
            case 7:
                name = "recsRcvd";
                data = rdr.getRecordsReceived();
                val = recsRcvd;
                break;
            case 8:
                name = "stopsRcvd";
                data = rdr.getStopMessagesReceived();
                val = stopsRcvd;
                break;
            default:
                name = "unknown";
                data = null;
                val = Long.MAX_VALUE;
                break;
            }

            assertNotNull("Got null " + name + " array", data);
            assertEquals("Bad " + name + " length",
                         numReceiveChans, data.length);
            if (numReceiveChans > 0) {
                assertEquals("Bad " + name + "[0] value",
                             val, data[0].longValue());
            }
        }
    }
                                           
    int createServer(Selector sel)
        throws IOException
    {
        ServerSocketChannel ssChan = ServerSocketChannel.open();
        ssChan.configureBlocking(false);
        ssChan.socket().setReuseAddress(true);

        ssChan.socket().bind(null);

        ssChan.register(sel, SelectionKey.OP_ACCEPT);

        return ssChan.socket().getLocalPort();
    }

    private static final int harvestBuffers(SimpleReader tstRdr, int bufLen,
                                            IByteBufferCache bufMgr)
    {
        return harvestBuffers(tstRdr, bufLen, bufMgr, false, 0);
    }

    private static final int harvestBuffers(SimpleReader tstRdr, int bufLen,
                                            IByteBufferCache bufMgr,
                                            boolean checkId, int prevId)
    {
        if (!tstRdr.hasPayloads()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }

            return 0;
        }

        int numHarvested = 0;
        for (ByteBuffer buf : tstRdr.getPayloads()) {
            assertEquals("Bad payload length", bufLen, buf.getInt(0));
            assertEquals("Bad buffer position",
                         0, buf.position());
            assertEquals("Bad buffer position",
                         bufLen, buf.limit());

            if (checkId) {
                int newId = buf.getInt(4);
                assertEquals("Bad buffer ID", prevId + 1, newId);
                prevId = newId;
            }

            bufMgr.returnBuffer(buf);
            numHarvested++;
        }

        return numHarvested;
    }

    private static final void sendStopMsg(WritableByteChannel sinkChannel)
        throws IOException
    {
        if (stopMsg == null) {
            stopMsg = ByteBuffer.allocate(4);
            stopMsg.putInt(0, 4);
            stopMsg.limit(4);
        }

        stopMsg.position(0);
        sinkChannel.write(stopMsg);
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        tstRdr = null;

        sinkStopNotificationCalled = false;
        sinkErrorNotificationCalled = false;

        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new MockAppender(logLevel));
    }

    public static Test suite()
    {
        return new TestSuite(PayloadReaderTest.class);
    }

    protected void tearDown()
        throws Exception
    {
        if (tstRdr != null) {
            tstRdr.destroyProcessor();
        }

        super.tearDown();
    }

    public void testBasic()
        throws IOException
    {
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN * 20, BUFFER_LEN * 40,
                                "BasicCache");

        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();

        tstRdr = new SimpleReader("Basic");

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr, 256);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
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
            }

            recvCnt += harvestBuffers(tstRdr, bufLen, bufMgr);

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        tstRdr = new SimpleReader("StartStop");

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        tstRdr.forcedStopProcessing();

        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());

        // try it a second time
        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        tstRdr.forcedStopProcessing();

        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());

        tstRdr.destroyProcessor();

        waitUntilDestroyed(tstRdr);
        assertTrue("PayloadReader did not die after kill request",
                   tstRdr.isDestroyed());

        try {
            tstRdr.startProcessing();
            fail("PayloadReader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    public void testOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("OutputInput");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
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
            }

            recvCnt += harvestBuffers(tstRdr, bufLen, bufMgr);

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);
    }

    public void testMultiOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "MultiOutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("MultiOutputInput");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr, 1024);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after StartSig", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;
        final int groupSize = 3;

        final int numToSend = INPUT_OUTPUT_LOOP_CNT * groupSize;
        int id = 1;
        int recvId = 0;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < numToSend) {
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
            }

            int numBufs = harvestBuffers(tstRdr, bufLen, bufMgr, true, recvId);
            if (numBufs > 0) {
                recvId += numBufs;
                recvCnt += numBufs;
            }

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after stop", tstRdr.isStopped());
    }

    public void testMultiSizeOutputInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "MultiSize");

        tstRdr = new SimpleReader("MultiSize");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        // avoid a Sun race condition
        try {
            Thread.sleep(100);
        } catch (Exception ex) {
            // ignore interrupts
        }

        //for (int msgSize = 10; msgSize <= 13; msgSize++) {
        //    for (int bufLen = 32; bufLen <= 40; bufLen++) {
        for (int msgSize = 10; msgSize <= 13; msgSize++) {
            for (int bufLen = 32; bufLen <= 40; bufLen++) {
                // create a pipe for use in testing
                Pipe testPipe = Pipe.open();
                Pipe.SinkChannel sinkChannel = testPipe.sink();
                sinkChannel.configureBlocking(false);

                Pipe.SourceChannel sourceChannel = testPipe.source();
                sourceChannel.configureBlocking(false);

                tstRdr.addDataChannel(sourceChannel, bufMgr, bufLen);

                tstRdr.startProcessing();
                waitUntilRunning(tstRdr);
                assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                           ", not Running after startup (msgSize " + msgSize +
                           ", bufLen " + bufLen + ")", tstRdr.isRunning());

                assertEquals("There are acquired byte buffers before start" +
                             " (msgSize " + msgSize + ", bufLen " + bufLen +
                             ")", 0, bufMgr.getCurrentAquiredBuffers());

                // now move some buffers
                ByteBuffer testBuf;

                final int groupSize = 3;

                int id = 1;
                int recvId = 0;

                int xmitCnt = 0;
                int recvCnt = 0;
                int loopCnt = 0;
                while (recvCnt < INPUT_OUTPUT_LOOP_CNT * groupSize) {
                    if (xmitCnt < INPUT_OUTPUT_LOOP_CNT * groupSize) {
                        final int acquireLen = msgSize * groupSize;
                        testBuf = bufMgr.acquireBuffer(acquireLen);
                        assertNotNull("Unable to acquire transmit buffer on " +
                                      xmitCnt + " try.", testBuf);

                        for (int i = 0; i < groupSize; i++) {
                            final int start = msgSize * i;
                            testBuf.putInt(start, msgSize);
                            testBuf.putInt(start + 4, id++);
                        }
                        testBuf.limit(acquireLen);
                        testBuf.position(0);
                        sinkChannel.write(testBuf);
                        bufMgr.returnBuffer(testBuf);
                        xmitCnt += groupSize;
                    }

                    int numBufs = harvestBuffers(tstRdr, msgSize, bufMgr,
                                                 true, recvId);
                    if (numBufs > 0) {
                        recvId += numBufs;
                        recvCnt += numBufs;
                    }

                    loopCnt++;
                    if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                        fail("Received " + recvCnt + " payloads after " +
                             xmitCnt + " buffers were transmitted (msgSize " +
                             msgSize + ", bufLen " + bufLen + ")");
                    }
                }

                sendStopMsg(sinkChannel);

                waitUntilStopped(tstRdr);
                assertTrue("Failure on sendStopMsg command.",
                           sinkStopNotificationCalled);
                assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                           ", not Idle after stop", tstRdr.isStopped());

                assertEquals("There are still unreturned byte buffers",
                             0, bufMgr.getCurrentAquiredBuffers());
            }
        }
    }

    public void testDisposing()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutputInput");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("Disposing");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        for (int i = 0; i < 5; i++) {
            final int acquireLen = bufLen;
            testBuf = bufMgr.acquireBuffer(acquireLen);
            assertNotNull("Unable to acquire transmit buffer on try #" + i,
                          testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(0);
            sinkChannel.write(testBuf);
            bufMgr.returnBuffer(testBuf);

            if (i == 1) {
                tstRdr.startDisposing();
            }

            harvestBuffers(tstRdr, bufLen, bufMgr);
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);
    }

/*
    public void XXXtestOutputInputWithSemaphore()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "OutInSem");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("OutInSemaphore");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());


        InputChannel inChan =
            tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 1024;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (recvCnt < INPUT_OUTPUT_LOOP_CNT) {
            if (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
                testBuf = bufMgr.acquireBuffer(BUFFER_LEN);
                assertNotNull("Unable to acquire transmit buffer on " +
                              xmitCnt + " try.", testBuf);

                testBuf.putInt(0, bufLen);
                testBuf.limit(bufLen);
                testBuf.position(bufLen);
                testBuf.flip();

                sinkChannel.write(testBuf);
                bufMgr.returnBuffer(testBuf);

                xmitCnt++;
            }

            recvCnt += harvestEngine(tstRdr, inChan, bufLen);

            loopCnt++;
            if (loopCnt == recvCnt + INPUT_OUTPUT_LOOP_CNT) {
                fail("Received " + recvCnt + " payloads after " + xmitCnt +
                     " buffers were transmitted");
            }
        }

        sendStopMsg(sinkChannel);

        Thread.sleep(100);

        assertTrue("Failure on sendStopMsg command.",
                   sinkStopNotificationCalled);

        assertEquals("Failure on sendStopMsg command.",
                     0, tstRdr.inputAvailable.permits());
    }
*/

/*
    public void XXXtestSimulatedError()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "SimError");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("SimError");
        tstRdr.registerComponentObserver(this);

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        final int acquireLen = bufLen;
        testBuf = bufMgr.acquireBuffer(acquireLen);
        assertNotNull("Unable to acquire transmit buffer", testBuf);

        testBuf.putInt(0, bufLen);
        testBuf.limit(bufLen);
        testBuf.position(bufLen);
        testBuf.flip();

        tstRdr.injectError();
        sinkChannel.write(testBuf);
        bufMgr.returnBuffer(testBuf);
        for (int i = 0; !tstRdr.isError() && i < 10; i++) {
            Thread.sleep(100);
        }

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Error after ErrorSig", tstRdr.isError());
        Thread.sleep(100);

        assertTrue("Error notification was not received.",
                   sinkErrorNotificationCalled);

        sendStopMsg(sinkChannel);

        Thread.sleep(100);
        assertFalse("Sink stop notification was received.",
                    sinkStopNotificationCalled);
    }
*/

    public void testGetters()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "Getters");

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        Pipe.SinkChannel sinkChannel = testPipe.sink();
        sinkChannel.configureBlocking(false);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        sourceChannel.configureBlocking(false);

        tstRdr = new SimpleReader("Getters");
        tstRdr.registerComponentObserver(this);

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addDataChannel(sourceChannel, bufMgr);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        checkGetters(tstRdr, 1, 0, 0, 0, 0, 0);

        // now move some buffers
        ByteBuffer testBuf;

        final int bufLen = 64;

        final int acquireLen = bufLen;
        //testBuf = bufMgr.acquireBuffer(acquireLen);
        testBuf = ByteBuffer.allocateDirect(acquireLen);
        assertNotNull("Unable to acquire transmit buffer", testBuf);

        testBuf.putInt(0, bufLen);
        testBuf.limit(bufLen);
        testBuf.position(bufLen);
        testBuf.flip();

        sinkChannel.write(testBuf);
        bufMgr.returnBuffer(testBuf);
        for (int i = 0; !tstRdr.isError() && i < 10; i++) {
            Thread.sleep(100);
        }

        checkGetters(tstRdr, 1, 1, BUFFER_LEN, bufLen, 1, 0);

        assertFalse("PayloadReader in Error state after ErrorSig",
                   tstRdr.isError());
        assertFalse("Error notification was received.",
                   sinkErrorNotificationCalled);

        sendStopMsg(sinkChannel);

        for (int i = 0; i < 5 && !tstRdr.isStopped(); i++) {
            Thread.sleep(100);
        }

        assertTrue("Sink stop notification was not received.",
                   sinkStopNotificationCalled);

        checkGetters(tstRdr, 0, 0, BUFFER_LEN, bufLen, 1, 1);
    }

    /**
     * Test starting and stopping server version of input tstRdr.
     */
    public void testInetServer()
        throws Exception
    {
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "InetServer");

        tstRdr = new SimpleReader("InetServer");

        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.startServer(bufMgr);

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after server start", tstRdr.isStopped());

        InetSocketAddress addr =
            new InetSocketAddress("localhost", tstRdr.getServerPort());

        SocketChannel chan = SocketChannel.open(addr);

        Thread.sleep(100);

        // this socket will die in the middle of the show
        SocketChannel redShirt = SocketChannel.open(addr);

        Thread.sleep(100);

        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        ByteBuffer testBuf;

        final int bufLen = 64;

        int xmitCnt = 0;
        int recvCnt = 0;
        int loopCnt = 0;
        while (xmitCnt < INPUT_OUTPUT_LOOP_CNT) {
            final int acquireLen = bufLen;
            testBuf = bufMgr.acquireBuffer(acquireLen);
            assertNotNull("Unable to acquire transmit buffer on " +
                          xmitCnt + " try.", testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);
            if (xmitCnt < 2) {
                testBuf.position(0);
                redShirt.write(testBuf);
            } else {
                redShirt.close();
            }

            bufMgr.returnBuffer(testBuf);

            xmitCnt++;
            Thread.sleep(100);

            // XXX should check recvCnt
        }

        tstRdr.forcedStopProcessing();
        Thread.sleep(100);

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());

        // try it a second time
        tstRdr.startProcessing();
        waitUntilRunning(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Running after startup", tstRdr.isRunning());

        tstRdr.forcedStopProcessing();
        Thread.sleep(100);

        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after StopSig", tstRdr.isStopped());


        tstRdr.destroyProcessor();

        waitUntilDestroyed(tstRdr);
        assertTrue("PayloadReader did not die after kill request",
                   tstRdr.isDestroyed());

        try {
            tstRdr.startProcessing();
            fail("PayloadReader restart after kill succeeded");
        } catch (Error e) {
            // expect this to fail
        }
    }

    /**
     * Test multiple input engine servers.
     */
    public void testMultiServer()
        throws Exception
    {
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "MultiServer");

        final int numTstRdrs = 4;

        PayloadReader[] tstRdrs = new SimpleReader[numTstRdrs];

        // create a bunch of engines
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i] = new SimpleReader("MultiServer");

            tstRdrs[i].start();
            waitUntilStopped(tstRdrs[i]);
            assertTrue("PayloadReader in " + tstRdrs[i].getPresentState() +
                       ", not Idle after creation", tstRdrs[i].isStopped());
        }

        // start all the servers
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].startServer(bufMgr);

            assertTrue("PayloadReader in " + tstRdrs[i].getPresentState() +
                   ", not Idle after server start", tstRdrs[i].isStopped());
        }

        SocketChannel[] chans = new SocketChannel[numTstRdrs];

        // open a channel to each engine
        for (int i = 0; i < numTstRdrs; i++) {
            InetSocketAddress addr =
                new InetSocketAddress("localhost", tstRdrs[i].getServerPort());
            chans[i] = SocketChannel.open(addr);
        }

        Thread.sleep(100);

        // gentlemen, start your engines
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].startProcessing();
        }

        Thread.sleep(100);

        // make sure they're all running
        for (int i = 0; i < numTstRdrs; i++) {
            waitUntilRunning(tstRdrs[i]);
            assertTrue("PayloadReader in " + tstRdrs[i].getPresentState() +
                       ", not Running after server start",
                       tstRdrs[i].isRunning());
        }

        ByteBuffer testBuf;

        final int bufLen = 64;

        for (int i = 0; i < numTstRdrs; i++) {
            testBuf = bufMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire transmit buffer#" + i, testBuf);

            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chans[i].write(testBuf);
            bufMgr.returnBuffer(testBuf);
        }

        Thread.sleep(100);

        boolean gotAll = false;

        // wait until we've got data on all channels
        final int numTries = 5;
        for (int i = 0; !gotAll && i < numTries; i++) {
            boolean rcvdData = true;
            for (int j = 0; rcvdData && j < numTstRdrs; j++) {
                Long[] rcvd = tstRdrs[j].getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine#" + j,
                              rcvd);
                assertEquals("Unexpected number of connections for tstRdr#" + j,
                             1, rcvd.length);
                if (rcvd[0].longValue() < bufLen) {
                    rcvdData = false;
                }
            }

            // if we've got data in all engines, we're done
            if (rcvdData) {
                gotAll = true;
            } else {
                Thread.sleep(100);
            }
        }

        // stop everything
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].forcedStopProcessing();
        }

        // destroy everything
        for (int i = 0; i < numTstRdrs; i++) {
            tstRdrs[i].destroyProcessor();
        }
        for (int i = 0; i < numTstRdrs; i++) {
            waitUntilDestroyed(tstRdrs[i]);
        }
        for (int i = 0; i < numTstRdrs; i++) {
            assertTrue("PayloadReader#" + i +
                       " did not die after kill request",
                       tstRdrs[i].isDestroyed());
        }
    }

    public void testServerInput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache bufMgr =
            new ByteBufferCache(BUFFER_LEN, BUFFER_LEN*20,
                                BUFFER_LEN*40, "ServerInput");

        Selector sel = Selector.open();

        int port = createServer(sel);

        tstRdr = new SimpleReader("ServerInput");
        tstRdr.start();
        waitUntilStopped(tstRdr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after creation", tstRdr.isStopped());

        tstRdr.addReverseConnection("localhost", port, bufMgr);
        assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                   ", not Idle after start", tstRdr.isStopped());

        ByteBuffer testBuf;

        for (int i = 0; i < 2; i++) {
            tstRdr.startProcessing();
            waitUntilRunning(tstRdr);
            assertTrue("PayloadReader in " + tstRdr.getPresentState() +
                       ", not Running after StartSig", tstRdr.isRunning());

            SocketChannel chan = acceptChannel(sel);

            final int bufLen = 40;

            testBuf = bufMgr.acquireBuffer(bufLen);
            testBuf.putInt(0, bufLen);
            testBuf.limit(bufLen);
            testBuf.position(bufLen);
            testBuf.flip();

            chan.write(testBuf);
            bufMgr.returnBuffer(testBuf);

            // wait until we've got data on all channels
            boolean gotAll = false;
            final int numTries = 5;
            for (int t = 0; !gotAll && t < numTries; t++) {
                boolean rcvdData = true;
                Long[] rcvd = tstRdr.getBytesReceived();
                assertNotNull("Got null byteRcvd array from engine", rcvd);
                assertEquals("Unexpected number of connections for engine",
                             1, rcvd.length);
                if (rcvd[0].longValue() < bufLen) {
                    rcvdData = false;
                }

                // if we've got data, we're done
                if (rcvdData) {
                    gotAll = true;
                } else {
                    Thread.sleep(100);
                }
            }

            Long[] totRcvd = tstRdr.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", totRcvd);
            assertEquals("Unexpected number of connections for engine",
                         1, totRcvd.length);
            assertEquals("Bad number of bytes", bufLen, totRcvd[0].longValue());

            testBuf = bufMgr.acquireBuffer(4);
            assertNotNull("Unable to acquire stop buffer", testBuf);

            testBuf.putInt(4);
            testBuf.limit(4);
            testBuf.position(4);
            testBuf.flip();

            chan.write(testBuf);
            bufMgr.returnBuffer(testBuf);

            int reps = 0;
            while (reps < 10 && !tstRdr.isStopped()) {
                Thread.sleep(10);
                reps++;
            }
            if (reps >= 10) {
                fail("Engine did not stop after receiving stop msg");
            }

            // make sure receive engines have been detatched
            Long[] postRcvd = tstRdr.getBytesReceived();
            assertNotNull("Got null byteRcvd array from engine", postRcvd);
            assertEquals("Unexpected number of connections for engine",
                         0, postRcvd.length);
        }

        tstRdr.destroyProcessor();
        waitUntilDestroyed(tstRdr);
        assertTrue("PayloadReader did not die after kill request",
                   tstRdr.isDestroyed());
    }

    public synchronized void update(Object object, String notificationID)
    {
        if (object instanceof NormalState){
            NormalState state = (NormalState)object;
            if (state == NormalState.STOPPED){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkStopNotificationCalled = true;
                } else {
                    throw new Error("Unexpected notification update");
                }
            }
        } else if (object instanceof ErrorState){
            ErrorState state = (ErrorState)object;
            if (state == ErrorState.UNKNOWN_ERROR){
                if (notificationID.equals(DAQCmdInterface.SINK)){
                    sinkErrorNotificationCalled = true;
                } else {
                    throw new Error("Unexpected notification update");
                }
            }
        }
    }

    private static final void waitUntilDestroyed(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isDestroyed(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    private static final void waitUntilRunning(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isRunning(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    private static final void waitUntilStopped(PayloadReader rdr)
    {
        for (int i = 0; i < 5 && !rdr.isStopped(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
