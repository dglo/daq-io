package icecube.daq.io.test;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.DAQComponentObserver;
import icecube.daq.io.ErrorState;
import icecube.daq.io.NormalState;
import icecube.daq.io.OutputChannel;
import icecube.daq.io.PayloadTransmitChannel;
import icecube.daq.io.SingleOutputEngine;

import icecube.daq.io.test.IOTestUtil;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockWriteableChannel;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.VitreousBufferCache;

import java.io.IOException;

import java.net.BindException;
import java.net.InetSocketAddress;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import java.util.Iterator;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

/**
 * SingleOutputEngine unit tests.
 */
public class SingleOutputEngineTest
    extends LoggingCase
{
    class Observer
        implements DAQComponentObserver
    {
        private String sinkNotificationId;
        private boolean sinkStopNotificationCalled;
        private boolean sinkErrorNotificationCalled;

        private String sourceNotificationId;
        private boolean sourceStopNotificationCalled;
        private boolean sourceErrorNotificationCalled;

        boolean gotSinkError()
        {
            return sinkErrorNotificationCalled;
        }

        boolean gotSinkStop()
        {
            return sinkStopNotificationCalled;
        }

        boolean gotSourceError()
        {
            return sourceErrorNotificationCalled;
        }

        boolean gotSourceStop()
        {
            return sourceStopNotificationCalled;
        }

        void setSinkNotificationId(String id)
        {
            sinkNotificationId = id;
        }

        void setSourceNotificationId(String id)
        {
            sourceNotificationId = id;
        }

        public synchronized void update(Object object, String notificationId)
        {
            if (object instanceof NormalState) {
                NormalState state = (NormalState)object;
                if (state == NormalState.STOPPED) {
                    if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                        notificationId.equals(sourceNotificationId))
                    {
                        sourceStopNotificationCalled = true;
                    } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                               notificationId.equals(sinkNotificationId))
                    {
                        sinkStopNotificationCalled = true;
                    } else {
                        throw new Error("Unexpected stop notification \"" +
                                        notificationId + "\"");
                    }
                } else {
                    throw new Error("Unexpected notification state " +
                                    state);
                }
            } else if (object instanceof ErrorState) {
                ErrorState state = (ErrorState)object;
                if (state == ErrorState.UNKNOWN_ERROR) {
                    if (notificationId.equals(DAQCmdInterface.SOURCE) ||
                        notificationId.equals(sourceNotificationId))
                    {
                        sourceErrorNotificationCalled = true;
                    } else if (notificationId.equals(DAQCmdInterface.SINK) ||
                               notificationId.equals(sinkNotificationId))
                    {
                        sinkErrorNotificationCalled = true;
                    } else {
                        throw new Error("Unexpected error notification \"" +
                                        notificationId + "\"");
                    }
                } else {
                    throw new Error("Unexpected notification state " +
                                    state);
                }
            } else {
                throw new Error("Unexpected notification object " +
                                object.getClass().getName());
            }
        }
    }

    // private static member data
    private static final int BUFFER_BLEN = 32000;
    private static final String SRC_NOTIFICATION_ID = "SourceID";
    private static final String SRC_ERROR_ID = "SourceErrorID";

    /**
     * The object being tested.
     */
    private SingleOutputEngine engine;

    /**
     * Constructs and instance of this test.
     *
     * @param name the name of the test.
     */
    public SingleOutputEngineTest(String name)
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

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite() {
        return new TestSuite(SingleOutputEngineTest.class);
    }

    /**
     * Tears down the fixture, for example, close a network connection. This
     * method is called after a test is executed.
     *
     * @throws Exception if super class tearDown fails.
     */
    protected void tearDown()
        throws Exception
    {
        if (engine != null) {
            if (!engine.isDestroyed()) {
                if (engine.isError()) {
                    engine.clearError();
                }
                if (!engine.isStopped()) {
                    engine.forcedStopProcessing();
                }

                engine.destroyProcessor();
            }

            engine = null;
        }

        super.tearDown();
    }

    /**
     * Test starting and stopping engine.
     */
    public void testStartStop()
        throws Exception
    {
        engine = new SingleOutputEngine("StartStop", 0, "test");
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        engine.addDataChannel(new MockWriteableChannel(), null);
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // try it a second time
        engine.addDataChannel(new MockWriteableChannel(), null);
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.forcedStopProcessing();
        IOTestUtil.waitUntilStopped(engine, "forced stop");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        // now try a stop message
        engine.addDataChannel(new MockWriteableChannel(), null);
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.sendLastAndStop();
        IOTestUtil.waitUntilStopped(engine, "send last");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        engine.destroyProcessor();
        IOTestUtil.waitUntilDestroyed(engine);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        try {
            engine.startProcessing();
            fail("SingleOutputEngine restart after kill succeeded");
        } catch (Exception e) {
            // expect this to fail
        }

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());
    }

    public void testOutputLoop()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        // create a pipe for use in testing
        Pipe testPipe = Pipe.open();
        testPipe.sink().configureBlocking(false);
        testPipe.source().configureBlocking(true);

        Observer observer = new Observer();

        engine = new SingleOutputEngine("OutputLoop", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        OutputChannel transmitEng =
            engine.addDataChannel(testPipe.sink(), cacheMgr);

        assertTrue("SingleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        final int bufLen = 4096;

        // now move some buffers
        ByteBuffer testInBuf = ByteBuffer.allocate(bufLen * 2);

        ByteBuffer testOutBuf;
        for (int i = 0; i < 100; i++) {
            testOutBuf = cacheMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire buffer#" + i, testOutBuf);
            testOutBuf.putInt(0, bufLen);
            testOutBuf.limit(bufLen);
            testOutBuf.position(bufLen);
            testOutBuf.flip();

            transmitEng.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 10; j++) {
                if (!transmitEng.isOutputQueued()) {
                    break;
                }

                Thread.sleep(100);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                        transmitEng.isOutputQueued());

            testInBuf.position(0);
            testInBuf.limit(4);
            int numBytes = testPipe.source().read(testInBuf);
            assertEquals("Bad return count on read#" + i,
                         4, numBytes);
            assertEquals("Bad message byte count on read#" + i,
                         bufLen, testInBuf.getInt(0));

            testInBuf.limit(bufLen);
            int remBytes = testPipe.source().read(testInBuf);
            assertEquals("Bad remainder count on read#" + i,
                         bufLen - 4, remBytes);
        }

        engine.sendLastAndStop();
        Thread.sleep(10);
        transmitEng.flushOutQueue();

        IOTestUtil.waitUntilStopped(engine, "send last");

        assertTrue("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sourceStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
    }

    public void testServerDisconnect()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        Selector sel = Selector.open();

        int port = createServer(sel);

        //Observer observer = new Observer();

        engine = new SingleOutputEngine("ServerDisconnect", 0, "test");
        //engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        OutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);
        assertTrue("SingleOutputEngine is not connected", engine.isConnected());
        assertTrue("SingleOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        SocketChannel chan = acceptChannel(sel);

        assertTrue("SingleOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());

        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        engine.disconnect();
        assertTrue("SingleOutputEngine is still connected",
                   !engine.isConnected());
        assertTrue("SingleOutputEngine in " + engine.getPresentState() +
                   ", not Idle", engine.isStopped());
    }

    public void testServerOutput()
        throws Exception
    {
        // buffer caching manager
        IByteBufferCache cacheMgr = new VitreousBufferCache();

        Selector sel = Selector.open();

        int port = createServer(sel);

        Observer observer = new Observer();

        engine = new SingleOutputEngine("ServerOutput", 0, "test");
        engine.registerComponentObserver(observer);
        engine.start();
        IOTestUtil.waitUntilStopped(engine, "creation");

        SocketChannel sock =
            SocketChannel.open(new InetSocketAddress("localhost", port));
        sock.configureBlocking(false);

        assertEquals("Bad number of log messages",
                     0, getNumberOfMessages());

        OutputChannel transmitEng = engine.connect(cacheMgr, sock, 1);

        SocketChannel chan = acceptChannel(sel);

        assertTrue("SingleOutputEngine in " + engine.getPresentState() +
                   ", not Idle after StopSig", engine.isStopped());
        engine.startProcessing();
        IOTestUtil.waitUntilRunning(engine);

        final int bufLen = 40;

        // now move some buffers
        ByteBuffer testInBuf = ByteBuffer.allocate(bufLen * 2);

        ByteBuffer testOutBuf;
        for (int i = 0; i < 100; i++) {
            testOutBuf = cacheMgr.acquireBuffer(bufLen);
            assertNotNull("Unable to acquire buffer#" + i, testOutBuf);
            testOutBuf.putInt(0, bufLen);
            testOutBuf.limit(bufLen);
            testOutBuf.position(bufLen);
            testOutBuf.flip();

            transmitEng.receiveByteBuffer(testOutBuf);
            for (int j = 0; j < 10; j++) {
                if (!transmitEng.isOutputQueued()) {
                    break;
                }

                Thread.sleep(100);
            }
            assertFalse("PayloadTransmitChannel did not send buf#" + i,
                       transmitEng.isOutputQueued());

            testInBuf.position(0);
            testInBuf.limit(4);
            int numBytes = chan.read(testInBuf);
            assertEquals("Bad return count on read#" + i,
                         4, numBytes);
            assertEquals("Bad message byte count on read#" + i,
                         bufLen, testInBuf.getInt(0));

            testInBuf.limit(bufLen);
            int remBytes = chan.read(testInBuf);
            assertEquals("Bad remainder count on read#" + i,
                         bufLen - 4, remBytes);
        }

        engine.sendLastAndStop();
        transmitEng.flushOutQueue();
        IOTestUtil.waitUntilStopped(engine, "send last");

        assertTrue("Failure on sendLastAndStop command.",
                   observer.gotSourceStop());
        assertFalse("Got sourceStop notification",
                   observer.gotSinkStop());
        assertFalse("Got sinkError notification",
                   observer.gotSinkError());
        assertFalse("Got sourceError notification",
                   observer.gotSourceError());

        assertTrue("ByteBufferCache is not balanced", cacheMgr.isBalanced());
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
