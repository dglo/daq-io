package icecube.daq.io;

import icecube.daq.io.test.LoggingCase;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.VitreousBufferCache;

import icecube.daq.payload.impl.UTCTime8B;

import icecube.daq.splicer.ClosedStrandException;
import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.StrandTail;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;

import java.util.List;

import java.util.zip.DataFormatException;

import junit.framework.Test;
import junit.framework.TestSuite;

import junit.textui.TestRunner;

public class SpliceableInputChannelTest
    extends LoggingCase
{
    class MockParent
        implements InputChannelParent
    {
        public void channelStopped() { }
    }

    class MockStrandTail
        implements StrandTail
    {
        public void close()
        {
            throw new Error("Unimplemented");
        }

        public Spliceable head()
        {
            throw new Error("Unimplemented");
        }

        public boolean isClosed()
        {
            throw new Error("Unimplemented");
        }

        public StrandTail push(List spliceables)
            throws OrderingException, ClosedStrandException
        {
            throw new Error("Unimplemented");
        }

        public StrandTail push(Spliceable spliceable)
            throws OrderingException, ClosedStrandException
        {
            throw new Error("Unimplemented");
        }

        public int size()
        {
            throw new Error("Unimplemented");
        }
    }

    class UnpushableStrandTail
        extends MockStrandTail
    {
        private boolean throwOrderingEx;

        UnpushableStrandTail(boolean throwOrderingEx)
        {
            this.throwOrderingEx = throwOrderingEx;
        }

        public StrandTail push(Spliceable spliceable)
            throws OrderingException, ClosedStrandException
        {
            if (throwOrderingEx) {
                throw new OrderingException("Test exception");
            }

            throw new ClosedStrandException("Test exception");
        }
    }

    public class MockSpliceable
        implements ILoadablePayload, Spliceable
    {
        private IByteBufferCache bufMgr;
        private ByteBuffer buf;
        private int len;
        private int type;
        private long time;
        private IUTCTime timeObj;

        public MockSpliceable(IByteBufferCache bufMgr, ByteBuffer buf)
        {
            this.bufMgr = bufMgr;
            this.buf = buf;

            len = buf.getInt(0);
            type = buf.getInt(4);
            time = buf.getLong(8);
        }

        public int compareTo(Object obj)
        {
            throw new Error("Unimplemented");
        }

        public Object deepCopy()
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

        public IUTCTime getPayloadTimeUTC()
        {
            if (timeObj == null) {
                timeObj = new UTCTime8B(time);
            }

            return timeObj;
        }

        public int getPayloadType()
        {
            return type;
        }

        public void loadPayload()
            throws IOException, DataFormatException
        {
            throw new IOException("Unimplemented");
        }

        public void recycle()
        {
            bufMgr.returnBuffer(buf);
        }
    }

    public class MockFactory
        implements SpliceableFactory
    {
        private IByteBufferCache bufMgr;

        public MockFactory(IByteBufferCache bufMgr)
        {
            this.bufMgr = bufMgr;
        }

        public void backingBufferShift(List list, int index, int shift)
        {
            throw new Error("Unimplemented");
        }

        public Spliceable createCurrentPlaceSpliceable()
        {
            throw new Error("Unimplemented");
        }

        public Spliceable createSpliceable(ByteBuffer buf)
        {
            return new MockSpliceable(bufMgr, buf);
        }

        public void invalidateSpliceables(List list)
        {
            throw new Error("Unimplemented");
        }

        public boolean skipSpliceable(ByteBuffer buf)
        {
            throw new Error("Unimplemented");
        }
    }

    /**
     * Constructs an instance of this test.
     *
     * @param name the name of the test.
     */
    public SpliceableInputChannelTest(String name)
    {
        super(name);
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(SpliceableInputChannelTest.class);
    }

    public void testOutOfOrderRecycle()
        throws IOException
    {
        MockParent parent = new MockParent();

        Pipe pipe = Pipe.open();

        IByteBufferCache bufMgr = new VitreousBufferCache();

        MockFactory factory = new MockFactory(bufMgr);

        SpliceableInputChannel chan =
            new SpliceableInputChannel(parent, pipe.source(), bufMgr, 256,
                                       factory);

        final int type = 666;
        final long time = 123456L;

        for (int i = 0; i < 2; i++) {
            final long expBytes = bufMgr.getCurrentAquiredBytes();

            ByteBuffer buf = bufMgr.acquireBuffer(16);
            buf.putInt(buf.capacity());
            buf.putInt(type);
            buf.putLong(time);

            chan.setStrandTail(new UnpushableStrandTail(i == 0));

            assertEquals("Unexpected log message", 0, getNumberOfMessages());

            chan.pushPayload(buf);

            assertEquals("Buffer cache memory leak",
                         expBytes, bufMgr.getCurrentAquiredBytes());

            assertEquals("Expected log message", 1, getNumberOfMessages());
            assertEquals("Bad log message",
                         "Couldn't push payload type " + type + ", length " +
                         buf.capacity() + ", time " + time + "; recycling",
                         getMessage(0));
            clearMessages();
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
