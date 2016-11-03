package icecube.daq.io;

import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.io.test.MockWriteableChannel;
import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests BufferedWritableChannel.java
 */
public class BufferedWritableChannelTest
{

    private MockBufferCache mockCache;
    private MockWriteableChannel mockTarget;
    private BufferedWritableChannel subject;

//    private int bufferSize = 1099;
    private int bufferSize = 10;

    @Before
    public void setUp()
            throws Exception
    {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        mockCache = new MockBufferCache("test");
        mockTarget = new MockWriteableChannel();
        subject = new BufferedWritableChannel(mockCache, mockTarget,
                bufferSize);
    }


    @Test
    public void testWriteWithAutoFlush() throws IOException
    {
        ///
        /// Tests that writes are buffered until the buffer
        /// is full+1
        ///

        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(0, mockTarget.getBytesWritten());

        // should buffer
        for(int written=1; written<=bufferSize; written++)
        {
            ByteBuffer msg = mockCache.acquireBuffer(1);
            subject.write(msg);
            assertEquals(written, mockCache.getCurrentAquiredBuffers());
            assertEquals(0, mockTarget.getBytesWritten());
            assertEquals(written, subject.bufferedMessages());
            assertEquals(written, subject.bufferedBytes());
        }

        // auto-flush
        ByteBuffer msg = mockCache.acquireBuffer(1);
        subject.write(msg);
        assertEquals(1, mockCache.getCurrentAquiredBuffers());
        assertEquals(bufferSize, mockTarget.getBytesWritten());
        assertEquals(1, subject.bufferedMessages());
        assertEquals(1, subject.bufferedBytes());

    }

    @Test
    public void testWriteWithManualFlush() throws IOException
    {
        ///
        /// Tests that writes are buffered until flush is called
        ///

        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(0, mockTarget.getBytesWritten());

        // should buffer
        for(int written=1; written<=bufferSize; written++)
        {
            ByteBuffer msg = mockCache.acquireBuffer(1);
            subject.write(msg);
            assertEquals(written, mockCache.getCurrentAquiredBuffers());
            assertEquals(0, mockTarget.getBytesWritten());
            assertEquals(written, subject.bufferedMessages());
            assertEquals(written, subject.bufferedBytes());
        }

        // manual-flush
        subject.flush();
        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(bufferSize, mockTarget.getBytesWritten());
        assertEquals(0, subject.bufferedMessages());
        assertEquals(0, subject.bufferedBytes());

    }


    @Test
    public void testMessageLargerThanBuffer() throws IOException
    {
        ///
        /// Tests messages larger than the buffer result in a flush
        /// and direct write.
        ///

        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(0, mockTarget.getBytesWritten());

        // should buffer
        for(int written=1; written<=bufferSize; written++)
        {
            ByteBuffer msg = mockCache.acquireBuffer(1);
            subject.write(msg);
            assertEquals(written, mockCache.getCurrentAquiredBuffers());
            assertEquals(0, mockTarget.getBytesWritten());
            assertEquals(written, subject.bufferedMessages());
            assertEquals(written, subject.bufferedBytes());
        }

        // large message should flush and write
        int large = bufferSize + 1;
        ByteBuffer msg = mockCache.acquireBuffer(large);
        subject.write(msg);
        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(bufferSize + large, mockTarget.getBytesWritten());
        assertEquals(0, subject.bufferedMessages());
        assertEquals(0, subject.bufferedBytes());

    }


    @Test
    public void testIsOpen()
    {
        // isOpen() should delegate to target.  In this case
        // the mock target generates an error
        try
        {
            subject.isOpen();
            fail("Exception expected");
        }
        catch (Error error)
        {
            assertEquals("Unimplemented", error.getMessage());
        }
    }


    @Test
    public void testClose() throws IOException
    {
        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(0, mockTarget.getBytesWritten());

        // should buffer
        for(int written=1; written<=bufferSize; written++)
        {
            ByteBuffer msg = mockCache.acquireBuffer(1);
            subject.write(msg);
            assertEquals(written, mockCache.getCurrentAquiredBuffers());
            assertEquals(0, mockTarget.getBytesWritten());
            assertEquals(written, subject.bufferedMessages());
            assertEquals(written, subject.bufferedBytes());
        }

        // close should flush
        subject.close();

        assertEquals(0, mockCache.getCurrentAquiredBuffers());
        assertEquals(bufferSize, mockTarget.getBytesWritten());
        assertEquals(0, subject.bufferedMessages());
        assertEquals(0, subject.bufferedBytes());
    }


}
