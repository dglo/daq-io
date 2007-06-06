package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.SelectableChannel;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class PushPayloadReader
    extends PayloadReader
{
    class PushInputChannel
        extends InputChannel
    {
        private PushPayloadReader reader;

        PushInputChannel(InputChannelParent parent, SelectableChannel channel,
                         IByteBufferCache bufMgr, int bufSize)
            throws IOException
        {
            super(parent, channel, bufMgr, bufSize);

            reader = (PushPayloadReader) parent;
        }

        void notifyOnStop()
        {
            reader.channelStopped(this);
            super.notifyOnStop();
        }

        public void pushPayload(ByteBuffer payBuf)
            throws IOException
        {
            dequeuedMessages++;
            reader.pushBuffer(payBuf);
        }
    }

    private static final Log LOG = LogFactory.getLog(PushPayloadReader.class);

    private ArrayList<InputChannel> pushChanList =
        new ArrayList<InputChannel>();

    private long dequeuedMessages;
    private long stopMessagesPropagated;
    private long totStops;

    // default maximum size of strand queue
    public PushPayloadReader(String name)
        throws IOException
    {
        super(name);
    }

    void channelStopped(InputChannel chan)
    {
        pushChanList.remove(chan);
        if (pushChanList.size() == 0) {
            sendStop();
            stopMessagesPropagated++;
            totStops++;
        }
    }

    public InputChannel createChannel(SelectableChannel channel,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        if (pushChanList.size() == 0) {
            dequeuedMessages = 0;
            stopMessagesPropagated = 0;
        }

        InputChannel chan =
            new PushInputChannel(this, channel, bufMgr, bufSize);
        pushChanList.add(chan);
        return chan;
    }

    /**
     * Get the number of messages received during this run.
     *
     * @return number of messages received
     */
    public long getDequeuedMessages()
    {
        return dequeuedMessages;
    }

    /**
     * Get the number of stop messages passed on to other objects
     * during this run.
     *
     * @return total number of stop messages
     */
    public long getStopMessagesPropagated()
    {
        return stopMessagesPropagated;
    }

    /**
     * Get the total number of stop messages received.
     *
     * @return total number of stop messages
     */
    public long getTotalStopsReceived()
    {
        return totStops;
    }

    public abstract void pushBuffer(ByteBuffer bb)
        throws IOException;

    public abstract void sendStop();
}
