package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;

public abstract class PushStreamReader
    extends DAQStreamReader
{
    class PushInputChannel
        extends InputChannel
    {
        private PushStreamReader reader;

        PushInputChannel(IOChannelParent parent, SelectableChannel channel,
                         String name, IByteBufferCache bufMgr, int bufSize)
            throws IOException
        {
            super(parent, channel, name, bufMgr, bufSize);

            reader = (PushStreamReader) parent;
        }

        @Override
        public void notifyOnStop()
        {
            reader.channelStopped(this);
            super.notifyOnStop();
        }

        @Override
        public void pushPayload(ByteBuffer payBuf)
            throws IOException
        {
            dequeuedMessages++;
            reader.pushBuffer(payBuf);
        }

        /**
         * Unimplemented.
         *
         * @param compObserver component observer
         * @param notificationID ID string
         */
        @Override
        public void registerComponentObserver(DAQComponentObserver compObserver,
                                              String notificationID)
        {
            throw new Error("Unimplemented");
        }
    }

    private ArrayList<InputChannel> pushChanList =
        new ArrayList<InputChannel>();

    private long dequeuedMessages;
    private long stopMessagesPropagated;
    private long totStops;

    // default maximum size of strand queue
    public PushStreamReader(String name)
        throws IOException
    {
        super(name);
    }

    @Override
    public void channelStopped(IOChannel chan)
    {
        pushChanList.remove(chan);
        if (pushChanList.size() == 0) {
            sendStop();
            stopMessagesPropagated++;
            totStops++;
        }

        super.channelStopped(chan);
    }

    @Override
    public InputChannel createChannel(SelectableChannel channel, String name,
                                      IByteBufferCache bufMgr, int bufSize)
        throws IOException
    {
        if (pushChanList.size() == 0) {
            dequeuedMessages = 0;
            stopMessagesPropagated = 0;
        }

        InputChannel chan =
            new PushInputChannel(this, channel, name, bufMgr, bufSize);
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
