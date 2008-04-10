/*
 * class: SourceIdPayloadOutputEngine
 *
 * Version $Id: SourceIdPayloadOutputEngine.java 2894 2008-04-10 18:57:32Z dglo $
 *
 * Date: May 23 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.impl.SourceID4B;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;

/**
 * This class ...does what?
 *
 * @author mcp
 * @version $Id: SourceIdPayloadOutputEngine.java 2894 2008-04-10 18:57:32Z dglo $
 */
public class SourceIdPayloadOutputEngine
    extends PayloadOutputEngine
    implements DAQComponentOutputProcess
{

    private IByteBufferCache bufMgr;
    private HashMap idRegistry = new HashMap();
    private long messagesSent = 0;

    public SourceIdPayloadOutputEngine(String type,
                                       int id,
                                       String fcn) {
        // parent constructor wants same args
        super(type, id, fcn);
    }

    // instance member method (alphabetic)
    // keep reference to buffer manager so that we can return buffers
    // after transmits  Note: this interface is needed because the buffer
    // manager is created in the SystemTestPayloadInputEngine class and
    // is not available to the parent class when creating this class
    public void registerBufferManager(IByteBufferCache manager) {
        bufMgr = manager;
    }

    // get reference to buffer manager
    public IByteBufferCache getBufferManager() {
        return bufMgr;
    }

    public QueuedOutputChannel connect(IByteBufferCache bufCache,
                                       WritableByteChannel chan, int srcId)
        throws IOException
    {
        return addDataChannel(chan, new SourceID4B(srcId));
    }

    public QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                              ISourceID sourceID)
    {
        // ask payloadOutputEngine to make us a payloadTransmitEngine
        QueuedOutputChannel eng = super.addDataChannel(channel, bufMgr);
        // register it locally so that we can find it when we need it
        idRegistry.put(new Integer(sourceID.getSourceID()), eng);
        return eng;
    }

    public QueuedOutputChannel lookUpEngineBySourceID(ISourceID id) {
        Integer realID = new Integer(id.getSourceID());
        if (idRegistry.containsKey(realID)) {
            return (QueuedOutputChannel) idRegistry.get(realID);
        } else {
            return null;
        }
    }

    public void sendPayload(ISourceID id, ByteBuffer payload){
        if (!idRegistry.containsKey(new Integer(id.getSourceID()))) {
            throw new RuntimeException("SourceID " + id.getSourceID() + "not registered");
        } else {
            QueuedOutputChannel eng = (QueuedOutputChannel) idRegistry.get(id);
            eng.receiveByteBuffer(payload);
            messagesSent++;
        }
    }

    public long getMessagesSent() {
        return messagesSent;
    }
}
