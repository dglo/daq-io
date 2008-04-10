/*
 * class: DAQComponentOutputProcess
 *
 * Version $Id: DAQComponentOutputProcess.java 2894 2008-04-10 18:57:32Z dglo $
 *
 * Date: March 24 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import icecube.daq.payload.IByteBufferCache;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * This represents the engine for the transmit channels.
 *
 * @version $Id: DAQComponentOutputProcess.java 2894 2008-04-10 18:57:32Z dglo $
 * @author mcp
 */
public interface DAQComponentOutputProcess extends DAQComponentIOProcess {

    QueuedOutputChannel addDataChannel(WritableByteChannel channel,
                                       IByteBufferCache bufMgr);

    QueuedOutputChannel connect(IByteBufferCache bufMgr,
                                WritableByteChannel chan, int srcId)
        throws IOException;

    void disconnect()
        throws IOException;

    boolean isConnected();

    void sendLastAndStop();

}
