/*
 * class: PayloadTransmitChannel
 *
 * Version $Id: PayloadTransmitChannel.java,v 1.10 2005/11/10 05:48:49 artur Exp $
 *
 * Date: May 15 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.io;

import EDU.oswego.cs.dl.util.concurrent.Mutex;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import icecube.daq.common.NormalState;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.common.DAQComponentObserver;
import icecube.daq.common.ErrorState;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IByteBufferReceiver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides the WritableByteChannel operations. It is also responsible
 * for returning buffers into the buffer cache.
 *
 * @author mcp
 * @version $Id: PayloadTransmitChannel.java,v 1.10 2005/11/10 05:48:49 artur Exp $
 */
public class PayloadTransmitChannel implements IByteBufferReceiver {

    // Log object for this class
    private static final Log log = LogFactory.getLog(PayloadTransmitChannel.class);

    // decalre some state transitions
    private static final int STATE_IDLE = 0;
    private static final int STATE_GETBUFFER = 1;
    private static final int STATE_TRANSMSG = 2;
    private static final int STATE_TRANSDONE = 3;
    private static final int STATE_ERROR = 4;
    private static final int STATE_CLOSED = 5;

    private static final int SIG_IDLE = 0;
    private static final int SIG_GET_BUFFER = 1;
    private static final int SIG_TRANSMIT = 2;
    private static final int SIG_DONE = 3;
    private static final int SIG_RESTART = 4;
    private static final int SIG_STOP = 5;
    private static final int SIG_ERROR = 6;
    private static final int SIG_CLOSE = 7;

    public static final String STATE_IDLE_NAME = "Idle";
    public static final String STATE_GETBUFFER_NAME = "GetBuffer";
    public static final String STATE_TRANSMSG_NAME = "TransMsg";
    public static final String STATE_TRANSDONE_NAME = "TransDone";
    public static final String STATE_ERROR_NAME = "Error";
    public static final String STATE_CLOSED_NAME = "Closed";

    private static final String[] STATE_NAME_MAP = {STATE_IDLE_NAME,
                                                    STATE_GETBUFFER_NAME,
                                                    STATE_TRANSMSG_NAME,
                                                    STATE_TRANSDONE_NAME,
                                                    STATE_ERROR_NAME,
                                                    STATE_CLOSED_NAME};

    private static final long DISPOSE_TIME_MSEC = 500;
    private static final int INT_SIZE = 4;

    // print log messages with state change information?
    private static final boolean TRACE_STATE = false;
    // print log messages with data dumps
    private static final boolean TRACE_DATADUMP = false;

    // sempahore to be used as a mutex to lock out cuncurrent ops
    // between wait code and channel locator callbacks
    private Mutex stateMachineMUTEX = new Mutex();
    // our internal state
    private int presState;
    // our last state
    private int prevState;
    // count transitions
    private int transitionCnt = 0;
    // count illeagal transition requests
    private int illegalTransitionCnt = 0;

    private WritableByteChannel channel = null;
    // local copy of selector
    private Selector selector;
    // local copy of selector key
    private SelectionKey selectionKey = null;
    // flag for end of data notification
    private boolean lastMsgAndStop;
    // counters for timer implemetation
    private long startTimeMsec;
    // buffer cache manager that is source of receive buffers
    private IByteBufferCache bufferMgr;
    // receive buffer in use
    private ByteBuffer buf;
    // ID string used to identify us during callbacks
    private String id;
    // local transmit counters
    protected long bytesSent;
    protected long recordsSent;
    protected long stopMsgSent;
    // local boolean to synchronize selector cancels and
    // prevent cancelled selector key exceptions
    private boolean cancelSelectorOnExit;
    private ByteBuffer lastMessage;

    // output buffer queue
    public LinkedQueue outputQueue;
    private boolean queueStillEmpty;

    private DAQComponentObserver compObserver;
    private String notificationID;

    private boolean debug = false;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public PayloadTransmitChannel(String myID,
                                  WritableByteChannel channel,
                                  Selector sel,
                                  IByteBufferCache bufMgr) {
        id = myID;
        presState = STATE_IDLE;
        prevState = presState;
        cancelSelectorOnExit = false;
        selector = sel;
        bufferMgr = bufMgr;
        outputQueue = new LinkedQueue();
        // make up a last message that we can use when we need it
        lastMessage = ByteBuffer.allocate(INT_SIZE);
        lastMessage.putInt(0, INT_SIZE);
        this.channel = channel;

    }

    protected void enterIdle() {
    }

    protected void exitIdle() {
        lastMsgAndStop = false;
    }

    protected void enterGetBuffer() {
        if (outputQueue.isEmpty()) {
            if (debug && !queueStillEmpty) {
                log.info(id + ":XMIT:EmptyQueue");
                queueStillEmpty = true;
            }
        } else {
            try {
                buf = (ByteBuffer) outputQueue.take();
                transition(SIG_TRANSMIT);
            } catch (InterruptedException e) {
                log.error(e);
            }
            if (debug) {
                log.info(id + ":XMIT:GotBuf " + buf);
                queueStillEmpty = false;
            }
        }
    }

    protected void exitGetBuffer() {
    }

    protected void enterTransMsg() {
        try {
            ((SelectableChannel) channel).register(selector,
                    SelectionKey.OP_WRITE, this);
            // make sure we don't cance the selector when we exit
            cancelSelectorOnExit = false;
        } catch (IOException e) {
            log.error(id + " cannot transmit", e);
            // flag error and return
            transition(SIG_ERROR);
            throw new RuntimeException(e);
        }
        buf.position(0);
        buf.limit(buf.getInt(0));
        if (buf.getInt(0) == INT_SIZE) {
            // this is the last message, flag state machine to stop when complete
            lastMsgAndStop = true;
            if (TRACE_DATADUMP && log.isErrorEnabled()) {
                log.error(id + ":XMIT:stop");
            } else if (debug) {
                log.info(id + ":XMIT:stop");
            }
        } else if (TRACE_DATADUMP && log.isErrorEnabled()) {
            log.error(id + ":XMIT:" + icecube.daq.payload.DebugDumper.toString(buf));
        }
        try {
            channel.write(buf);
            if (debug) {
                log.info(id + ":XMIT:wrote " + buf.getInt(0) + " bytes");
            }
        } catch (IOException e) {
            //need to do something here
            transition(SIG_ERROR);
            if (debug) {
                log.info(id + ":XMIT:WriteError");
                throw new RuntimeException(e);
            }
        }
    }

    protected void exitTransMsg() {
        // track some statistics
        bytesSent += buf.getInt(0);
        if (debug) {
            log.info(id + ":XMIT:sent " + bytesSent + " bytes");
        }
        recordsSent += 1;
        if (debug) {
            log.info(id + ":XMIT:sent " + recordsSent + " recs");
        }
        if (buf.getInt(0) == INT_SIZE) {
            stopMsgSent += 1;
        }
        // return the buffer via the supplied manager
        else {
            if (bufferMgr != null) {
                bufferMgr.returnBuffer(buf);
            }
        }
        cancelSelectorOnExit = true;
    }

    protected void enterTransDone() {
        if (lastMsgAndStop) {
            transition(SIG_STOP);
        } else {
            transition(SIG_RESTART);
        }
    }

    protected void exitTransDone() {
    }

    protected void enterError() {
        if (compObserver != null) {
            if (debug) {
                log.info(id + ":XMIT:Error");
            }
            compObserver.update(ErrorState.UNKNOWN_ERROR, notificationID);
        }
    }

    protected void exitError() {
    }

    protected void notifyOnStop() {
        if (compObserver != null) {
            if (debug) {
                log.info(id + ":XMIT:Stop");
            }
            compObserver.update(NormalState.STOPPED, notificationID);
        }
    }

    // all the following methods are mutex locked and thread safe.
    public void stopEngine() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_STOP);
            stateMachineMUTEX.release();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    public void sendLastAndStop() {
        try {
            outputQueue.put(lastMessage);
        } catch (InterruptedException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    public void startEngine() {
        try {
            stateMachineMUTEX.acquire();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
        transition(SIG_GET_BUFFER);
        stateMachineMUTEX.release();
    }

    public void flushOutQueue() {
        selector.wakeup();
    }

    public void registerComponentObserver(DAQComponentObserver compObserver,
                                          String notificationID) {
        if (this.compObserver != null) {
            try {
                throw new Error("StackTrace");
            } catch (Error err) {
                log.error("Setting multiple observers", err);
            }
        }

        this.compObserver = compObserver;
        this.notificationID = notificationID;
    }

    public String presentState() {
        // don't need to interlock this one.
        return STATE_NAME_MAP[presState];
    }

    public void injectError() {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_ERROR);
            stateMachineMUTEX.release();
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
    }

    public void processTimer() {
        if (presState == STATE_GETBUFFER) {
            enterGetBuffer();
        }
    }

    public void processSelect(SelectionKey selKey) {
        selectionKey = selKey;
        if (presState != STATE_TRANSMSG) {
            // should not be getting selects here
            selectionKey.cancel();
        } else {
            if (buf.hasRemaining()) {
                try {
                    channel.write(buf);
                } catch (IOException ioe) {
                    transition(SIG_ERROR);
                    log.error("IOException on channel.write(): ", ioe);
                    throw new RuntimeException(ioe);
                }
            } else {
                transition(SIG_DONE);
            }
            if (cancelSelectorOnExit) {
                // its ok to do it now.
                selectionKey.cancel();
            }
        }
    }

    void close()
        throws IOException
    {
        try {
            stateMachineMUTEX.acquire();
            transition(SIG_CLOSE);
            stateMachineMUTEX.release();
        } catch (InterruptedException e) {
            // should not happen
        }
    }

    private void enterClosed()
    {
        try {
            channel.close();
        } catch (IOException ioe) {
            log.error("Cannot close channel", ioe);
        }

        channel = null;
    }

    private void transition(int signal) {
        if (log.isDebugEnabled() && TRACE_STATE) {
            log.debug("PayloadTransmitEngine " + id +
                    " state " + getStateName(presState) +
                    " transition signal " + getSignalName(signal));
        } else if (debug) {
            log.info(id + " " + getStateName(presState) + " -> " +
                     getSignalName(signal));
        }

        // note, in order to simplify state machine operation, NO
        // transitions are allowed in state exit routines.  They are allowed
        // in state enter routines, since the present state flag
        // has been appropriately changed and things are reentrant
        switch (presState) {
            case STATE_IDLE:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitIdle();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_GET_BUFFER:
                            {
                                exitIdle();
                                doTransition(STATE_GETBUFFER);
                                enterGetBuffer();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitIdle();
                                doTransition(STATE_CLOSED);
                                enterClosed();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_GETBUFFER:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitGetBuffer();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_TRANSMIT:
                            {
                                exitGetBuffer();
                                doTransition(STATE_TRANSMSG);
                                enterTransMsg();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitGetBuffer();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitGetBuffer();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_TRANSMSG:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitTransMsg();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_DONE:
                            {
                                exitTransMsg();
                                doTransition(STATE_TRANSDONE);
                                enterTransDone();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitTransMsg();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitTransMsg();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_TRANSDONE:
                {
                    switch (signal) {
                        case SIG_ERROR:
                            {
                                exitTransDone();
                                doTransition(STATE_ERROR);
                                enterError();
                                break;
                            }
                        case SIG_RESTART:
                            {
                                exitTransDone();
                                doTransition(STATE_GETBUFFER);
                                enterGetBuffer();
                                break;
                            }
                        case SIG_STOP:
                            {
                                exitTransDone();
                                doTransition(STATE_IDLE);
                                notifyOnStop();
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitTransDone();
                                doTransition(STATE_CLOSED);
                                notifyOnStop();
                                enterClosed();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_ERROR:
                {
                    switch (signal) {
                        case SIG_IDLE:
                            {
                                exitError();
                                doTransition(STATE_IDLE);
                                enterIdle();
                                break;
                            }
                        case SIG_CLOSE:
                            {
                                exitError();
                                doTransition(STATE_CLOSED);
                                enterIdle();
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            case STATE_CLOSED:
                {
                    switch (signal) {
                        case SIG_CLOSE:
                            {
                                // do nothing
                                break;
                            }
                        default:
                            illegalTransitionCnt++;
                            break;
                    }
                    break;
                }
            default:
                {
                    illegalTransitionCnt++;
                    break;
                }
        }
    }

    private void doTransition(int nextState) {
        prevState = presState;
        presState = nextState;
        transitionCnt++;
    }

    /**
     * Receives a ByteBuffer from a source.
     *
     * @param tBuffer ByteBuffer the new buffer to be processed.
     */
    public void receiveByteBuffer(ByteBuffer tBuffer) {
        try {
            if (tBuffer.getInt(0) != tBuffer.limit()) {
                if (tBuffer.getInt(0) > tBuffer.capacity()) {
                    throw new RuntimeException("ByteBuffer corrupted - rec length: "
                            + tBuffer.getInt(0) + " capacity: " + tBuffer.capacity());
                }
                tBuffer.limit(tBuffer.getInt(0));
            }
            outputQueue.put(tBuffer);
        } catch (InterruptedException ie) {
            log.error(ie);
            throw new RuntimeException(ie);
        }
        flushOutQueue();
    }

    public void destinationClosed() {
        flushOutQueue();
    }

    public boolean isOutputQueued() {
        return !outputQueue.isEmpty();
    }

    private static final String getStateName(int state) {
        final String name;
        switch (state) {
            case STATE_IDLE:
                name = STATE_IDLE_NAME;
                break;
            case STATE_GETBUFFER:
                name = STATE_GETBUFFER_NAME;
                break;
            case STATE_TRANSMSG:
                name = STATE_TRANSMSG_NAME;
                break;
            case STATE_TRANSDONE:
                name = STATE_TRANSDONE_NAME;
                break;
            case STATE_ERROR:
                name = STATE_ERROR_NAME;
                break;
            case STATE_CLOSED:
                name = STATE_CLOSED_NAME;
                break;
            default:
                name = "UNKNOWN#" + state;
                break;
        }
        return name;
    }

    private static final String getSignalName(int signal) {
        final String name;
        switch (signal) {
            case SIG_IDLE:
                name = "IDLE";
                break;
            case SIG_GET_BUFFER:
                name = "GET_BUFFER";
                break;
            case SIG_TRANSMIT:
                name = "TRANSMIT";
                break;
            case SIG_DONE:
                name = "DONE";
                break;
            case SIG_RESTART:
                name = "RESTART";
                break;
            case SIG_STOP:
                name = "STOP";
                break;
            case SIG_ERROR:
                name = "ERROR";
                break;
            case SIG_CLOSE:
                name = "CLOSE";
                break;
            default:
                name = "UNKNOWN#" + signal;
                break;
        }
        return name;
    }
}
