/**
 * 
 * Date: Nov 13, 2006 4:23:25 PM
 * 
 * (c) 2005 IceCube Collaboration
 */
package icecube.daq.io;

/**
 * @author artur
 * @version : , v 1.1, Nov 13, 2006 4:23:25 PM artur $
 */
public class NormalState {

    // TODO: Add more states here as needed
    public static final NormalState IDLE = new NormalState("IDLE");
    public static final NormalState READY = new NormalState("READY");
    public static final NormalState STOPPED = new NormalState("STOPPED");
    public static final NormalState STOPPING = new NormalState("STOPPING");
    public static final NormalState RUNNING = new NormalState("RUNNING");
    public static final NormalState DISPOSING = new NormalState("DISPOSING");
    public static final NormalState DESTROYED = new NormalState("DESTROYED");

    private final String myName; // for debug only

    private NormalState(String name) {
        myName = name;
    }

    public String toString() {
        return myName;
    }
}
