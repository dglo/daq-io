package icecube.daq.io;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.test.LoggingCase;
import icecube.daq.io.test.MockBufferCache;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayloadDestination;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IEventPayload;
import icecube.daq.payload.IHitData;
import icecube.daq.payload.IReadoutDataPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.Poolable;
import icecube.daq.util.DOMRegistry;
import icecube.daq.payload.impl.DOMID;
import icecube.daq.payload.impl.EventPayload_v5;
import icecube.daq.payload.impl.EventPayload_v6;
import icecube.daq.payload.impl.PayloadFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.zip.DataFormatException;


import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;


public class PayloadFileReaderTest
    extends LoggingCase
{
    public PayloadFileReaderTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return new TestSuite  (PayloadFileReaderTest.class);
    }

    public void testMethods() throws Exception
    {
	File file = new File("subdir");
	String fileName;
	URL url = this.getClass().getResource("/subdir");
	fileName = url.getFile();
	PayloadFileReader pfr, pfr1;
	pfr = new PayloadFileReader(fileName);
	pfr1 = new PayloadFileReader(file);
	
	assertNotNull("returns this object", pfr1.iterator());
		

	try {
	    pfr1.remove();
	} catch (Error e) {
	    if(!e.getMessage().equals("Unimplemented")) {
		throw new Error("Unimplemented");
	    }
	}

	pfr1.close();
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
