/**
 * LoadTesterThread.java
 *
 * @author Ben Ransford
 * @version $Id: LoadTesterThread.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.test;

// package imports.
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import javax.net.SocketFactory;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.util.MD5;
import org.dom4j.Document;

public class LoadTesterThread extends Thread {
    private InetAddress addr;
    private LoadTester parent;
    private int port;
    private SOAPMessage soapMessage;
    private String soapSum;
    private int groupSize;
    private String methodName;
    private int howmany = 1;

    public LoadTesterThread (ThreadGroup tg, String name, LoadTester parent,
			     int howmany) {
	super(tg, name);
	try {
	    this.addr = InetAddress.getByName(parent.getHost());
	} catch (UnknownHostException uhe) {
	    parent.fatalError(uhe.getMessage());
	}
	System.err.println(" for " + addr.toString());
	this.parent = parent;
	this.port = parent.getPort();
	this.groupSize = parent.getNumClients();
	this.howmany = howmany;
    }

    public void run () {
	Socket sock = null;
	PrintWriter out = null;
	BufferedReader in = null;
	long connectTime=0, methodTime=0, methodStart=0;

	try {
	    long prebanner = System.currentTimeMillis();
	    sock = parent.getSocketFactory().createSocket(addr, port);
	    out = new PrintWriter(sock.getOutputStream(), false);
	    in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
	    
	    String line = in.readLine();
	    if ((line != null) && line.startsWith("+ READY")) {
		connectTime = System.currentTimeMillis() - prebanner;

		for (int idx = 0; idx < howmany; idx++) {
		    out.println("SOAP " + soapSum + " WILL_WAIT");
		    out.println(soapMessage);
		    out.println("&");
		    methodStart = System.currentTimeMillis();
		    out.flush();
		    
		    line = in.readLine();
		    if ((line != null) && line.startsWith("+ OK")) {
			methodTime = System.currentTimeMillis() - methodStart;
			// read out the rest of the response just so
			// we don't poison any buffers anywhere
			while ((line = in.readLine()) != null) {
			    if (line.equals("&")) {
				break;
			    }
			}
		    } else {
			throw new HandlerException("Server failed");
		    }
		    System.out.println(methodName + "\t" +
				       soapMessage.toString().length() + "\t" +
				       groupSize + "\t" +
				       connectTime + "\t" +
				       methodTime);
		}
		out.println("QUIT");
		out.flush();
	    }
	} catch (Exception e) {
	    parent.fatalError(e.getMessage());
	} finally {
	    try {
		out.close();
		in.close();
		sock.close();
	    } catch (Exception e) { }
	}
	    
    }

    public LoadTesterThread setDocument (Document d, String methodName) {
	try {
	    soapMessage = new SOAPMessage(d);
	} catch (Exception e) {
	    System.err.println("Error assembling SOAP message.");
	    parent.fatalError(e.getMessage());
	}
	soapSum = ((new MD5(soapMessage.toString() + "\n")).calc()).toString();

	this.methodName = methodName;
	return this;
    }
}
