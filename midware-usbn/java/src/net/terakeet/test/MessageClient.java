/**
 * MessageClient.java
 *
 * @author Ben Ransford
 * @version $Id: MessageClient.java,v 1.2 2011/04/19 14:56:30 sravindran Exp $
 */

package net.terakeet.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import net.terakeet.soapware.MessageException;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.util.MD5;
import net.terakeet.util.MidwareLogger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class MessageClient {
    static MidwareLogger logger = new MidwareLogger(MessageClient.class.getName());
    private String host;
    private int port;
    private File file;

    MessageClient (String host, int port, String filename) throws TestFailure {
	File msgFile = new File(filename);
	if (! msgFile.canRead())
	    throw new TestFailure("Cannot read file " + filename);
	this.host = host;
	this.port = port;
	this.file = msgFile;
    }

    static void usage () {
	System.err.println("Usage: java MessageClient <host> <port> request.xml");
	System.exit(1);
    }

    public Document getResponse () {
	logger.debug("Trying to connect to " + host + ":" + port);
	Socket sock = null;
	PrintWriter out = null;
	BufferedReader in = null;
	try {
	    SOAPMessage msg = new SOAPMessage(file);
	    String msgSum = ((new MD5(msg.toString() + "\n")).calc()).toString();
	    sock = new Socket(host, port);
	    out = new PrintWriter(sock.getOutputStream(), true);
	    in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
	    String line = in.readLine();
	    if ((line != null) && line.startsWith("+ READY ")) {
		out.println("SOAP " + msgSum + " WILL_WAIT");
		out.println(msg);
		out.println("&");

		line = in.readLine();
		if ((line != null) && line.startsWith("+ OK qid=")) {
		    logger.debug("Server queued message, saying: " + line);
		} else {
		    logger.generalWarning("Server couldn't queue message; line = " + line);
		}

		line = in.readLine();
		if ((line != null) && line.startsWith("+ OK ")) {
		    logger.debug("Response header: " + line);
		    StringBuffer resp = new StringBuffer();
		    while ((line = in.readLine()) != null) {
			if (line.equals("&")) {
			    break;
			}
			resp.append(line);
			resp.append("\n");
		    }
		    if (resp != null) {
			String rsp = resp.toString();
			SOAPMessage response = null;
			try {
			    response = new SOAPMessage(rsp);
			} catch (MessageException me) {
			    logger.generalWarning("Something is wrong with response:\n" + rsp);
			}
			if (response != null)
			    logger.info("Response:\n" + response.toString());
		    }
		    out.println("QUIT");
		} else {
		    logger.generalWarning("Bad response after message: line == " + line);
		}
	    } else {
		logger.generalWarning("Eek! Banner == " + line);
	    }
	} catch (Exception e) {
	    logger.generalWarning(e.toString());
	    e.printStackTrace();
	} finally {
	    try {
		out.close();
		in.close();
		sock.close();
	    } catch (IOException ioe) {
		logger.generalWarning(ioe.getMessage());
	    } finally {
		logger.debug("Client is done.");
	    }
	}
	return (Document)null;
    }

    public static void main (String[] args) {
	if (args.length != 3) {
	    usage();
	}

	// configure log4j
	BasicConfigurator.configure();
	logger.setLevel(Level.DEBUG);

	String host = args[0];
	int port = (Integer.valueOf(args[1])).intValue();
	String filename = args[2];
	try {
	    Document response = (new MessageClient(host, port, filename)).getResponse();
	} catch (Exception e) {
	    logger.generalWarning(e);
	}
    }
}
