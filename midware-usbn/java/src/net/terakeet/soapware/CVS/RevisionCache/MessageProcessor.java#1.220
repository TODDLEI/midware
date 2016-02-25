/**
 * MessageProcessor.java
 *
 * @author Ben Ransford
 * @version $Id: MessageProcessor.java,v 1.220 2013/09/29 08:14:02 sravindran Exp $
 */
package net.terakeet.soapware;

import java.io.*;
import java.net.Socket;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Vector;
import net.terakeet.util.MD5;
import net.terakeet.soapware.queue.QueueException;
import net.terakeet.soapware.queue.QueueManager;
import net.terakeet.util.MidwareLogger;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.XML;

/**
 * Handles an incoming message-processing session.  Passes SOAP
 * requests on to the correct handlers; returns error information if
 * applicable.
 */
public class MessageProcessor extends Thread {
    private MidwareLogger logger = null;
    private static final int INITIAL_THREADS = 5;
    private Socket sock = null;
    private BufferedReader in;
    private PrintWriter out;
    static String hostname = "";
    private HandlerMap handlerMap;
    private MessageListener parent;
    private QueueManager queueManager;

    /** the String used to terminate XML input over a socket. */
    public static String TERM_STR = "&";

    // holds state information.
    private int state = 0;

    public static final int ST_READY = 1;
    public static final int ST_SOAP_WAIT = 2;
    public static final int ST_SOAP_RCVD = 3;
    public static final int ST_CKSUM_OK = 4;

    public static final int ST_QDISPATCH = 5;
    public static final int ST_QRUN = 6;
    public static final int ST_QRETURN = 7;

    public static final int ST_QUEUE_START = 8;
    public static final int ST_QUEUE_WRITE = 9; // not used
    public static final int ST_QUEUE_MOVE = 10; // not used
    public static final int ST_QUEUE_OK = 11;

    public static final int ST_CLEANUP = 100;

    public static final int ST_CKSUM_FAIL = 998;
    public static final int ST_ERROR = 999;

    // string matchers.
    private static Pattern P_SOAPCMD, P_FETCHRSP;

    // string matcher for new bevBox code.
    private static String P_ADDREADINGSTART, P_ADDREADINGEND;

    // string matcher for new bevBox code.
    private static String P_ADDCOMPREADINGSTART, P_ADDCOMPREADINGEND;

    /**
     * Default constructor.
     * @param tg the thread group that contains MessageProcessor threads
     * @param name a unique name for this thread
     * @param ml the parent/supervisor of this thread
     */
    public MessageProcessor (ThreadGroup tg, String name, MessageListener ml)
	throws IOException {
	super(tg, name);
	this.parent = ml;
	this.handlerMap = ml.getHandlerMap();
	this.queueManager = ml.getQueueManager();

	/* initialize patterns for matching commands */
	if (MessageProcessor.P_SOAPCMD == null) {
	    MessageProcessor.P_SOAPCMD =
		Pattern.compile("^SOAP +([0-9a-fA-F]{32})? *(WILL_WAIT)?$");
	    MessageProcessor.P_FETCHRSP =
		Pattern.compile("^FETCH +([,0-9a-fA-F]+)$");
	}

	/* initialize patterns for matching commands */
	if (P_ADDREADINGSTART == null) {
	    P_ADDREADINGSTART               = "<m:addReading";
	    P_ADDREADINGEND                 = "</m:addReading>";
	}

	/* initialize patterns for matching commands */
	if (P_ADDCOMPREADINGSTART == null) {
	    P_ADDCOMPREADINGSTART           = "<m:addComponentReading>";
	    P_ADDCOMPREADINGEND             = "</m:addComponentReading>";
	}
    }

    public MessageProcessor setHostname (String hostname) {
	this.hostname = hostname;
	return this;
    }

    public MessageProcessor setSocket (Socket sock) throws IOException {
	this.sock = sock;
	this.in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
	this.out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream()));
	return this;
    }

    /**
     * Standard thread <code>run()</code> function.  Does all the
     * work.
     */
    public void run () {
        logger                              = new MidwareLogger(MessageProcessor.class.getName());
	//logger.connection("thread started.");

	/* now do the hard part(s) */
	try {
	    out.println("+ READY " + hostname);
	    out.flush();
	    this.state                      = ST_READY;

	    String command;
            String newCommand               = "";
            boolean newPacket               = false, regularPacket = true;
	    while ((command = in.readLine().trim()) != null) {
                
                //logger.connection(command);

                if (command.startsWith(P_ADDREADINGSTART)) {
                    regularPacket           = false;
                    newPacket               = true;
                    command                 = "<m:addReading xmlns:m=\"http://www.terakeet.net/schemas/message\">";
                }
                
                if (command.equals(P_ADDREADINGEND)) {
                    newPacket               = false;
                    newCommand              += command;
                    //logger.connection(bevBoxCommand);
                    if (!processBevBoxCommand(newCommand))
		    break;
                }

                if (command.contains(P_ADDCOMPREADINGSTART)) {
                    regularPacket           = false;
                    newPacket               = true;
                    command                 = "<m:addComponentReading xmlns:m=\"http://www.terakeet.net/schemas/message\">";
                }

                if (command.contains(P_ADDCOMPREADINGEND)) {
                    newPacket               = false;
                    newCommand              += command;
                    logger.connection(newCommand);
                    if (!processComponentCommand(newCommand))
		    break;
                }
                
                if (newPacket) {
                    newCommand              += command;
                }


                if (command.contains("JSON")) {
                    regularPacket           = false;
                    try {
                        JSONObject jObject  = new JSONObject(command);
                        XML xmlObject       = new XML();
                        String xml          = xmlObject.toString(jObject);
                        xml                 = xml.replaceAll("<JSON", "<m:");
                        xml                 = xml.replaceAll("JSON><", " xmlns:m=\"http://www.terakeet.net/schemas/usadMessage\"><");
                        xml                 = xml.replaceAll("</JSON", "</m:");
                        xml                 = xml.replaceAll("JSON>", ">");
                        //logger.connection(xml);
                        if (!processJSONCommand(xml))
                        break;
                    } catch (Exception e) {
                       logger.dbError("getJson error: "+e.toString());
                   }
                }

                if (regularPacket) {
                    if (!processCommand(command))
                    break;
                }

                /*
                 */
                
            }
	} catch (IOException ioe) {
	    logger.dbError(ioe.getMessage());
	} finally {
	    try {
		in.close();
		out.close();
		sock.close();
	    } catch (IOException ioe) {
		logger.dbError(ioe.getMessage());
	    } catch (Exception e) {
                logger.dbError("Error: " + e.toString());
            } finally {
		logger.debug("thread ended.");
	    }
	}
    }

    /**
     * Processes a client's command from this thread's socket.
     * @param command the command (from the client on the other end of
     * the socket) to be processed.
     * @return <code>false</code> if the command received was
     * <code>QUIT</code>; returns <code>true</code> otherwise.
     * @throws IOException if there is any error writing to the output
     * socket.
     */
    private boolean processCommand (String command) throws IOException {
	if (command == null) {
	    throw new NullPointerException("Cannot process null command string.");
	}

	if (command.equals("QUIT")) {
	    return false;
	}

	if (command.startsWith("PING")) {
	    out.println("+ OK");
	    out.flush();
	    return true;
	}

	if (command.equals("STATUS")) {
	    if (queueManager.isQueueAvailable()) {
		out.println("+ OK " + parent.threadCount());
	    } else {
		out.println("- ERR No queue available.");
	    }
	    out.flush();
	    return true;
	}

	/* handle retrieval of a stored response */
	Matcher m = P_FETCHRSP.matcher(command);
	if (m.matches()) {
	    String qid = m.group(1);
	    /* if response exists, return it the same way we return a
	     * regular on-line response, then delete it from the
	     * response queue */
	    /* else tell the client that the response isn't ready. */
	    try {
		SOAPMessage response = queueManager.fetchResponse(qid);
		String resp = response.toString();
		String rmd5 = (new MD5(resp+"\n")).calc().toString();
		out.println("+ OK " + rmd5);
		out.println(resp + "\n&");
		out.flush();
		queueManager.removeResponse(qid);
	    } catch (QueueException qe) {
		out.println("- ERR no response available.");
		out.flush();
	    }
	    return true;
	}

	/* handle a SOAP message submission */
	m = P_SOAPCMD.matcher(command);
	if (m.matches()) {
	    state = ST_SOAP_WAIT;
	    /* the command string will take the form
	         SOAP {cksum} WILL_WAIT
	       if the client plans to hold the connection open
	       (thereby leaving this thread alive) until we've
	       prepared and sent a response, or
	         SOAP {cksum}
	       if the client plans to eat and run, as it were.
	    */
	    String wantChecksum = m.group(1);
	    boolean willWait = ((m.groupCount() > 1)
				&& (m.group(2).equals("WILL_WAIT")));

	    /* read the XML document from the socket.  the terminating
	     * character will be a TERM_STR on a line by itself.  if
	     * TERM_STR absolutely must appear on a line by itself, it
	     * must surrounded by '{' '}' characters. */
	    StringBuffer buf = new StringBuffer();
	    String data;
	    String termEscaped = "{"+TERM_STR+"}";
	    while ((data = in.readLine()) != null) {
		if (termEscaped.equals(data)) {
		    buf.append(TERM_STR);
		} else if (TERM_STR.equals(data)) {
		    break;
		} else {
		    buf.append(data);
		    buf.append("\n");
		}
	    }
	    String xmlDoc = buf.toString().replaceAll("\\r", "");
            
	    state = ST_SOAP_RCVD;

	    /* integrity check: make sure the document received over
	     * the socket has the checksum claimed by the client. */
	    String gotChecksum = (new MD5(xmlDoc)).calc().toString();
	    if ((wantChecksum == null) || wantChecksum.equals(gotChecksum)) {
		state = ST_CKSUM_OK;
		SOAPMessage sm;
		try {
		    sm = new SOAPMessage(xmlDoc);
		    //logger.connection("SOAP message received");
		    state = ST_QUEUE_START;

		    /* if client doesn't plan to wait around, queue
		     * the message. */
		    if (!willWait) {
                        //logger.connection("Processing will not wait message");
			String qid;
			try {
			    qid = queueManager.queueWaiting(sm);
			} catch (QueueException qe) {
			    if (! qe.notifyAdministrator()) {
				queueManager.chooseBestQueue();
				qid = queueManager.queueWaiting(sm);
			    } else {
				throw qe;
			    }
			}
			state = ST_QUEUE_OK;
			out.println("+ OK qid=" + qid);
			out.flush();
		    } else {
                        //logger.connection("Processing will not wait message");
			String qid;
			try {
			    qid = queueManager.queueActive(sm);
			} catch (QueueException qe) {
			    if (! qe.notifyAdministrator()) {
				queueManager.chooseBestQueue();
				qid = queueManager.queueActive(sm);
			    } else {
				throw qe;
			    }
			}
			state = ST_QUEUE_OK;
			out.println("+ OK qid=" + qid);
			out.flush();

			/* iterate over the children of the request's SOAP
			 * Body tag and get a response for each one. */
			SOAPMessage response = new SOAPMessage();
			while (sm.hasNext()) {
			    Element processMe = sm.next();
			    String eltName = processMe.getName();
			    logger.debug("Processing node: " + eltName);
                            String error = null;
                            String className = "";
			    try {
				state = ST_QDISPATCH;
				Class instantiateMe = handlerMap.get(eltName);
				className = instantiateMe.getName();
				logger.debug("Instantiating " + className);
				Object o = instantiateMe.newInstance();
				Handler handler = (Handler)o;
				logger.debug("Dispatch: " + eltName +
					     " -> " + className);

				/* important part */
				state = ST_QRUN;
				handler.handle(processMe, response.getBodyElement());
                                logger.debug(className + " done.");
			    } catch (HandlerException he) {
                                error = eltName + " (" + className + "):   " +
                                        he.toString();
				logger.generalWarning("Error while running handler");
				logger.handlerException(he.toString());
				out.println("- ERR " + he.getMessage());
				out.flush();
			    } catch (InstantiationException ie) {
                                error = ie.toString();
				logger.generalWarning("Cannot instantiate handler class.");
				logger.midwareError("IA: "+ie.toString());
				out.println("- ERR " + ie.getMessage());
				out.flush();
			    } catch (IllegalAccessException iae) {
                                error = "Illegal Access: " + iae.toString();
				logger.generalWarning("IAE: "+iae.toString());
				out.println("- ERR System error");
				out.flush();
			    } catch (Exception e) {
                                error = e.toString();
                                logger.generalWarning("Error: " + e.toString());
                                out.println("- ERR System error");
                                out.flush();
			    }
                            if (null != error) {
                                ErrorMailer.addError(error);
                            }
			}

			/* queue the response in case anything goes
			 * wrong */
			state = ST_QRETURN;
			queueManager.queueResponse(response, qid);
			/* ... and remove the request once the
			 * response is queued up. */
			queueManager.removeActive(qid);

			String resp = response.toString();
			String rmd5 = (new MD5(resp+"\n")).calc().toString();
			out.println("+ OK " + rmd5);
			out.println(resp + "\n&");
			out.flush();

			/* since the response has been successfully
			 * flushed to the client, go ahead and delete
			 * it from the response queue. */
			state = ST_CLEANUP;
			queueManager.removeResponse(qid);
		    }
		} catch (MessageException me) {
                    logger.generalWarning("SOAP message not parseable from input");
		    logger.handlerException("SOAP message not parseable from input");
		    out.println("- ERR message could not be parsed.");
		    out.flush();
		} catch (QueueException qe) {
                    logger.generalWarning("ERR message could not be queued; retry later");
		    logger.midwareError("Queue error" + (qe.notifyAdministrator() ?
						 " (FATAL)" : "") +
				": " + qe.getMessage());
		    out.println("- ERR message could not be queued; retry later.");
		    out.flush();
		} catch (Exception e) {
                    logger.generalWarning("Error: " + e.toString());
                    out.println("- ERR System error");
                    out.flush();
                }
	    } else {
		state = ST_CKSUM_FAIL;
		logger.generalWarning("bad checksum received (c:"+wantChecksum +
			    " s:"+gotChecksum + ")");
		out.println("- ERR bad checksum");
		out.flush();
	    }

	    return true;
	}

	/* XXX should we have an error threshold after which an
	 * unrecognized command will disconnect the client? */
	state = ST_ERROR;
	out.println("- ERROR unrecognized command.");
	out.flush();
	
	/*
	logger.handlerException("Unrecognized command: " + command);
        if (out.checkError()) { // flushes, too
	    state = ST_ERROR;
	    throw new IOException("Error writing to socket.");
	}
	*/
	return true;
    }

    /**
     * Processes a client's command from this thread's socket.
     * @param command the command (from the client on the other end of
     * the socket) to be processed.
     * @return <code>false</code> if the command received was
     * <code>QUIT</code>; returns <code>true</code> otherwise.
     * @throws IOException if there is any error writing to the output
     * socket.
     */
    private boolean processBevBoxCommand(String command) throws IOException {
        
        state                               = ST_SOAP_WAIT;
        
        StringBuffer buf                    = new StringBuffer();
        buf.append("<?xml version=\"1.0\" encoding=\"utf-\"?>");
        buf.append("\n");
        buf.append("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Body xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\">");
        buf.append(command);
        buf.append("</SOAP-ENV:Body></SOAP-ENV:Envelope>");
        buf.append("\n");
        String xmlDoc                       = buf.toString().replaceAll("\\r", "");
        state                               = ST_SOAP_RCVD;
        logger.connection(xmlDoc);

        try {
            SOAPMessage sm                  = new SOAPMessage(xmlDoc);
            //logger.connection("SOAP message received");
            state                           = ST_QUEUE_START;

            String qid;
            try {
                qid                         = queueManager.queueActive(sm);
            } catch (QueueException qe) {
                if (! qe.notifyAdministrator()) {
                    queueManager.chooseBestQueue();
                    qid                     = queueManager.queueWaiting(sm);
                } else {
                    throw qe;
                }
            }
            state                           = ST_QUEUE_OK;
            //out.println("+ OK qid=" + qid);
            //out.flush();

            /* iterate over the children of the request's SOAP
             * Body tag and get a response for each one. */
            SOAPMessage response            = new SOAPMessage();
            while (sm.hasNext()) {
                Element processMe           = sm.next();
                String eltName              = processMe.getName();
                //logger.connection("Processing node: " + eltName);
                String error                = null;
                String className            = "";
                try {
                    state                   = ST_QDISPATCH;
                    Class instantiateMe     = handlerMap.get(eltName);
                    className               = instantiateMe.getName();
                    //logger.connection("Instantiating " + className);
                    Object o                = instantiateMe.newInstance();
                    Handler handler         = (Handler)o;
                    //logger.connection("Dispatch: " + eltName + " -> " + className);

                    /* important part */
                    state                   = ST_QRUN;
                    handler.handle(processMe, response.getBodyElement());
                    //logger.connection(className + " done.");
                } catch (HandlerException he) {
                    error                   = eltName + " (" + className + "):   " + he.toString();
                    logger.generalWarning("Error while running handler");
                    logger.handlerException(he.toString());
                } catch (InstantiationException ie) {
                    error                   = ie.toString();
                    logger.generalWarning("Cannot instantiate handler class.");
                    logger.midwareError("IA: "+ie.toString());
                } catch (IllegalAccessException iae) {
                    error                   = "Illegal Access: " + iae.toString();
                    logger.generalWarning("IAE: "+iae.toString());
                } catch (Exception e) {
                    error                   = e.toString();
                    logger.generalWarning("Error: " + e.toString());
                }
                if (null != error) {
                    ErrorMailer.addError(error);
                }
            }

            /* queue the response in case anything goes
             * wrong */
            state                           = ST_QRETURN;
            queueManager.queueResponse(response, qid);
            /* ... and remove the request once the
             * response is queued up. */
            queueManager.removeActive(qid);

            out.println(response.toString());
            out.flush();

            /* since the response has been successfully
             * flushed to the client, go ahead and delete
             * it from the response queue. */
            state                           = ST_CLEANUP;
            queueManager.removeResponse(qid);
        } catch (MessageException me) {
            logger.generalWarning("SOAP message not parseable from input");
            logger.handlerException("SOAP message not parseable from input");
            out.println("- ERR message could not be parsed.");
            out.flush();
        } catch (QueueException qe) {
            logger.generalWarning("ERR message could not be queued; retry later");
            logger.midwareError("Queue error" + (qe.notifyAdministrator() ? " (FATAL)" : "") + ": " + qe.getMessage());
            out.println("- ERR message could not be queued; retry later.");
            out.flush();
        } catch (Exception e) {
            logger.generalWarning("Error: " + e.toString());
            out.println("- ERR System error");
            out.flush();
        }
        
        return true;
    }

    /**
     * Processes a client's command from this thread's socket.
     * @param command the command (from the client on the other end of
     * the socket) to be processed.
     * @return <code>false</code> if the command received was
     * <code>QUIT</code>; returns <code>true</code> otherwise.
     * @throws IOException if there is any error writing to the output
     * socket.
     */
    private boolean processComponentCommand (String command) throws IOException {

        state                               = ST_SOAP_WAIT;

        StringBuffer buf                    = new StringBuffer();
        buf.append("<?xml version=\"1.0\" encoding=\"utf-\"?>");
        buf.append("\n");
        buf.append("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Body xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\">");
        buf.append(command);
        buf.append("</SOAP-ENV:Body></SOAP-ENV:Envelope>");
        buf.append("\n");
        String xmlDoc                       = buf.toString().replaceAll("\\r", "");
        state                               = ST_SOAP_RCVD;
        //logger.connection(xmlDoc);

        try {
            SOAPMessage sm                  = new SOAPMessage(xmlDoc);
            //logger.connection("SOAP message received");
            state                           = ST_QUEUE_START;

            String qid;
            try {
                qid                         = queueManager.queueActive(sm);
            } catch (QueueException qe) {
                if (! qe.notifyAdministrator()) {
                    queueManager.chooseBestQueue();
                    qid                     = queueManager.queueWaiting(sm);
                } else {
                    throw qe;
                }
            }
            state                           = ST_QUEUE_OK;
            //out.println("+ OK qid=" + qid);
            //out.flush();

            /* iterate over the children of the request's SOAP
             * Body tag and get a response for each one. */
            SOAPMessage response            = new SOAPMessage();
            while (sm.hasNext()) {
                Element processMe           = sm.next();
                String eltName              = processMe.getName();
                //logger.connection("Processing node: " + eltName);
                String error                = null;
                String className            = "";
                try {
                    state                   = ST_QDISPATCH;
                    Class instantiateMe     = handlerMap.get(eltName);
                    className               = instantiateMe.getName();
                    //logger.connection("Instantiating " + className);
                    Object o                = instantiateMe.newInstance();
                    Handler handler         = (Handler)o;
                    //logger.connection("Dispatch: " + eltName + " -> " + className);

                    /* important part */
                    state                   = ST_QRUN;
                    handler.handle(processMe, response.getBodyElement());
                    //logger.connection(className + " done.");
                } catch (HandlerException he) {
                    error                   = eltName + " (" + className + "):   " + he.toString();
                    logger.generalWarning("Error while running handler");
                    logger.handlerException(he.toString());
                } catch (InstantiationException ie) {
                    error                   = ie.toString();
                    logger.generalWarning("Cannot instantiate handler class.");
                    logger.midwareError("IA: "+ie.toString());
                } catch (IllegalAccessException iae) {
                    error                   = "Illegal Access: " + iae.toString();
                    logger.generalWarning("IAE: "+iae.toString());
                } catch (Exception e) {
                    error                   = e.toString();
                    logger.generalWarning("Error: " + e.toString());
                }
                if (null != error) {
                    ErrorMailer.addError(error);
                }
            }

            /* queue the response in case anything goes
             * wrong */
            state                           = ST_QRETURN;
            queueManager.queueResponse(response, qid);
            /* ... and remove the request once the
             * response is queued up. */
            queueManager.removeActive(qid);

            out.println(response.toString() + "\n&");
            out.flush();
            
            /* since the response has been successfully
             * flushed to the client, go ahead and delete
             * it from the response queue. */
            state                           = ST_CLEANUP;
            queueManager.removeResponse(qid);
        } catch (MessageException me) {
            logger.generalWarning("SOAP message not parseable from input");
            logger.handlerException("SOAP message not parseable from input");
            out.println("- ERR message could not be parsed.");
            out.flush();
        } catch (QueueException qe) {
            logger.generalWarning("ERR message could not be queued; retry later");
            logger.midwareError("Queue error" + (qe.notifyAdministrator() ? " (FATAL)" : "") + ": " + qe.getMessage());
            out.println("- ERR message could not be queued; retry later.");
            out.flush();
        } catch (Exception e) {
            logger.generalWarning("Error: " + e.toString());
            out.println("- ERR System error");
            out.flush();
        }

        return true;
    }
    private boolean processJSONCommand(String command) throws IOException {

        state                               = ST_SOAP_WAIT;

        StringBuffer buf                    = new StringBuffer();
        buf.append("<?xml version=\"1.0\" encoding=\"utf-\"?>");
        buf.append("\n");
        buf.append("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Body xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\">");
        buf.append(command);
        buf.append("</SOAP-ENV:Body></SOAP-ENV:Envelope>");
        buf.append("\n");
        String xmlDoc                       = buf.toString().replaceAll("\\r", "");
        state                               = ST_SOAP_RCVD;
        //logger.connection(xmlDoc);

        try {
            SOAPMessage sm                  = new SOAPMessage(xmlDoc);
            //logger.connection("SOAP message received");
            state                           = ST_QUEUE_START;

            String qid;
            try {
                qid                         = queueManager.queueActive(sm);
            } catch (QueueException qe) {
                if (! qe.notifyAdministrator()) {
                    queueManager.chooseBestQueue();
                    qid                     = queueManager.queueWaiting(sm);
                } else {
                    throw qe;
                }
            }
            state                           = ST_QUEUE_OK;
            //out.println("+ OK qid=" + qid);
            //out.flush();

            /* iterate over the children of the request's SOAP
             * Body tag and get a response for each one. */
            SOAPMessage response            = new SOAPMessage();
            while (sm.hasNext()) {
                Element processMe           = sm.next();
                String eltName              = processMe.getName();
                //logger.connection("Processing node: " + eltName);
                String error                = null;
                String className            = "";
                try {
                    state                   = ST_QDISPATCH;
                    Class instantiateMe     = handlerMap.get(eltName);
                    className               = instantiateMe.getName();
                    //logger.connection("Instantiating " + className);
                    Object o                = instantiateMe.newInstance();
                    Handler handler         = (Handler)o;
                    //logger.connection("Dispatch: " + eltName + " -> " + className);

                    /* important part */
                    state                   = ST_QRUN;
                    handler.handle(processMe, response.getBodyElement());
                    //logger.connection(className + " done.");
                } catch (HandlerException he) {
                    error                   = eltName + " (" + className + "):   " + he.toString();
                    logger.generalWarning("Error while running handler");
                    logger.handlerException(he.toString());
                    out.println("- ERR " + he.getMessage());
                    out.flush();
                } catch (InstantiationException ie) {
                    error                   = ie.toString();
                    logger.generalWarning("Cannot instantiate handler class.");
                    logger.midwareError("IA: "+ie.toString());
                } catch (IllegalAccessException iae) {
                    error                   = "Illegal Access: " + iae.toString();
                    logger.generalWarning("IAE: "+iae.toString());
                } catch (Exception e) {
                    error                   = e.toString();
                    logger.generalWarning("Error: " + e.toString());
                }
                if (null != error) {
                    ErrorMailer.addError(error);
                }
            }

            /* queue the response in case anything goes
             * wrong */
            state                           = ST_QRETURN;
            queueManager.queueResponse(response, qid);
            /* ... and remove the request once the
             * response is queued up. */
            queueManager.removeActive(qid);

            try {
                String clearResponse    = response.toString();
                
                clearResponse           = clearResponse.replaceAll("<?xml version=\"1.0\" encoding=\"UTF-8\"?>","");
                //clearResponse           = clearResponse.replaceAll("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Body><m:authUserResponse xmlns:m=\"http://www.terakeet.net/schemas/usadMessage\">", "<JSONauthUserResponse>");
                clearResponse           = clearResponse.replaceAll("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Body>", "");
                clearResponse           = clearResponse.replaceAll(" xmlns:m=\"http://www.terakeet.net/schemas/usadMessage\">", ">");
                clearResponse           = clearResponse.replaceAll(" xmlns:m=\"http://www.terakeet.net/schemas/usadMessage\"/>", "/>");
                clearResponse           = clearResponse.replaceAll("</SOAP-ENV:Body></SOAP-ENV:Envelope>", "");

                //clearResponse           = clearResponse.replaceAll("</m:authUserResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>", "</JSONauthUserResponse>");
                //logger.dbError(clearResponse);
                XML xmlObject           = new XML();
                JSONObject jObject      = new JSONObject();
                jObject                 = xmlObject.toJSONObject(clearResponse);
                out.println(jObject.toString() + "\n&");
                out.flush();
            } catch (Exception e) {
               logger.dbError("getJson error: "+e.getMessage());
           }

            /* since the response has been successfully
             * flushed to the client, go ahead and delete
             * it from the response queue. */
            state                           = ST_CLEANUP;
            queueManager.removeResponse(qid);
        } catch (MessageException me) {
            logger.generalWarning("SOAP message not parseable from input");
            logger.handlerException("SOAP message not parseable from input");
            out.println("- ERR message could not be parsed.");
            out.flush();
        } catch (QueueException qe) {
            logger.generalWarning("ERR message could not be queued; retry later");
            logger.midwareError("Queue error" + (qe.notifyAdministrator() ? " (FATAL)" : "") + ": " + qe.getMessage());
            out.println("- ERR message could not be queued; retry later.");
            out.flush();
        } catch (Exception e) {
            logger.generalWarning("Error: " + e.toString());
            out.println("- ERR System error");
            out.flush();
        }

        return true;
    }

    private String prepareClearStringRespose(String response) {
        response                            = response.replaceAll("\n", "");
        response                            = response.substring(38);
        response                            = response.replaceAll("/>", "></m:addReadingResponse>");
        return response;
    }

}
