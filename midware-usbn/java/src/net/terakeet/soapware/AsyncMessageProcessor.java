/**
 * AsyncMessageProcessor.java
 *
 * @author Ben Ransford
 * @version $Id: AsyncMessageProcessor.java,v 1.20 2011/04/19 16:27:50 sravindran Exp $
 */
package net.terakeet.soapware;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.terakeet.soapware.queue.QueueException;
import net.terakeet.soapware.queue.QueueManager;
import net.terakeet.soapware.queue.RunningMessages;
import net.terakeet.util.MidwareLogger;
import org.dom4j.Element;
import org.apache.log4j.Logger;

/**
 * Handles an incoming message-processing session.  Passes SOAP
 * requests on to the correct handlers; returns error information if
 * applicable.
 */
public class AsyncMessageProcessor extends Thread {
    private MidwareLogger logger = null;
    static RunningMessages curMsgs = RunningMessages.getInstance();
    private File queuePath, queuedMessage;
    private HandlerMap handlerMap;
    private QueueManager queueManager;

    private static final Pattern qidPtn =
	Pattern.compile("^.*,([0-9a-fA-F]{32},[0-9]+)\\.xml$");

    /** Create a processor to handle one message. Call <code>run</code to
     *  start the processing.
     *  @param queuePath the location of directory that holds the queue
     *  @param msg the message to process
     */
    public AsyncMessageProcessor (File queuePath, File msg) {
	this.queuePath = queuePath;
	this.queuedMessage = msg;
    }

    public AsyncMessageProcessor setHandlerMap (HandlerMap hm) {
	this.handlerMap = hm;
	return this;
    }

    public AsyncMessageProcessor setQueueManager (QueueManager qm) {
	this.queueManager = qm;
	return this;
    }

    /**
     * Process the message.
     */
    public void run () {
        logger = new MidwareLogger(AsyncMessageProcessor.class.getName());
	logger.debug("Processing " + queuedMessage + " from " + queuePath);
        logger.queueDebug("Processing " + queuedMessage + " from " + queuePath);
	SOAPMessage msg;
	try {
	    msg = SOAPMessage.instanceFromFile(queuedMessage);
	} catch (MessageException me) {
            logger.queueError("MessageException from AsyncMP.run(): "+me);
	    logger.midwareError(me.getMessage());
	    return;
	}

	/* iterate over the children of the request's SOAP
	 * Body tag and get a response for each one. */
	SOAPMessage response = new SOAPMessage();
	while (msg.hasNext()) {
	    Element processMe = msg.next();
            String className = "";
	    String eltName = processMe.getName();
	    logger.debug("Processing node: " + eltName);
            String error = null;
	    try {
		Class instantiateMe = handlerMap.get(eltName);
		className = instantiateMe.getName();
		logger.debug("Instantiating " + className);
		Object o = instantiateMe.newInstance();
		Handler handler = (Handler)o;
		logger.debug("Dispatch: " + eltName +
			     " -> " + className);
		
		/* important part */
		handler.handle(processMe, response.getBodyElement());
		
		logger.debug(className + " done.");
	    } catch (HandlerException he) {
                error = eltName + " (" + className + "):   " + he.toString();
		logger.dbError("Error while running handler");
                logger.dbError("HandlerException from AsyncMP during handler.handle: "+he);
		logger.handlerException(he.toString());
	    } catch (InstantiationException ie) {
                error = ie.toString();
                logger.dbError("InstantiantionException from AsyncMP during handler.handle: "+ie);
		logger.dbError("Cannot instantiate handler class.");
		logger.midwareError(ie.toString());
	    } catch (IllegalAccessException iae) {
                error = "Illegal Access: " + iae.toString();
                logger.dbError("IllegalAccessException from AsyncMP during handler.handle: "+iae);
		logger.midwareError(iae.toString());
	    } catch (Exception e) {
                error = e.toString();
                logger.midwareError("Error: " + e.toString());
            }
            if (null != error) {
                ErrorMailer.addError(error);
            }
	}
	
	/* queue the response in case anything goes
	 * wrong */
	Matcher m = qidPtn.matcher(queuedMessage.getName());
	if (m.matches()) {
	    String qid = m.group(1);
	    try {
                logger.debug("queuing response");
		queueManager.queueResponse(response, qid);
	    } catch (QueueException qe) {
                logger.dbError("QueueException from AsyncMP during queueManager.queueResponse: "+qe);
		logger.midwareError("Error while queuing response: " + qe.getMessage());
	    }
	} else {
            logger.dbError("Queued message has improper qid; response would fail.");
	    logger.midwareError("Queued message has improper qid; response would fail." +
			"  Deleting.");
	}

	/* all went well; delete the original message */
        logger.queueDebug("Deleting "+ queuedMessage + " from " + queuePath);
	if (queuedMessage.delete()) {
            logger.queueDebug("Delete successful");
            curMsgs.remove(queuedMessage.getAbsolutePath());
        } else {
            logger.queueDebug("Could not delete file");
        }
    }
}
