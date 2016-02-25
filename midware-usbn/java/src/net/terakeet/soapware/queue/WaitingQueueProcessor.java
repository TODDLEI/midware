/**
 * WaitingQueueProcessor.java
 *
 * @author Ben Ransford
 * @version $Id: WaitingQueueProcessor.java,v 1.2 2007/10/23 19:36:44 anonymous Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import net.terakeet.soapware.AsyncMessageProcessor;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.MessageListener;
import net.terakeet.util.FileCopier;
import org.apache.log4j.Logger;

public class WaitingQueueProcessor extends Thread implements QueueProcessor,
							     QueueManagerListener {
    static Logger logger = Logger.getLogger(WaitingQueueProcessor.class.getName());
    private File queuePath;
    ExpiredMessageFilter exFilter = new ExpiredMessageFilter();
    private long INTERVAL = 5000l;
    private MessageListener messageListener;

    /**
     * @param queuePath the path to a queue full of messages waiting
     * to be processed.
     */
    public WaitingQueueProcessor (File queuePath, MessageListener ml) {
	this.queuePath = queuePath;
	this.INTERVAL = Long.parseLong(HandlerUtils
				       .getSetting("queue.waiting.processInterval"));
	this.messageListener = ml;
    }

    public void setQueuePath (File newPath) {
	this.queuePath = newPath;
    }

    /**
     * Standard <code>Thread.run()</code> method.  Loops through
     * messages in the "waiting" queue and tries to claim and process
     * messages that are older than their timeouts (thereby giving the
     * original owner a chance to "get around to" processing a message
     * once it queues the message).
     */
    public void run () {
	/* sleep for some portion of the repeat interval to decrease
	 * the probability that this runs at exactly the same time as
	 * another queue watcher */
	try {
	    sleep((long)(Math.random() * (double)(INTERVAL * 2l)));
	} catch (InterruptedException ie) { }

	while (true) {
	    logger.debug("Running WaitingQueueProcessor");

	    /* grab a list of expired queue files */
	    File subPath = new File(queuePath.getAbsolutePath() +
				    File.separator + "waiting");
	    File[] expired = subPath.listFiles(exFilter);
	    
	    if (expired != null) {
		for (int i = 0; i < expired.length; i++) {
		    /* claim the message */
		    try {
			File claimed = QueueManager.claimMessage(expired[i],
								 QueueManager.ACTIVE_TTL);
                        if (null != claimed) {
                            File active = new File(queuePath.getAbsolutePath() +
                                                   File.separator + "active" +
                                                   File.separator + claimed.getName());
                            FileCopier.move(claimed, active);
			
                            /* dispatch a processor for the message */
                            (new AsyncMessageProcessor(queuePath,
                                                       active.getAbsoluteFile()))
                                .setHandlerMap(messageListener.getHandlerMap())
                                .setQueueManager(messageListener.getQueueManager())
                                .start();
                        }
		    } catch (QueueException qe) {
			logger.warn(qe.getMessage());
		    } catch (IOException ioe) {
			logger.warn("Couldn't create entry in active queue.");
		    }
		}
	    }

	    /* claim a new copy of the message file to the active queue
	     * (via clever file naming technique).  if this fails, touch
	     * the lastModified attribute of the original file in the
	     * waiting queue and bail out. */
	    /* `touch' the new file's lastModified attribute in the active
	     * queue. */
	    /* dispatch a handler to process the message from the active
	     * queue. */
	    /* write the handler's response to the response queue. */
	    try {
		sleep(INTERVAL);
	    } catch (InterruptedException ie) { }
	}
    }
}
