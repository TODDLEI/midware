/**
 * ActiveQueueProcessor.java
 *
 * @author Ben Ransford
 * @version $Id: ActiveQueueProcessor.java,v 1.6 2011/04/19 16:28:19 sravindran Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import net.terakeet.soapware.AsyncMessageProcessor;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.MessageListener;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;

public class ActiveQueueProcessor extends Thread implements QueueProcessor,
							    QueueManagerListener {
    MidwareLogger logger = new MidwareLogger(ActiveQueueProcessor.class.getName());
    //Logger logger = Logger.getLogger(ActiveQueueProcessor.class.getName());
    private File queuePath;
    ExpiredMessageFilter exFilter = new ExpiredMessageFilter();
    private long INTERVAL = 5000l;
    private MessageListener messageListener;

    /**
     * @param queuePath the path to a queue full of "active" messages
     * (messages for which processing has started but may not have
     * finished)
     */
    public ActiveQueueProcessor (File queuePath, MessageListener ml) {
	this.queuePath = queuePath;
	this.INTERVAL = Long.parseLong(HandlerUtils
				       .getSetting("queue.active.processInterval"));
	this.messageListener = ml;
    }

    public void setQueuePath (File newPath) {
	this.queuePath = newPath;
    }

    /**
     * Standard <code>Thread.run()</code> method.  Loops through
     * messages in the "active" queue and tries to claim and process
     * messages whose timeouts have passed.  For each message whose
     * timeout has elapsed, reclaim the message as belonging to this
     * server and then dispatch a handler to process it.  Then write
     * the handler's response to the response queue.
     */
    public void run () {
	/* sleep for some portion of the repeat interval to decrease
	 * the probability that this runs at exactly the same time as
	 * another queue watcher */
	try {
	    sleep((long)(Math.random() * (double)(INTERVAL * 2l)));
	} catch (InterruptedException ie) { }

	while (true) {
	    logger.debug("Running ActiveQueueProcessor");

	    /* grab a list of expired queue files */
	    File subPath = new File(queuePath.getAbsolutePath() +
				    File.separator + "active");
	    File[] expired = subPath.listFiles(exFilter);
	    
	    if (expired != null) {
                if (expired.length > 0) logger.debug("AQP processing "+expired.length+" file(s)");
		for (int i = 0; i < expired.length; i++) {
		    /* claim the message */
		    try {
			File f = QueueManager.claimMessage(expired[i],
							   QueueManager.ACTIVE_TTL);
			if (null != f) {
                            /* dispatch a processor for the message */
                            (new AsyncMessageProcessor(queuePath,
                                                       new File(f.getAbsolutePath())))
                                .setHandlerMap(messageListener.getHandlerMap())
                                .setQueueManager(messageListener.getQueueManager())
                                .start();
                        }
		    } catch (QueueException qe) {
                        logger.dbError("QueueException thrown to AQP: "+qe);
			logger.dbError(qe.getMessage());
		    }
		}
	    }

	    try {
		sleep(INTERVAL);
	    } catch (InterruptedException ie) { }
	}
    }

}
