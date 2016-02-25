/**
 * PrimaryQueueWatcher.java
 *
 * @author Ben Ransford
 * @version $Id: PrimaryQueueWatcher.java,v 1.2 2008/04/09 18:21:20 aastle Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.util.MailException;
import net.terakeet.util.TemplatedMessage;

/**
 * Watches for the reappearance of a directory and moves files (via
 * copy-then-delete) from a less felicitous directory upon this
 * reappearance.  If the desired directory never reappears, notify an
 * administrator.
 */
public class PrimaryQueueWatcher extends Thread {
    static Logger logger = Logger.getLogger(PrimaryQueueWatcher.class.getName());
    private QueueManager queueManager;
    private boolean qWaiting = false;

    /** How often (in milliseconds) to check for the return of the
     * shared queue. */
    public static long RETURN_CHECK_INTERVAL = 1000l; // 1 second

    /** How many times to check for the desired directory's return
     * before notifying an administrator of the failure and then
     * exiting. */
    public static int NUM_TRIES = 1800;

    /**
     * @param qm A <code>QueueManager</code> that is supposed to be
     * worrying about various queues.
     * @throws IOException if the path named by
     * <code>alternateQueuePath</code> is not accessible.  Obviously
     * it is expected that the path referred to by
     * <code>preferredQueuePath</code> is not always accessible.
     */
    public PrimaryQueueWatcher (QueueManager qm) { // throws IOException {
	logger.info("PrimaryQueueWatcher started.");
	this.queueManager = qm;
    }

    /**
     * Notifies an administrator via email that the queue never
     * reappeared.
     * @param errMsg an error message to inform the administrator what
     * is wrong.
     */
    private void sendNotificationEmail (String errMsg) {
	String emailTemplatePath = HandlerUtils.getSetting("email.templatePath");
	try {
	    TemplatedMessage msg =
		new TemplatedMessage("ERROR on server",
				     emailTemplatePath,
				     "adminError");
	    msg.setSender("nobody");
	    msg.setRecipient(HandlerUtils.getSetting("server.admin"));
	    msg.setField("SERVER_ID", HandlerUtils.getSetting("server.id"));
	    msg.setField("ERROR", errMsg);
	    
	    msg.send();
	} catch (MailException me) {
	    logger.warn("Couldn't send email to administrator!");
	}
    }

    /**
     * Standard <code>Thread.run()</code> method.  Checks every
     * <code>RETURN_CHECK_INTERVAL</code> milliseconds whether the
     * shared queue is accessible.  Sends a notification email to an
     * administrator if <code>NUM_TRIES</code> checks have failed.
     */
    public void run () {
	int ct = 0;
	boolean queueAvailable = false;
	File priQueue = queueManager.getPrimaryQueue();
	qWaiting = true;
	while (!queueAvailable && (ct < NUM_TRIES)) {
	    try {
		sleep(1000 /* ms */);
	    } catch (InterruptedException ie) { }
	    logger.debug("PrimaryQueueWatcher run " + ct + " of " + NUM_TRIES);
	    if (queueManager.isValidQueue(priQueue)) {
		try {
		    queueManager.chooseBestQueue(true);
		    queueManager.moveAlternateToPrimary();
		} catch (QueueException qe) {
		    continue;
		}
		logger.info("Queue has returned.");
		qWaiting = false;
		queueAvailable = true;
	    }
	    ct++;
	}

	if (!queueAvailable) {
	    sendNotificationEmail("Primary queue (" + priQueue.getAbsolutePath() +
				  ") has been missing for " +
				  String.valueOf(NUM_TRIES) + " seconds.");
	}
    }

    /**
     * @return <code>true</code> if this PrimaryQueueWatcher is currently
     * waiting for a primary queue to reappear.
     */
    public boolean waitingForQueue () {
	return qWaiting;
    }

}
