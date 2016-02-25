/**
 * QueueManager.java
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;
import net.terakeet.util.MD5;
import net.terakeet.soapware.MessageException;
import net.terakeet.soapware.MessageListener;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.util.FileCopier;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;

public class QueueManager {
    static MidwareLogger logger = null;
    static RunningMessages curMsgs = RunningMessages.getInstance();

    private static boolean queueAvailable = false, canRecover = true;
    File primaryQueue = null, alternateQueue = null,
	currentQueue = null;
    private static String serverId = null;
    public static long ACTIVE_TTL, WAITING_TTL, RESPONSE_TTL;
    private static final long QUEUE_MAX_WAIT = 30000;

    /** Subdirectory names; these exist in a valid queue. */
    public static final String[] queueNames = {"active", "waiting", "responses"};

    private PrimaryQueueWatcher watcher;

    private FileFilter xmlFilter = new FileFilter() {
	    public boolean accept (File f) {
		return f.getName().endsWith(".xml");
	    }
	};

    private Vector listeners = new Vector();
    
    /**
     * Checks whether a given directory is a valid top-level queue
     * directory.  Tries to make subdirectories if it can't find them.
     * @param d a <code>File</code> representation of the full path to
     * a possibly valid queue directory.
     * @return <code>true</code> if the directory named by
     * <code>d</code> is a valid writable queue directory;
     * <code>false</code> otherwise.
     */
    public static boolean isValidQueue (File d) {
	if (! (d.exists() && d.isDirectory() && d.canWrite())) {
	    return false;
	}
	String pfx = d.getAbsolutePath() + File.separator;

	File f;
	for (int i = 0; i < queueNames.length; i++) {
	    f = new File(pfx + queueNames[i]);
	    if (!f.exists() && !f.isDirectory() && !f.mkdir()) {
		return false;
	    }
	}

	return true;
    }

    /**
     * @param primaryQueuePath the full path to the primary message
     * queue
     * @param alternateQueuePath the full path to the alternate
     * message queue
     * @throws QueueException if either path parameter is not a proper
     * queue or cannot be made into a proper queue.
     */
    public QueueManager (String primaryQueuePath, String alternateQueuePath,
			 MessageListener ml) throws QueueException {
        logger                              = new MidwareLogger(QueueManager.class.getName());
	try {
	    this.primaryQueue = new File(primaryQueuePath).getCanonicalFile();
	} catch (IOException ioe) {
	    throw new QueueException("Cannot canonicalize primary queue path.");
	}
	if (! isValidQueue(primaryQueue)) {
	    throw new QueueException("Primary queue is not accessible.");
	}

	try {
	    this.alternateQueue = new File(alternateQueuePath).getCanonicalFile();
	} catch (IOException ioe) {
	    throw new QueueException("Cannot canonicalize alternate queue path.");
	}
	if (! isValidQueue(alternateQueue)) {
	    throw new QueueException("Alternate queue is not accessible.");
	}

	currentQueue = primaryQueue;
	this.queueAvailable = true;
	serverId = ml.getServerId();

	try {
	    QueueManager.ACTIVE_TTL =
		Long.parseLong(ml.getSetting("queue.active.ttl"));
	} catch (NumberFormatException nfe) {
	    QueueManager.ACTIVE_TTL = 30000l;
	    logger.dbError("Bad value for queue.active.ttl property.");
	}
	try {
	    QueueManager.WAITING_TTL =
		Long.parseLong(ml.getSetting("queue.waiting.ttl"));
	} catch (NumberFormatException nfe) {
	    QueueManager.WAITING_TTL = 30000l;
	    logger.dbError("Bad value for queue.active.ttl property.");
	}
	try {
	    QueueManager.RESPONSE_TTL =
		Long.parseLong(ml.getSetting("queue.response.ttl"));
	} catch (NumberFormatException nfe) {
	    QueueManager.RESPONSE_TTL = 30000l;
	    logger.dbError("Bad value for queue.active.ttl property.");
	}

	this.watcher = new PrimaryQueueWatcher(this);
	
	moveAlternateToPrimary();
    }

    /**
     * Queues a message to the "active" queue.
     * @param msg a message to be queued
     * @return a new queueID that refers to the new message
     * @throws QueueException if a fatal queuing error occurred.
     */
    public String queueActive (SOAPMessage msg) throws QueueException {
	return queueGeneric(msg, ACTIVE_TTL, "active", null);
    }

    /**
     * Queues a message to the "waiting" queue.
     * @param msg a message to be queued
     * @return a new queueID that refers to the new message
     * @throws QueueException if a fatal queuing error occurred.
     */
    public String queueWaiting (SOAPMessage msg) throws QueueException {
	return queueGeneric(msg, WAITING_TTL, "waiting", null);
    }

    /**
     * Queues a message to the "responses" queue.
     * @param msg a message to be queued
     * @param qid a unique message identifier
     * @throws QueueException if a fatal queuing error occurred.
     */
    public void queueResponse (SOAPMessage msg, String qid) throws QueueException {
	queueGeneric(msg, RESPONSE_TTL, "responses", qid, false);
    }

    /**
     * Queues a message in a given queue, optionally with a given
     * queue ID.
     * @param msg a message to be queued
     * @param ttl the message's time-to-live
     * @param dir the directory to which 
     * @param qid a unique <code>String</code> that identifies this
     * message.
     * @throws QueueException if the message cannot be queued in the 
     */
    private String queueGeneric (SOAPMessage msg, long ttl,
				 String dir, String qid) throws QueueException {
	return queueGeneric(msg, ttl, dir, qid, true);
    }

    /**
     * Queues a message in a given queue, optionally with a given
     * queue ID.
     * @param msg a message to be queued
     * @param ttl the message's time-to-live
     * @param dir the directory to which 
     * @param qid a unique <code>String</code> that identifies this
     * message.
     * @param useServerId whether the queued filename should include
     * the server's unique ID.
     * @throws QueueException if the message cannot be queued in the 
     */
    private String queueGeneric (SOAPMessage msg, long ttl, String dir, String qid, boolean useServerId) throws QueueException {
	String msgSum                       = (new MD5(msg.toString())).calc().toString();

	while (!queueAvailable && canRecover) {
	    try {
		// wait for the queue to become available (say, it's being set)
		wait(QUEUE_MAX_WAIT);
	    } catch (InterruptedException ie) {
	    }
	}

	String pfx                          = currentQueue.getAbsolutePath() + File.separator + dir + File.separator;

	/* write the message to a temp file in the queue */
	String tmpFilename                  = pfx + "." + serverId + "," +
        (int)(Math.random() * 2147483647 /* 2^31 - 1 */) + ".tmp";
        FileWriter tmpWriter                = null;
	try {
            tmpWriter                       = new FileWriter(tmpFilename);
	    msg.writeTo(tmpWriter);
	} catch (IOException ioe) {
	    throw new QueueException(ioe.getMessage(), false);
	} finally {
            if (null != tmpWriter) {
                try {
                    tmpWriter.close();
                } catch (IOException ignore) {}
            }
	}

	/* rename the temp file to a real filename of the form
	 * "s,t,0123456789ABCDEF0123456789ABCDEF,1000000000.xml",
	 * where s is this server's identifier (unique within its
	 * cluster), t is the TTL of the message, the 32-character hex
	 * string is the message's MD5 checksum, and the fourth field
	 * (the length of which will change sometime in the year 2286)
	 * is the UNIX time (the number of milliseconds since 1 Jan
	 * 1970). */
	String queueId                      = (qid != null) ? qid : msgSum + "," + System.currentTimeMillis();
	String newFilename                  = pfx + (useServerId ? (serverId + ",") : "") + String.valueOf(ttl / 1000l) + "," + queueId + ".xml";
	//logger.connection("Writing to queue file " + newFilename);
        
        // if the directory is the active queue, the process is running
        if ("active".equals(dir)) {
            curMsgs.add(newFilename);
        }
	try {
	    (new File(tmpFilename)).renameTo(new File(newFilename));
	} catch (SecurityException se) {
            curMsgs.remove(newFilename);
	    throw new QueueException(se.getMessage());
	}
        //logger.connection("queueID: " + queueId);
	return queueId;
    }

    /**
     * Removes a message from the "active" queue.
     * @param qid the (unique) queue ID of the message
     * @throws QueueException if the message cannot be removed.
     */
    public void removeActive (String qid) throws QueueException {
	removeGeneric("active", ACTIVE_TTL, qid);
    }

    /**
     * Removes a message from the "active" queue.
     * @param qid the (unique) queue ID of the message
     * @throws QueueException if the message cannot be removed.
     */
    public void removeWaiting (String qid) throws QueueException {
	removeGeneric("waiting", WAITING_TTL, qid);
    }

    /**
     * Removes a message from the "active" queue.
     * @param qid the (unique) queue ID of the message
     * @throws QueueException if the message cannot be removed.
     */
    public void removeResponse (String qid) throws QueueException {
	removeGeneric("responses", RESPONSE_TTL, qid, false);
    }

    private void removeGeneric (String queue, long ttl, String qid)
	throws QueueException {
	removeGeneric(queue, ttl, qid, true);
    }

    /**
     * Removes a message from a queue.
     * @param queue the name of the queue from which to remove the
     * message
     * @param ttl the (per-type) TTL of the message to be deleted
     * @param qid the (unique) queue ID of the message to be deleted
     * @param useServerId whether to use the (unique) server ID when
     * assembling the filename to remove.
     * @throws QueueException if the message cannot be removed.
     */
    private void removeGeneric (String queue, long ttl, String qid, boolean useServerId) throws QueueException {
	while (!queueAvailable && canRecover) {
	    try {
		// wait for the queue to become available (say, it's being set)
		wait(QUEUE_MAX_WAIT);
	    } catch (InterruptedException ie) {
	    }
	}

	String pfx                          = currentQueue.getAbsolutePath() + File.separator + queue + File.separator;
	String filename                     = pfx + (useServerId ? (serverId + ",") : "") + String.valueOf(ttl / 1000l) + "," + qid + ".xml";
	//logger.connection("About to delete " + filename);
	if ((new File(filename)).delete()) {
            // message is no longer running
            curMsgs.remove(filename);
        } else {
	    logger.dbError("Unable to delete " + filename + " from " + queue + " queue.");
	    throw new QueueException ("Could not delete file.");
	}
    }

    /**
     * Fetches a response from the queue and returns it as a
     * <code>SOAPMessage</code>.
     * @return a <code>SOAPMessage</code> representation of the
     * requested message.
     * @param qid the (unique) queue ID of the message to be fetched.
     * @throws QueueException if the response is not ready yet or if it
     */
    public SOAPMessage fetchResponse (String qid) throws QueueException {
	while (!queueAvailable && canRecover) {
	    try {
		wait(QUEUE_MAX_WAIT);
	    } catch (InterruptedException ie) {
	    }
	}

	String pfx = currentQueue.getAbsolutePath() + File.separator +
	    "responses" + File.separator;
	String filename = pfx + String.valueOf(RESPONSE_TTL / 1000l) +
	    "," + qid + ".xml";
	File f = new File(filename);
	logger.debug("Fetching " + filename + " from queue.");
	if (f.canRead()) {
	    try {
		return SOAPMessage.instanceFromFile(f);
	    } catch (MessageException me) {
		throw new QueueException((Throwable)me);
	    }
	} else {
	    /* this is a 'debug' instead of a 'warn' because this will
	     * occur every time a response is not yet ready. */
	    logger.debug("Unable to fetch " + filename + " from queue.");
	    throw new QueueException("Could not fetch file from queue.");
	}
    }

    public QueueManagerListener addListener (QueueManagerListener l) {
	listeners.add(l);
	return l;
    }
    
    /**  Set all living listeners to point to the current Queue.
     */
    private void notifyListeners () {
	logger.debug("Notifying " + listeners.size() +
		     " QueueManagerListeners of queue change.");
	for (Iterator i = listeners.iterator(); i.hasNext(); ) {
	    QueueManagerListener l = (QueueManagerListener)(i.next());
	    if (l.isAlive()) {
		l.setQueuePath(currentQueue);
	    }
	}
    }

    public synchronized void chooseBestQueue () throws QueueException {
	chooseBestQueue(false);
    }

    public synchronized void chooseBestQueue (boolean force) throws QueueException {
	if (!force && watcher.isAlive() && watcher.waitingForQueue()) {
	    logger.debug("Watcher is already alive and waiting for a queue to return");
	    return;
	}

	boolean tellListeners = false;
	queueAvailable = false;

	if (isValidQueue(primaryQueue)) {
	    logger.debug("Switching to primary queue.");
	    currentQueue = primaryQueue;
	    tellListeners = true;
	} else {
	    logger.dbError("Primary queue is inaccessible; switching to " +
			"alternate queue.");
	    canRecover = true;
	    if (isValidQueue(alternateQueue)) {
		currentQueue = alternateQueue;
		logger.debug("Switch to alternate queue succeeded." +
			     "  Starting PrimaryQueueWatcher thread.");
		tellListeners = true;
		if (!watcher.isAlive()) {
		    watcher = new PrimaryQueueWatcher(this);
		    watcher.start();
		}
	    } else {
		logger.dbError("NO VALID QUEUE AVAILABLE!");
		canRecover = false;
		throw new QueueException("No valid queue available.");
	    }
	}

	/* notify all objects waiting to queue messages that the queue
	 * has become available. */
	queueAvailable = true;
	canRecover = true;
	notifyAll();
	if (tellListeners) notifyListeners();
    }

    /**
     * Moves the contents of the alternate queue to the primary queue.
     */
    public void moveAlternateToPrimary () throws QueueException {
	queueAvailable = false;
	int i, j;
	File altPath;
	String priPath;
	try {
	    int totalMoved = 0;
	    for (i = 0; i < queueNames.length; i++) {
		altPath = new File(alternateQueue.getAbsolutePath() + File.separator +
				   queueNames[i]);
		priPath = primaryQueue.getAbsolutePath() + File.separator +
		    queueNames[i] + File.separator;
		File[] xmlFiles = altPath.listFiles(xmlFilter);
		if (xmlFiles != null) { // might be null if alt. queue is gone
		    for (j = 0; j < xmlFiles.length; j++) {
			FileCopier.move(xmlFiles[j],
					new File(priPath + xmlFiles[j].getName()));
		    }
		    totalMoved += j;
		}
	    }
	    logger.debug("Moved " + totalMoved + " files from alternate " +
			"to primary queue.");
	} catch (IOException ioe) {
	    throw new QueueException((Throwable)ioe);
	}
	queueAvailable = true;
    }

    public File getPrimaryQueue () {
	return this.primaryQueue;
    }

    public File getAlternateQueue () {
	return this.alternateQueue;
    }

    public File getCurrentQueue () {
	return this.currentQueue;
    }

    public boolean isQueueAvailable () {
	return this.queueAvailable;
    }

    public static File claimMessage (File expiredMessage, long newttl)
	throws QueueException {
	String name = expiredMessage.getName();
	String[] nameParts = name.split(",");
	/* example: "s,t,0123456789ABCDEF0123456789ABCDEF,1000000000.xml" */
	long ts = System.currentTimeMillis();

	if (nameParts.length == 4) {
	    String oldDir = expiredMessage.getAbsoluteFile().getParent();
	    String newFilename = (oldDir != null ? (oldDir + File.separator) : "") +
		serverId + "," + String.valueOf(newttl / 1000l) + "," +
		nameParts[2] + "," + String.valueOf(ts) + ".xml";
            
	    File ret = null;
            
            if (curMsgs.rename(expiredMessage.getAbsolutePath(), newFilename)) {
                ret = new File(newFilename);
                if (!expiredMessage.renameTo(ret)) {
                    curMsgs.remove(newFilename);
                    throw new QueueException("Unable to rename queue file '"+expiredMessage.getAbsolutePath()+"' to '"
                            +newFilename+"'");
                }
                ret.setLastModified(ts);
            }
	    return ret;
	} else if (curMsgs.add(expiredMessage.getAbsolutePath())) {
	    expiredMessage.setLastModified(ts);
	    return expiredMessage;
        } else {
            return null;
	}
    }
}
