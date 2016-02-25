/**
 * QueueCleaner.java
 *
 * @author Ben Ransford
 * @version $Id: QueueCleaner.java,v 1.3 2011/04/19 16:28:11 sravindran Exp $
 */
package net.terakeet.soapware.queue;

// package imports.
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;

/**
 * Examines queues and deletes temporary files older than a certain
 * age.  Runs as a thread because we have no idea how long the
 * cleaning process will take.
 */
public class QueueCleaner extends Thread implements QueueManagerListener {
    static MidwareLogger logger = new MidwareLogger(QueueCleaner.class.getName());
    private File queuePath;
    private long INTERVAL, TMPTTL;
    private FileFilter tmpFilter;
	    
    /**
     * @param queuePath the path to a queue that must be tidied.
     */
    public QueueCleaner (File queuePath) {
	this.queuePath = queuePath;
	this.INTERVAL = Long.parseLong(HandlerUtils
				       .getSetting("queue.cleanupInterval"));
	this.TMPTTL = Long.parseLong(HandlerUtils
				     .getSetting("queue.tempfile.ttl"));
	tmpFilter = new FileFilter () {
		public boolean accept (File f) {
		    if (! f.getName().matches("^\\..*\\.tmp$")) {
			return false;
		    }
		    return ((System.currentTimeMillis() -
			     f.lastModified()) > TMPTTL);
		}
	    };
    }

    /**
     * @param newPath the new queue path, to be set whenever the
     * QueueManager switches between queues.
     */
    public void setQueuePath (File newPath) {
	this.queuePath = newPath;
    }

    /**
     * Standard <code>Thread.run()</code> method.  Deletes every file
     * in the queue that ismore than <code>TMPTTL</code> milliseconds
     * old.
     */
    public void run () {
	String[] queueNames = QueueManager.queueNames;

	while (true) {
	    try {
		sleep(INTERVAL);
	    } catch (InterruptedException ie) { }

	    if (! QueueManager.isValidQueue(queuePath)) {
		logger.dbError("No queue to clean up.");
		continue;
	    }
	    String pfx = queuePath.getAbsolutePath() + File.separator;
	    logger.debug("Running QueueCleaner on " + pfx);

	    File dir;
	    for (int i = 0; i < queueNames.length; i++) {
		dir = new File(pfx + queueNames[i]);
		if (dir.canRead() && dir.isDirectory()) {
		    File[] expired = dir.listFiles(tmpFilter);
		    for (int j = 0; j < expired.length; j++) {
			if (! expired[j].delete()) {
			    logger.dbError("Unable to delete expired temp file " +
					expired[j].getAbsolutePath());
			}
		    }
		}
	    }
	}
    }

}
