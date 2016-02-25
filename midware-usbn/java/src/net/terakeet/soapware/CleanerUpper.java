/**
 * CleanerUpper.java
 *
 * @author Ben Ransford
 * @version $Id: CleanerUpper.java,v 1.2 2005/12/21 18:27:30 anonymous Exp $
 */
package net.terakeet.soapware;

// package imports
import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

/**
 * A maintenance worker, meant to be run by a <code>Timer</code> every
 * few seconds or minutes.  The interval at which this task is
 * configurable via the <code>listener.cleanupInterval</code>
 * property.
 */
class CleanerUpper extends TimerTask {
    static Logger logger = Logger.getLogger(CleanerUpper.class.getName());
    private MessageListener listener;
    private Timer timer;
    private File handlerMapFile;
    private long interval;

    /**
     * Default constructor.
     * @param listener The <code>MessageListener</code> that
     * supervises certain elements to which we need access.
     * @param timer The <code>Timer</code> object that runs this task.
     * @param interval The interval at which this task has been set to
     * run.  Note that providing a value here does not actually set
     * the interval at which the task runs; that is controlled by the
     * <code>Timer.schedule()</code> function that actually schedules
     * this task.  However, the value provided there and the
     * <code>interval</code> provided here should be identical in
     * order to avoid unexpected behavior.
     */
    public CleanerUpper (MessageListener listener, Timer timer, long interval /*ms*/) {
	this.listener = listener;
	this.timer = timer;
	this.interval = interval;

	this.handlerMapFile = new File(listener.getHandlerMapPath());
    }

    /**
     * The <code>TimerTask</code>-mandated work method.  Logs some
     * information; checks whether certain files need to be reloaded;
     * logs some more.
     */
    public void run () {
	logger.debug("cleanup; " + listener.threadCount() + "thr");
        DatabaseConnectionManager.cleanup();
	/* if the age of the handler map file is less than one
	 * interval (i.e., if the file has been changed since the last
	 * time this task ran), log some information and reload the
	 * file. */
	if ((System.currentTimeMillis() - handlerMapFile.lastModified()) < interval) {
	    logger.info("Handler map has changed on disk.");
	    listener.reloadHandlerMap();
	}
    }
}
