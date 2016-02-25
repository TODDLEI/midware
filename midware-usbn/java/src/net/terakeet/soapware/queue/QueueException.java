/**
 * QueueException.java
 *
 * @author Ben Ransford
 * @version $Id: QueueException.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware.queue;

public class QueueException extends Exception {
    private boolean sendNotify = false;

    public QueueException (String msg) {
	this(msg, false);
    }

    public QueueException (String msg, boolean notify) {
	super(msg);
	sendNotify = notify;
    }

    public QueueException (Throwable t) {
	this(t, false);
    }

    public QueueException (Throwable t, boolean notify) {
	super(t);
	sendNotify = notify;
    }

    public boolean notifyAdministrator () {
	return sendNotify;
    }

}
