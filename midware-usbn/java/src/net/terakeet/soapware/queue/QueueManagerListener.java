/**
 * QueueManagerListener.java
 *
 * @author Ben Ransford
 * @version $Id: QueueManagerListener.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;

public interface QueueManagerListener {
    void setQueuePath (File f);
    boolean isAlive (); // actually from java.lang.Thread...
    void start ();
}
