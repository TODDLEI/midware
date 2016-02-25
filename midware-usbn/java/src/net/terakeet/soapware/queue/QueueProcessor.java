/**
 * QueueProcessor.java
 *
 * @author Ben Ransford
 * @version $Id: QueueProcessor.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.IOException;

/**
 * Examines a queue and claims and processes messages (requests) that
 * are waiting to be processed or answered.
 */
public interface QueueProcessor {
    /**
     * Standard <code>Thread.run()</code> method.  Processes messages
     * that are queued up for processing.  Works around such
     * inconveniences as locking, server failures, ...
     */
    void run ();

}
