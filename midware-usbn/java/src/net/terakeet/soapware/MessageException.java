/**
 * MessageException.java
 *
 * @author Ben Ransford
 * @version $Id: MessageException.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.soapware;

/**
 * Represents errors that may occur during the parsing or assembly of
 * SOAP messages.
 */
public class MessageException extends Exception {
    public MessageException (String msg) {
	super(msg);
    }
    public MessageException (Throwable t) {
	super(t);
    }
}
