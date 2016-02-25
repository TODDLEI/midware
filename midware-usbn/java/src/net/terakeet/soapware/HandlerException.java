/**
 * HandlerException.java
 *
 * @author Ben Ransford
 * @version $Id: HandlerException.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware;

/**
 * Represents a generic handler error -- namely, an error from one of
 * the classes in the <code>java.net.terakeet.soapwarehandlers</code>
 * package.
 */
public class HandlerException extends Exception {
    public HandlerException (String msg) {
	super(msg);
    }

    public HandlerException (Throwable t) {
	super(t);
    }
}
