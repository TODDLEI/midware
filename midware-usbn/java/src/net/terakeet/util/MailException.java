/**
 * MailException.java
 *
 * @author Ben Ransford
 * @version $Id: MailException.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.util;

/**
 * An email-related exception, to be thrown whenever something goes
 * wrong with email communication(s).
 */
public class MailException extends Exception {

    public MailException (String msg) {
	super(msg);
    }

    public MailException (Throwable t) {
	super(t);
    }

}
