/**
 * TestFailure.java
 *
 * @author Ben Ransford
 * @version $Id: TestFailure.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.test;

public class TestFailure extends Exception {
    public TestFailure (String msg) {
	super(msg);
    }
}
