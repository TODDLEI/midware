/**
 * ExpiredMessageFilter.java
 *
 * @author Ben Ransford
 * @version $Id: ExpiredMessageFilter.java,v 1.5 2007/10/23 20:27:31 anonymous Exp $
 */
package net.terakeet.soapware.queue;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpiredMessageFilter implements FileFilter {
    private static final Pattern fnPtn =
	Pattern.compile("^.*,([0-9]+),[0-9a-fA-F]{32},[0-9]+\\.xml$");
    private static final RunningMessages curMsgs = RunningMessages.getInstance();


    /**
     * @param f a <code>File</code> to be checked for expiredness.
     * @return <code>true</code> if a message is "expired" (i.e., is
     * older than its TTL, which TTL is determined from the message's
     * filename, <code>f</code>), <code>false</code> otherwise.
     * Also check that the file is still not currently being processed
     * by an existing thread.
     */
    public boolean accept (File f) {
	Matcher m = fnPtn.matcher(f.getName());
	if (m.matches() && !curMsgs.contains(f.getAbsolutePath())) {
	    try {
		return ((Long.parseLong(m.group(1)) * 1000)
			< (System.currentTimeMillis() - f.lastModified()));
	    } catch (NumberFormatException nfe) {
		return false;
	    }
	} else {
	    return false;
	}
    }

}
