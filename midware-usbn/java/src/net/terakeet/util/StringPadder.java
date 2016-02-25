/**
 * StringPadder.java
 *
 * @author Ben Ransford
 * @version $Id: StringPadder.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.util;

/**
 * Utility class containing functions that pad <code>String</code>s in
 * various interesting ways.
 */
public class StringPadder {
    public static final boolean LEFT_ALIGN = true;
    public static final boolean RIGHT_ALIGN = false;

    public static String pad (String str, int width, boolean alignment) {
	return pad(str, width, alignment, " ", true);
    }

    public static String pad (String str, int width, boolean alignment,
			      String padWith) {
	return pad(str, width, alignment, padWith, false);
    }
 
    /**
     * Trim, uppercase, and space-pad a <code>String</code> to a
     * certain width and with either left- or right-alignment.
     * @param str the <code>String</code> to pad
     * @param width the width to which <code>str</code> is to be
     * padded (or truncated if <code>width &gt; str.length()</code>)
     * @param alignment one of <code>ALIGN_LEFT</code> or
     * <code>ALIGN_RIGHT</code>.
     * @param padWith character (as a String) to pad with.
     * @param upperCase do we uppercase?
     */
    public static String pad (String str, int width, boolean alignment,
			      String padWith, boolean upperCase) {
	if (str == null) {
	    str = "";
	}
	str = str.trim();
	if (upperCase) {
	    str = str.toUpperCase();
	}

	int len = str.length();
	StringBuffer sb = new StringBuffer();
	if (len < width) { // paddy pad
	    if (alignment == LEFT_ALIGN) {
		sb.append(str);
	    }
	    for (int i = 0; i < (width - len); i++) {
		sb.append(padWith);
	    }
	    if (alignment == RIGHT_ALIGN) {
		sb.append(str);
	    }
	    return sb.toString();
	}
	if (len > width) { // trimmy trim
	    if (alignment == LEFT_ALIGN) {
		return str.substring(0, width);
	    } else {
		return str.substring((width - len), (width-1));
	    }
	}

	return str;
    }
}
