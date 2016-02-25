/**
 * HandlerUtils.java
 *
 * @author Ben Ransford
 * @version $Id: HandlerUtils.java,v 1.10 2015/11/04 21:06:51 sravindran Exp $
 */

package net.terakeet.soapware;

// package imports.
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import net.terakeet.soapware.security.*;


/**
 * Utility class containing various functions needed by classes that
 * implement the <code>net.terakeet.soapware.Handler</code> interface.
 * Note: this class must be initialized before use; the correct way to
 * do this is to call <code>HandlerUtils.initialize(this)</code> from
 * a <code>MessageListener</code>.
 */
public class HandlerUtils {
    static Logger logger = Logger.getLogger(HandlerUtils.class.getName());
    private static String tdsDriver = null;
    private static String tdsPrefix = null;
    private static MessageListener listener = null;

    /**
     * @return the name of the TDS (Sybase, MSSQL, etc.) driver being
     * used.  This will be a fully-qualified class name.
     */
    public static String getTDSDriverName () {
	return tdsDriver;
    }

    /**
     * @return the prefix to be used for JDBC URIs.
     */
    public static String getTDSPrefix () {
	return tdsPrefix;
    }

    /**
     * Readies this class for use.
     * @param ml a <code>MessageListener</code> object that has access
     * to a collection of application settings.
     */
    public static void initialize (MessageListener ml) throws HandlerException {
	if (listener == null) {
	    listener = ml;
	} else {
	    return;
	}

	String dvr = ml.getSetting("handlers.tdsDriver");
	if ((dvr == null) || "".equals(dvr)) {
	    throw new HandlerException("The \"handlers.tdsDriver\" property is not set in " +
				       "the properties file.  You must set this if you wish " +
				       "to connect to a database via TDS.");
	}
	tdsDriver = dvr;

	String pfx = ml.getSetting("handlers.tdsPrefix");
	tdsPrefix = ((pfx != null) && !"".equals(pfx))
	    ? pfx
	    : "jdbc:mysql";
    }

    /**
     * Initializes the database driver for use -- basically just makes
     * sure the right class exists and is instantiable.
     * @param driverName the name of the driver to be instantiated
     * @throws HandlerException if the requested class cannot be
     * instantiated.
     */
    public static void initializeDriver (String driverName) throws HandlerException {
	try {
	    Class.forName(driverName).newInstance();
	} catch (Exception e) {
	    throw new HandlerException(e);
	}
    }

    /**
     *  Checks to see if the clientKeyManager has loaded a secret key, and if not,
     *  set up the path and loads the key.
     */
    public static void initializeClientKeyManager() {
        if (!ClientKeyManager.isKeyLoaded()) {
            ClientKeyManager.setKeyPath(HandlerUtils.getSetting("security.clientkey"));
        }
    }
    
    /**
     * Connects to the database specified by <code>dbURL</code>.
     * @param dbURL the URL of a database to connect.
     * @throws HandlerException if the database cannot be reached or
     * is specified erroneously or incompletely.
     */
    public static Connection connectTo (String dbURL) throws HandlerException {
	Connection conn = null;
	try {
	    logger.debug("Connecting to database: " + dbURL);
	    DriverManager.setLoginTimeout(5);
	    conn = DriverManager.getConnection(dbURL);
	    logger.debug("Connection succeeded.");
	} catch (Exception e) {
	    logger.warn("Connection failed.");
	    throw new HandlerException(e);
	}
	return conn;
    }

    /**
     * Gets the value of an application-wide setting.
     * @param propName the setting whose value is to be queried
     * @return the setting (from this object's associated
     * ModuleListener's store) named by <code>propName</code>.
     */
    public static String getSetting (String propName) {
	return listener.getSetting(propName);
    }

    public static String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }
    
    /** If the String s is null, will return the second string
     */
    public static String nullToString(String s, String whenNull) {
        return (null == s) ? whenNull : s;
    }

    
    /**
     * Gets an integer-valued field from an XML Element family that
     * looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;326358&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing an
     * integer value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child does not contain an integer or if the child is
     * not present underneath <code>parent</code> at all.
     */
    public static int getRequiredInteger (Element parent, String tagName)
	throws HandlerException {
	Element el = parent.element(tagName);
	if (el != null) {
	    int ret;
	    String retStr = el.getTextTrim();
	    if ((retStr == null) || "".equals(retStr)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    }
	    try {
		ret = Integer.valueOf(retStr).intValue();
	    } catch (NumberFormatException nfe) {
                try {
                    float retFloat = Float.parseFloat(retStr);
                    ret = Math.round(retFloat);
                } catch (NumberFormatException nfe2) {
                    throw new HandlerException("Malformed integer value in required " +
					   "field '" + tagName + "'.");
                }
	    }
	    return ret;
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
    }

    /**
     * Gets a required child element from a parent element.
     * @param parent the ostensible parent of the required child
     * element.
     * @param tagName the tag name of the required child element.
     * @return the required child <code>Element</code>.
     * @throws HandlerException if the required child element is
     * missing.
     */
    public static Element getRequiredElement (Element parent, String tagName)
	throws HandlerException {
	Element el = parent.element(tagName);
	if (el != null) {
	    return el;
	} else {
	    throw new HandlerException("A '" + tagName + "' element is required.");
	}
    }

    
    public static final int NULL_INTEGER = -999;
    /**
     * Gets an integer-valued field from an XML Element family that
     * looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the ostensible parent of the tag whose value is
     * desired.
     * @param tagName the name of the tag whose value is desired.
     * @return the value of the requested child of
     * <code>parent</code>, or <code>-1</code> if the child is present
     * but empty, or <code>-999</code> if the child is not present.
     * Callers should check for a return value greater than or equal
     * to zero.
     * @throws HandlerException if the integer value is malformed.
     */
    public static int getOptionalInteger (Element parent, String tagName)
	throws HandlerException {
	Element el = parent.element(tagName);
	if (el != null) {
	    int ret;
	    String retStr = el.getTextTrim();
	    if ((retStr == null) || "".equals(retStr)) {
		return -1;
	    }
	    try {
		ret = Integer.valueOf(retStr).intValue();
	    } catch (NumberFormatException nfe) {
		throw new HandlerException("Malformed integer in optional " +
					   "field '" + tagName + "'.");
	    }
	    return ret;
	} else {
	    return NULL_INTEGER;
	}
    }

    /**
     * Gets a <code>String</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a String
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child contains no text or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static String getRequiredString (Element parent, String tagName)
	throws HandlerException {
	String ret;
	Element el = parent.element(tagName);
	if (el != null) {
	    ret = el.getTextTrim();
	    if ("".equals(ret)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    }
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
	return ret;
    }

    /**
     * Gets a <code>String</code>-valued field of digits
     * from an XML Element family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a String
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not a digit <code>String</code> or if the child is
     * not present underneath <code>parent</code> at all.
     */
    public static String getRequiredDigitString (Element parent, String tagName)
    throws HandlerException {
        String ret = getRequiredString(parent, tagName);
        try {
            new java.math.BigInteger(ret);
        } catch (NumberFormatException nfe) {
            throw new HandlerException("Field '" + tagName + "' is not a" +
            "valid digit string.");
        }
        return ret;
    }

    /**
     * Gets a <code>String</code>-valued field of digits with a specified length
     * from an XML Element family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a String
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not a digit <code>String</code> or if the length is
     * incorrect or if the child is not present underneath <code>parent</code>
     * at all.
     */
    public static String getRequiredDigitString (Element parent, String tagName,
        int length) throws HandlerException {
        String ret = getRequiredDigitString(parent, tagName);
        if (ret.length() != length) {
            throw new HandlerException("Field '" + tagName + "' is not of " +
            "length " + length + ".");
        }
        return ret;
    }

    /**
     * Gets a <code>long</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a String
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not a <code>long</code> or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static long getRequiredLong (Element parent, String tagName)
	throws HandlerException {
	long lng;
        String lngStr;
	Element el = parent.element(tagName);
	if (el != null) {
	    lngStr = el.getTextTrim();
	    if ("".equals(lngStr)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    } else {
                try {
                    lng = Long.parseLong(lngStr);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Field '" + tagName + "' must" +
                        "be a valid long.");
                }
            }
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
	return lng;
    }
    
    /**
     * Gets a long integer-valued field from an XML Element family that
     * looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;12345678&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the ostensible parent of the tag whose value is
     * desired.
     * @param tagName the name of the tag whose value is desired.
     * @return the value of the requested child of
     * <code>parent</code>, or <code>-1</code> if the child is present
     * but empty, or <code>-999</code> if the child is not present.
     * Callers should check for a return value greater than or equal
     * to zero.
     * @throws HandlerException if the integer value is malformed.
     */
    public static long getOptionalLong (Element parent, String tagName)
	throws HandlerException {
	Element el = parent.element(tagName);
	if (el != null) {
	    int ret;
	    String retStr = el.getTextTrim();
	    if ((retStr == null) || "".equals(retStr)) {
		return -1;
	    }
	    try {
		ret = Long.valueOf(retStr).intValue();
	    } catch (NumberFormatException nfe) {
		throw new HandlerException("Malformed long-int in optional " +
					   "field '" + tagName + "'.");
	    }
	    return ret;
	} else {
	    return -999;
	}
    }

    /**
     * Gets a <code>boolean</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a boolean
     * value.
     * @return the value of the requested child of, or false if it is missing
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not "true" or "false" or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static boolean getOptionalBoolean (Element parent, String tagName)
	throws HandlerException {
		boolean bool = false;
        String boolStr;
		Element el = parent.element(tagName);
		if (el != null) {
			boolStr = el.getTextTrim();
			if (boolStr != null) {
				bool = Boolean.parseBoolean(boolStr);
			}
		}
		return bool;
    }
    
    /**
     * Gets a <code>boolean</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a boolean
     * value.
     * @return the value of the requested child of, or the <code>defaultValue</code> if it is missing
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not "true" or "false"
     * underneath <code>parent</code> at all.
     */
    public static boolean getOptionalBoolean (Element parent, String tagName, boolean defaultValue)
	throws HandlerException {
		boolean bool = defaultValue;
        String boolStr;
		Element el = parent.element(tagName);
		if (el != null) {
			boolStr = el.getTextTrim();
			if (boolStr != null) {
				bool = Boolean.parseBoolean(boolStr);
			}
		}
		return bool;
    }
    

    /**
     * Gets a <code>boolean</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a boolean
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not "true" or "false" or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static boolean getRequiredBoolean (Element parent, String tagName)
	throws HandlerException {
	boolean bool;
        String boolStr;
	Element el = parent.element(tagName);
	if (el != null) {
	    boolStr = el.getTextTrim();
	    if ("".equals(boolStr)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    } else {
                if (!("true".equals(boolStr) || "false".equals(boolStr))) {
                    throw new HandlerException("Field '" + tagName + "' must" +
                        "be \"true\" or \"false\".");
                }
                bool = Boolean.valueOf(boolStr).booleanValue();
            }
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
	return bool;
    }
  
    /**
     * Gets a <code>boolean</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want: A day,month,year in the format "YYYY-MM-DD" or "MM-DD-YYYY"
     * with an optional time, which will be ignored.
     * @param tagName the name of the required tag containing a boolean
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not a valid date or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static DateParameter getRequiredDay (Element parent, String tagName)
	throws HandlerException {
        DateParameter result = null;
        String dateString;
	Element el = parent.element(tagName);
	if (el != null) {
	    dateString = el.getTextTrim();
	    if ("".equals(dateString)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    } else {
                result = new DateParameter(dateString);
                if (!result.isValid()) {
                    throw new HandlerException("Field '" + tagName + "' must" +
                        "be a valid date");
                }              
            }
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
	return result;
    }
    
    /**
     * Gets a <code>boolean</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want: A day,month,year in the format "YYYY-MM-DD HH:mm:ss" or "MM-DD-YYYY HH:mm:ss"
     * @param tagName the name of the required tag containing a boolean
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not a valid date or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static DateTimeParameter getRequiredTimestamp (Element parent, String tagName)
	throws HandlerException {
        DateTimeParameter result = null;
        String dateString;
	Element el = parent.element(tagName);
	if (el != null) {
	    dateString = el.getTextTrim();
	    if ("".equals(dateString)) {
		throw new HandlerException("Required field '" + tagName +
					   "' cannot be empty");
	    } else {
                result = new DateTimeParameter(dateString);
                if (!result.isValid()) {
                    throw new HandlerException("Field '" + tagName + "' must" +
                        "be a valid date");
                }              
            }
	} else {
	    throw new HandlerException("Must provide a value for required " +
				       "field '" + tagName + "'.");
	}
	return result;
    }
    
    
    /**
     * Gets a <code>float</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a float
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not parseable as a float or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static float getRequiredFloat(Element parent, String tagName) 
        throws HandlerException {
        float result =-1f;
        String floatString;
        Element el = parent.element(tagName);
        if (el != null) {
            floatString = el.getTextTrim();
            if ("".equals(floatString)) {
                throw new HandlerException("Required field '" + tagName +
                                           "' cannot be empty");                    
            } else {
                try {
                    result = Float.parseFloat(floatString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Unable to parse field '"+tagName+
                                               "' as a float {"+floatString+"}");
                }
            }

        }
        return result;       
    }
    
     /**
     * Gets a <code>double</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a float
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not parseable as a float or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static double getRequiredDouble(Element parent, String tagName) 
        throws HandlerException {
        double result =-1;
        String doubleString;
        Element el = parent.element(tagName);
        if (el != null) {
            doubleString = el.getTextTrim();
            if ("".equals(doubleString)) {
                throw new HandlerException("Required field '" + tagName +
                                           "' cannot be empty");                    
            } else {
                try {
                    result = Double.parseDouble(doubleString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Unable to parse field '"+tagName+
                                               "' as a double {"+doubleString+"}");
                }
            }

        }
        return result;       
    }

     /**
     * Gets a <code>double</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the <code>Element</code> that contains the tag
     * whose value we want.
     * @param tagName the name of the required tag containing a float
     * value.
     * @return the value of the requested child of
     * <code>parent</code>.
     * @throws HandlerException if the XML is malformed or if the
     * requested child is not parseable as a float or if the child is not present
     * underneath <code>parent</code> at all.
     */
    public static double getOptionalDouble(Element parent, String tagName)
        throws HandlerException {
        double result =-1;
        String doubleString;
        Element el = parent.element(tagName);
        if (el != null) {
            doubleString = el.getTextTrim();
            if ("".equals(doubleString)) {
                throw new HandlerException("Required field '" + tagName +
                                           "' cannot be empty");
            } else {
                try {
                    result = Double.parseDouble(doubleString);
                } catch (NumberFormatException nfe) {
                    throw new HandlerException("Unable to parse field '"+tagName+
                                               "' as a double {"+doubleString+"}");
                }
            }

        } else {
            result = -1;
        }
        return result;
    }

    /**
     * Gets a <code>String</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>
     * @param parent the ostensible parent of the tag whose value is
     * desired.
     * @param tagName the name of the tag whose value is desired.
     * @return the value of the requested child of
     * <code>parent</code>, or <code>""</code> if the child is present
     * but empty, or <code>null</code> if the child is not present.
     */
    public static String getOptionalString (Element parent, String tagName) {
	String ret = null;
	Element el = parent.element(tagName);
	if (el != null) {
	    ret = el.getTextTrim();
	    return (ret != null) ? ret : "";
	} else {
	    return null;
	}
    }
    
    /**
     * Gets a <code>Date</code>-valued field from an XML Element
     * family that looks like this:
     * <code>&lt;parent&gt;&lt;tagName&gt;blah&lt;/tagName&gt;&lt;/parent&gt;</code>.
     * The date must be in the format "yyyy-MM-dd'T'HH:mm:ssZ"
     * See SimpleDateFormat for more information.
     * @param parent the ostensible parent of the tag whose value is
     * desired.
     * @param tagName the name of the tag whose value is desired.
     * @return the value of the requested child of
     * <code>parent</code>, or null if the child is not present, or not parseable.
     */
    public static Date getOptionalDate (Element parent, String tagName) {
        Date result = null;
        Element el = parent.element(tagName);
        if (el != null) {
            try {
                result = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")).parse(el.getTextTrim());
            } catch (Exception e) {
                //do nothing, just return null
            }
        }
        return result;
    }

    /**
     * @param month the month.
     * @param year the year (the real year, none of that two-digit crap)
     * @return a <code>Calendar</code> object set to the last day of
     * the given month and year.
     */
    public static Calendar getLastDayOfMonth (int month, int year) {
	Calendar cal = Calendar.getInstance();
	cal.set(Calendar.YEAR, year);
	cal.set(Calendar.MONTH, month - 1);
	cal.set(Calendar.DAY_OF_MONTH, 1);

	/* to get the last day of the current month, get the first day
	 * of the next month and subtract a day. */
	cal.add(Calendar.MONTH, 1);
	cal.add(Calendar.DATE, -1);
	return cal;
    }

    /**
     * @param c a <code>Calendar</code> to be stringified.
     * @return a date in "MM/DD/YYYY" format.
     */
    public static String getDateString (Calendar c) {
	return getDateString(c, false);
    }

    /**
     * @param c a <code>Calendar</code> to be stringified.
     * @param twoDigitYear whether to return the year in a two-digit
     * format rather than a four-digit format.
     * @return a date in "MM/DD/YYYY" format.
     */
    public static String getDateString (Calendar c, boolean twoDigitYear) {
	DecimalFormat twoDigits = new DecimalFormat("00");
	int day = c.get(Calendar.DAY_OF_MONTH);
	logger.warn("MONTH: " + c.get(Calendar.MONTH));
	int month = c.get(Calendar.MONTH) + 1; // January == 0
	int year = c.get(Calendar.YEAR);
	String yearStr = String.valueOf(year);

	if (twoDigitYear) {
	    if (year >= 2000) {
		year -= 2000;
	    } else if (year >= 1970) {
		year -= 1900;
	    } else {
		year %= 100;
	    }
	    yearStr = twoDigits.format(year);
	}
	
	return twoDigits.format(month) + "/" +
	    twoDigits.format(day) + "/" +
	    yearStr;
    }

    /**
     * @param month a month (1 to 12)
     * @param year a year
     * @return whether the given month/year are in the future (i.e.,
     * are either this month or a future month).
     * @throws HandlerException if the month is invalid.
     */
    public static boolean isInFuture (int month, int year)
	throws HandlerException {
	if (month < 1 || month > 12) {
	    throw new HandlerException("Invalid month (" + month +
				       ") in expiration date.");
	}

	Calendar now = Calendar.getInstance();
	int curYear = now.get(Calendar.YEAR);
	int curMonth = now.get(Calendar.MONTH) + 1; // because january == 0

	return (year >= curYear || (year == curYear && month >= curMonth));
    }

    /**
     * @param month a month (1 to 12)
     * @param year a year
     * @return an MMYY representation of the given month/year.
     * @throws HandlerException if the month is invalid.
     */
    public static String toMMYY (int month, int year) throws HandlerException {
	if (month < 1 || month > 12) {
	    throw new HandlerException("Invalid month (" + month + ").");
	}

	String monthStr = (month < 10)
	    ? "0"+String.valueOf(month)
	    : String.valueOf(month);

	int centYear = year % 100;
	String yearStr = (centYear < 10)
	    ? "0"+String.valueOf(centYear)
	    : String.valueOf(centYear);

	return monthStr + yearStr;
    }
    
    /**
     * Converts a name into a 10-digit first initial, lastname string
     * @param user the name to format
     */
    public static String formatUser (String user) {
        boolean hasComma;
        if (null != user) {
            user = user.trim();
            hasComma = (-1 != user.indexOf(","));
            user = user.replaceFirst(",", "");
            String[] userParts = user.split(" ");
            String first = "", last = user;
            if (userParts.length >= 2) {
                if (hasComma) {
                    first = userParts[1];
                    last = userParts[0];
                } else {
                    first = userParts[0];
                    last = userParts[userParts.length - 1];
                }
            }
            if (first.length() > 0) {
                first = first.substring(0,1);
            }
            user = (first + last).trim();
            if (user.length() > 10) {
                user = user.substring(0,10);
            }
        }
        return user;
    }

}
