/**
 * MessageListenerSettings.java
 *
 * @author Ben Ransford
 * @version $Id: MessageListenerSettings.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */
package net.terakeet.soapware;

// package imports
import net.terakeet.util.ApplicationSettings;
import java.util.Properties;

class MessageListenerSettings extends ApplicationSettings {
    /* default values for various fields */
    private static final String DEFAULT_HOSTNAME = "default";
    private static final int DEFAULT_PORT = 2957; // a nice prime
    private static final int DEFAULT_INITIAL_THREADS = 5;
    private static final boolean DEFAULT_REQUIRE_SSL = true;

    /* regex to determine validity of a hostname.  NOTE: underscores
     * ('_') are not valid characters in hostnames.  (RFC 1123) */
    private static final String HOSTNAME_REGEX = "^(\\w[-\\w]*\\.?)+$";

    /* text of keys in properties file (the keyName in "keyName =
       value") */
    private String serverIdName = "server.id";
    private String hostnameName = "listener.hostname";
    private String portName = "listener.port";
    private String initialThreadsName = "listener.initialThreads";
    private String handlerMapPathName = "listener.handlerMapPath";
    private String requireSSLName = "listener.requireSSL";
    private String loggerConfigPathName = "logger.configPath";
    private String sharedQueuePathName = "queue.shared";
    private String localQueuePathName = "queue.local";

    /* variables to contain properties' values */
    private String serverId = null;
    private String hostname = DEFAULT_HOSTNAME;
    private int port = DEFAULT_PORT;
    private int initialThreads = DEFAULT_INITIAL_THREADS;
    private String handlerMapPath = null;
    private boolean requireSSL = DEFAULT_REQUIRE_SSL;
    private String loggerConfigPath = null;
    private String sharedQueuePath = null;
    private String localQueuePath = null;

    public MessageListenerSettings () {
	super("MessageListener.props",
	      "net.terakeet.soapware.MessageListener Properties");
	loadProperties();
    }

    protected void setDefaults (Properties defaultProps) {
    }

    protected void importSettings () {
	serverId = (String)properties.get(serverIdName);
	hostname = (String)properties.get(hostnameName);
	// note: String.matches(String) requires JDK >= 1.4
	if (hostname == null || !hostname.matches(HOSTNAME_REGEX)) {
	    hostname = DEFAULT_HOSTNAME;
	}
	try {
	    port = Integer.valueOf((String)properties.get(portName)).intValue();
	} catch (NumberFormatException nfe) {
	    port = DEFAULT_PORT;
	}
	try {
	    initialThreads = Integer.valueOf((String)
					     properties.get(initialThreadsName)).intValue();
	} catch (NumberFormatException nfe) {
	    initialThreads = DEFAULT_INITIAL_THREADS;
	}
	String requireSSLStr = (String)properties.get(requireSSLName);
	if ("false".equals(requireSSLStr)) { requireSSL = false; }
	loggerConfigPath = (String)properties.get(loggerConfigPathName);
	handlerMapPath = (String)properties.get(handlerMapPathName);
	sharedQueuePath = (String)properties.get(sharedQueuePathName);
	localQueuePath = (String)properties.get(localQueuePathName);
    }

    protected void exportSettings () {
	properties.put(serverIdName, serverId);
	properties.put(hostnameName, hostname);
	properties.put(portName, String.valueOf(port));
	properties.put(initialThreadsName, String.valueOf(initialThreads));
	properties.put(handlerMapPathName, handlerMapPath);
	properties.put(requireSSLName, String.valueOf(requireSSL));
	properties.put(loggerConfigPathName, loggerConfigPath);
	properties.put(sharedQueuePathName, sharedQueuePath);
	properties.put(localQueuePathName, localQueuePath);
    }

    public String getSetting (String propName) {
	return (String)properties.get(propName);
    }

    public void saveSettings () {
	saveProperties();
    }

    public String toString () {
	StringBuffer sb = new StringBuffer("[ ");
	sb.append(serverIdName + " = " + serverId + "\n");
	sb.append(hostnameName + " = " + hostname + "\n");
	sb.append(portName + " = " + port + "\n");
	sb.append(initialThreadsName + " = " + initialThreads + "\n");
	sb.append(handlerMapPathName + " = " + handlerMapPath + "\n");
	sb.append(requireSSLName + " = " + requireSSL + "\n");
	sb.append(sharedQueuePathName + " = " + sharedQueuePath + "\n");
	sb.append(localQueuePathName + " = " + localQueuePath + "\n");
	sb.append(loggerConfigPathName + " = " + loggerConfigPath);
	sb.append(" ]");
	return sb.toString();
    }

    public boolean getRequireSSL () { return requireSSL; }
    public void setRequireSSL (boolean b) { requireSSL = b; saveProperties(); }
    public String getLoggerConfigPath () { return loggerConfigPath; }
    public void setLoggerConfigPath (String p) { loggerConfigPath = p; saveProperties(); }
    public String getHandlerMapPath () { return handlerMapPath; }
    public void setHandlerMapPath (String p) { handlerMapPath = p; saveProperties(); }
    public String getSharedQueuePath () { return sharedQueuePath; }
    public void setSharedQueuePath (String p) { sharedQueuePath = p; saveProperties(); }
    public String getLocalQueuePath () { return localQueuePath; }
    public void setLocalQueuePath (String p) { localQueuePath = p; saveProperties(); }
    public String getServerId () { return serverId; }
    public void setServerId (String p) { serverId = p; saveProperties(); }
    public String getHostname () { return hostname; }
    public void setHostname (String p) {
	if (p != null && p.matches(HOSTNAME_REGEX)) {
	    hostname = p; saveProperties();
	}
    }
    public int getPort () { return port; }
    public void setPort (int p) { port = p; saveProperties(); }
    public int getInitialThreads () { return initialThreads; }
    public void setInitialThreads (int t) { initialThreads = t; saveProperties(); }
}
