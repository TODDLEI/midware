/**
 * MessageListener.java
 *
 * @author Ben Ransford
 * @version $Id: MessageListener.java,v 1.23 2015/09/20 04:07:27 sravindran Exp $
 */
package net.terakeet.soapware;

// package imports
import java.io.IOException;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.util.Timer;
import java.util.Vector;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import org.apache.log4j.PropertyConfigurator;
import net.terakeet.soapware.queue.ActiveQueueProcessor;
import net.terakeet.soapware.queue.WaitingQueueProcessor;
import net.terakeet.soapware.queue.QueueCleaner;
import net.terakeet.soapware.queue.QueueException;
import net.terakeet.soapware.queue.QueueManager;
import net.terakeet.util.MidwareLogger;

/**
 * The main runnable class.  Listens for incoming connections and
 * passes them to <code>MessageProcessor</code> objects.
 */
public class MessageListener {
    static MidwareLogger logger = new MidwareLogger(MessageListener.class.getName());
    static MessageListenerSettings appSettings;
    static boolean requireSSL;
    static boolean requireClientCerts;
    private ThreadGroup processors;
    private HandlerMap handlerMap;
    private QueueManager queueManager;

    /**
     * Generic constructor.  Reads settings, sets up various other
     * objects.
     */
    public MessageListener () {
	/* read this application's properties.  If the properties file can't be 
           found, the installation is seriously broken and shouldn't really be
	   run anyway. */
	appSettings = new MessageListenerSettings();
	if (appSettings.hasError()) {
	    System.err.println(appSettings.getError());
	    System.exit(1);
	}

	/* set up whether SSL is required */
	requireSSL = appSettings.getRequireSSL();
	requireClientCerts = (!"false"
			      .equals(getSetting("listener.requireClientCerts")));

	/* set up log4j to read its configuration from the file named
	   by log4jConfigFile.  */
	String lcp = appSettings.getLoggerConfigPath();
	if ((lcp == null) || "".equals(lcp)) {
	    System.err.println("Must set property logger.configPath in " +
			       appSettings.getPropertiesFilename());
	    System.exit(1);
	}
	PropertyConfigurator.configureAndWatch(appSettings.getLoggerConfigPath());

	/* set up a thread group for all message processors. */
	this.processors = new ThreadGroup("Request handlers");
	this.handlerMap = new HandlerMap(appSettings.getHandlerMapPath());

	/* set up a queue manager to "manage" various queues. */
	String primaryQueuePath = appSettings.getSharedQueuePath();
	String alternateQueuePath = appSettings.getLocalQueuePath();
	try {
	    this.queueManager = new QueueManager(primaryQueuePath,
						 alternateQueuePath, this);
	} catch (QueueException qe) {
	    System.err.println(qe.toString());
	    System.exit(1);
	}

	try {
	    HandlerUtils.initialize(this);
            
            DatabaseConnectionManager.addConnection("auper",
                    HandlerUtils.getSetting("auper.tdsDriver"),
                    HandlerUtils.getSetting("auper.tdsPrefix") + "://"
                    + HandlerUtils.getSetting("auper.server") + "/"
                    + HandlerUtils.getSetting("auper.database"),
                    HandlerUtils.getSetting("auper.user"),
                    HandlerUtils.getSetting("auper.password")
            );

            DatabaseConnectionManager.addConnection("report",
                    HandlerUtils.getSetting("report.tdsDriver"),
                    HandlerUtils.getSetting("report.tdsPrefix") + "://"
                    + HandlerUtils.getSetting("report.server") + "/"
                    + HandlerUtils.getSetting("report.database"),
                    HandlerUtils.getSetting("report.user"),
                    HandlerUtils.getSetting("report.password")
            );

            /*DatabaseConnectionManager.addHikariConnection("hikari",
                    HandlerUtils.getSetting("hikari.tdsDriver"),
                    HandlerUtils.getSetting("hikari.tdsPrefix") + "://"
                    + HandlerUtils.getSetting("hikari.server") + "/",
                    HandlerUtils.getSetting("hikari.database"),
                    HandlerUtils.getSetting("hikari.user"),
                    HandlerUtils.getSetting("hikari.password")
            );*/
	} catch (HandlerException he) {
            logger.handlerException(he.toString());
	    System.err.println(he.toString());
	    System.exit(1);
	}
    }

    /**
     * Simply passes a request for a certain setting to the
     * application settings object.
     */
    public String getSetting (String propName) {
	return appSettings.getSetting(propName);
    }

    /**
     * Runs this object with settings learned from the application
     * settings file.
     */
    public void run () {
	logger.debug("Listening on TCP port "+appSettings.getPort()+".");
	ServerSocket mainSock = null;
	Socket workSock = null;

        (new ErrorMailer()).start();
	
	Timer timer = new Timer();
	long interval = Long.parseLong(appSettings
				       .getSetting("listener.handlerMapInterval"))
	    * 1000l; // ms
	timer.schedule(new CleanerUpper(this, timer, interval),
		       0, // initial delay
		       interval);

	queueManager.addListener(new ActiveQueueProcessor(queueManager
							  .getCurrentQueue(), this))
	    .start();
	queueManager.addListener(new WaitingQueueProcessor(queueManager
							   .getCurrentQueue(), this))
	    .start();
	queueManager.addListener(new QueueCleaner(queueManager.getCurrentQueue()))
	    .start();
        
	try {
	    ServerSocketFactory ssf = getServerSocketFactory(requireSSL);
	    if (ssf == null) {
		System.exit(99);
	    }
            logger.connection("Creating main socket");
	    mainSock = ssf.createServerSocket(appSettings.getPort());
	    if (requireSSL && requireClientCerts) {
		((SSLServerSocket)mainSock).setNeedClientAuth(true);
	    }

	    String connString = "host";

	    while (true) {
		if (requireSSL) {
		    workSock = (SSLSocket)mainSock.accept();
		} else {
		    workSock = mainSock.accept();
		}
		connString = workSock.getInetAddress().toString() +
		    ":" + workSock.getPort();;
		logger.connection("Connected to peer " + connString);

		(new MessageProcessor(processors, connString, this))
		    .setHostname(appSettings.getHostname())
		    .setSocket(workSock)
		    .start();
	    }
	} catch (IOException ioe) {
	    logger.dbError(ioe.getMessage());
	} finally {
	    try {
		mainSock.close();
	    } catch (IOException ioe) {
		logger.dbError(ioe.getMessage());
	    }
	}
    }

    /**
     * Creates a ServerSocketFactory (optionally SSL-enabled) and
     * returns it.
     * @param ssl whether the sockets the returned factory creates are
     * SSL enabled.
     * @return a <code>ServerSocketFactory</code> that can create
     * server sockets.
     */
    public ServerSocketFactory getServerSocketFactory (boolean ssl) {
	if (ssl) {
	    try {
		char[] passphrase = getSetting("listener.keyStorePassword")
		    .toCharArray();
		SSLContext ctx = SSLContext.getInstance("TLS");
		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		KeyStore ks = KeyStore.getInstance("JKS");
		ks.load(new FileInputStream(getSetting("listener.keyStore")),
			passphrase);
		kmf.init(ks, passphrase);
		ctx.init(kmf.getKeyManagers(), null, null);
		return ctx.getServerSocketFactory();
	    } catch (Exception e) {
		logger.dbError(e.getMessage());
		logger.dbError("Unable to load SSL socket factory.");
		return null;
	    }
	} else {
	    return ServerSocketFactory.getDefault();
	}
    }

    /**
     * @return the number of running ("active")
     * <code>MessageProcessor</code> objects.
     */
    public int threadCount () {
	return processors.activeCount();
    }

    /**
     * @return the unique (within a cluster) identifier of this
     * server.
     */
    public String getServerId () {
	return appSettings.getServerId();
    }

    /**
     * @return the path to a map of tag names to handlers.
     */
    public String getHandlerMapPath () {
	return appSettings.getHandlerMapPath();
    }

    /**
     * Reloads the map of tag names to handlers.  This is called by
     * <code>CleanerUpper</code> whenever the handler map changes.
     */
    public void reloadHandlerMap () {
	handlerMap.reloadData();
    }

    /**
     * @return the current handler map for this listener.
     */
    public HandlerMap getHandlerMap () {
	return handlerMap;
    }

    /**
     * @return the current queue manager for this
     * <code>MessageListener</code> instance.
     */
    public QueueManager getQueueManager () {
	return this.queueManager;
    }

    /**
     * Generic Java <code>main()</code> method.
     * @param args command-line arguments
     */
    public static void main (String[] args) {
	(new MessageListener()).run();

	// exit code of 0 indicates no problemo.
	System.exit(0);
    }
}
