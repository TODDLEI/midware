/**
 * LoadTester.java
 *
 * @author Ben Ransford
 * @version $Id: LoadTester.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 */

package net.terakeet.test;

// package imports.
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.net.SocketFactory;
import javax.net.ssl.*;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class LoadTester {
    private SAXReader reader = null;
    private MethodWeightPair[] allMethods;
    private double totalWeight = 0;
    private int numClients = 0;
    private int requestsPerClient = 0;
    private String host = null;
    private int port = 0;
    private ThreadGroup threads;
    private LoadTesterThread[] threadArray;
    private SocketFactory socketFactory;
    private String keyStore = null;
    private String keyStorePassphrase = null;
    private Map requests = null;

    static void usage () {
	System.err.println("Usage: java LoadTester <host> <port> <ssl> " +
			   "testDescriptor.xml /path/to/sample/requests \\" +
			   "\n  [keystore keystore_passphrase]");
	System.err.println(" Host: middleware host");
	System.err.println(" Port: middleware port");
	System.err.println("  SSL: 1 or 0");
	System.exit(1);
    }

    public static void fatalError (String msg) {
	System.err.println(msg);
	System.exit(2);
    }

    /** Process a test descriptor XML file
     */
    private void loadTestDescriptor (String path) {
	Document doc = null;
	try {
	    doc = reader.read(new File(path));
	} catch (Exception de) {
	    fatalError("Unable to process test descriptor file: " + de.getMessage());
	}

	Element tpEl = doc.getRootElement();
	if (!"testParameters".equals(tpEl.getName())) {
	    fatalError("Error: test descriptor doesn't root at a" +
		       " testParameters element");
	}

	Element numClientsEl = tpEl.element("numClients");
	Element numRequestsEl = tpEl.element("requestsPerClient");
	try {
	    numClients = Integer.valueOf(numClientsEl.getText()).intValue();
	    requestsPerClient = Integer.valueOf(numRequestsEl.getText()).intValue();
	} catch (Exception e) {
	    fatalError("Error: couldn't process numClients or requestsPerClient" +
		       " field in test descriptor.");
	}

	Element weightsEl = tpEl.element("methodWeights");
	if (weightsEl == null) {
	    fatalError("Error: Weights must be specified for each method you" +
		       " want tested; at least one must exist in test descriptor.");
	}
	List methods = weightsEl.elements("method");
	allMethods = new MethodWeightPair[methods.size()];

	Element method = null;
	String methodName = null;
	int methodWeight = 0;
	int weightSum = 0;
	int midx = 0; // position in allMethods array currently being filled
	try {
	    for (Iterator i = methods.iterator(); i.hasNext(); ) {
		method = (Element)i.next();
		methodName = method.attributeValue("name");
		methodWeight = Integer.valueOf(method.getText()).intValue();
		weightSum += methodWeight;
		allMethods[midx++] = new MethodWeightPair(methodName,
							  (double)methodWeight);
	    }
	} catch (NumberFormatException nfe) {
	    fatalError("Error: Weights in methodWeights element must be integers.");
	}

	// normalize the weights; biggest gets 1.0
	double maxWeight = 0.0;
	double wgt;
	for (int i = 0; i < allMethods.length; i++) {
	    wgt = allMethods[i].getWeight();
	    if (wgt > maxWeight) {
		maxWeight = wgt;
	    }
	}
	for (int i = 0; i < allMethods.length; i++) {
	    allMethods[i].setWeight(allMethods[i].getWeight() / maxWeight);
            totalWeight += allMethods[i].getWeight();
	}
        java.util.Arrays.sort(allMethods);
    }

    private void loadSampleRequests (String path) {
	String filename, methodName;
	for (int i = 0; i < allMethods.length; i++) {
	    methodName = allMethods[i].getMethod();
	    filename = path + File.separator + methodName + ".xml";
	    File f = new File(filename);
	    if (f.canRead()) {
		try {
		    requests.put(methodName, reader.read(f));
		} catch (Exception e) {
		    e.printStackTrace();
		    fatalError(e.toString());
		}
	    } else {
		System.err.println("WARNING: cannot read " + filename);
	    }
	}
    }

    /** Initialize the load tester
     *  @param host the middleware host
     *  @param port the middleware port
     *  @param ssl true to use ssl encryption
     *  @param tdPath the path to the Test Descriptor File
     *  @param srPath the path to the sample requests
     *  @param keyStore for SSL, may be null if not needed
     *  @param ksPassphrase for SSL, may be null if not needed
     *
     */
    public LoadTester (String host, int port, boolean ssl,
		       String tdPath, String srPath,
		       String keyStore, String ksPassphrase) {
	reader = new SAXReader();
	this.host = host;
	this.port = port;
	this.keyStore = keyStore;
	this.keyStorePassphrase = ksPassphrase;

	this.threads = new ThreadGroup("Client threads");
	this.socketFactory = createSocketFactory(ssl);
	this.requests = new HashMap();

	loadTestDescriptor(tdPath);
	loadSampleRequests(srPath);
    }

    private SocketFactory createSocketFactory (boolean ssl) {
	if (ssl) {
	    try {
		char[] passphrase = keyStorePassphrase.toCharArray();
		SSLContext ctx = SSLContext.getInstance("TLS");
		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		KeyStore ks = KeyStore.getInstance("JKS");
		ks.load(new FileInputStream(keyStore), passphrase);
		kmf.init(ks, passphrase);
		ctx.init(kmf.getKeyManagers(), null, null);
		return ctx.getSocketFactory();
	    } catch (Exception e) {
		fatalError(e.getMessage());

		// this line is NEVER REACHED.
		return SocketFactory.getDefault();
	    }
	} else {
	    return SocketFactory.getDefault();
	}
    }
    
    /** Selects a random method the MethodWeightPair.
     */
    private String selectRandomMethod(MethodWeightPair[] pairs) {
	// int j = (int)Math.floor(Math.random() * (pairs.length - 1));
	// return pairs[j].getMethod();
        double targetWeight = Math.random() * totalWeight;
        double runningTotal = 0;
        for (int i = 0; i < allMethods.length; i++) {
            runningTotal += allMethods[i].getWeight();
            if (runningTotal >= targetWeight) {
                return allMethods[i].getMethod();
            }
        }
        
        // Should never get this far
        return allMethods[allMethods.length].getMethod();
    }
    
    public SocketFactory getSocketFactory () {
	return this.socketFactory;
    }

    public String getHost () {
	return this.host;
    }

    public int getPort () {
	return this.port;
    }

    public int getNumClients () {
	return numClients;
    }

    public void go () {
	// launch exactly numClients threads.  log.
	threadArray = new LoadTesterThread[numClients];
	for (int i = 0; i < numClients; i++) {
	    System.err.print("Creating thread #" + i);
	    threadArray[i] = (new LoadTesterThread(threads,
						   "client" + (i),
						   this,
						   requestsPerClient));
	    String methodName = selectRandomMethod(allMethods);            
	    System.err.println("thread #" + i + " gets " + methodName);
	    Document d = (Document)requests.get(methodName);
	    threadArray[i].setDocument(d, methodName);
	}
	// walk threadArray[] and start each thread.  log after all are started.
	for (int i = 0; i < threadArray.length; i++) {
	    threadArray[i].start();
	}
	System.err.println("Started " + threadArray.length + " threads.");
    }

    
    /**  Initialize the load tester.  
     *    Must provide the following arguments:
     *  host
     *  port
     *  descriptor file
     *  path to sample requests
     *    In addition, the following two optional args may be provided for SSL:
     *  keystore
     *  keystore path 
     */
    public static void main (String[] args) {
	if (args.length != 5 && args.length != 7) {
	    usage();
	}

	String h = args[0];
	int p = Integer.valueOf(args[1]).intValue();
	boolean ssl = ("1".equals(args[2]) || "true".equals(args[2]));
	String tdp = args[3];
	String srp = args[4];
	String ks  = (ssl && (args.length == 7)) ? args[5] : null;
	String ksp = (ssl && (args.length == 7)) ? args[6] : null;
	(new LoadTester(h,p,ssl,tdp,srp,ks,ksp)).go();
    }
}
