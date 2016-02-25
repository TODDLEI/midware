/*
 * LogTester.java
 *
 * Created on April 6, 2006, 2:48 PM
 *
 */

package net.terakeet.test;

import org.apache.log4j.*;

/** LogTester
 *
 */
public class LogTester {
    
    /** Creates a new instance of LogTester */
    public LogTester() {
    }
    
    public static void main (String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        
        Logger accessLog = Logger.getLogger("net.terakeet.access");
        Logger accessWarn = Logger.getLogger("net.terakeet.access.warning");
        accessLog.info("remoteReading by #11-301");
        accessLog.info("remoteReading by #11-301");
        accessWarn.warn("FAILED remoteReading by #12-301: Location mismatch (11)");
    }
    
}
