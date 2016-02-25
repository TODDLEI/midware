/**
 * AuperHandler
 * @author Patrick Danial
 */
package net.terakeet.soapware.handlers;

// package imports.
import net.terakeet.soapware.Handler;
import net.terakeet.soapware.HandlerException;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.SOAPMessage;
import net.terakeet.soapware.handlers.auper.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.lang.String;
import java.util.Iterator;
import java.util.Set;


/**    THIS CLASS CAN BE REMOVED
 *          **************
 *           (Apr 20 2006)
 *
 *  @deprecated no longer used
 */
public class AuperHandler implements Handler {
    static MidwareLogger logger = new MidwareLogger (AuperHandler.class.getName());
    static String driverClass = HandlerUtils.getSetting("auper.tdsDriver");
    static String connectionURL = null;
    
    /**
     * Generic constructor. Loads an appropriate database driver.
     * @throws Handler Exception if there's a problem loading the
     * database driver.
     */
    public AuperHandler() throws HandlerException {
        HandlerUtils.initializeDriver(driverClass);
        if (connectionURL == null) {
            connectionURL = HandlerUtils.getSetting("auper.tdsPrefix") + "://" +
            HandlerUtils.getSetting("auper.server") + "/" +
            HandlerUtils.getSetting("auper.database") + "?" +
            "user=" + HandlerUtils.getSetting("auper.user") + "&" +
            "password=" + HandlerUtils.getSetting("auper.password");
        }
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException {
        logger.xml("request: " + toHandle.asXML());
        logger.midwareError("Called Deprecated Method 'processReading' in AuperHandler");
        String function = toHandle.getName();
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        if ("processReading".equals(function)) {
            processReading(toHandle, responseFor(function,toAppend));
        } else {
            logger.generalWarning("Unknown function '" + function + "'.");
        }
        logger.xml("response: " + toAppend.asXML());
    }
    
    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }
    
    private void close(Statement s) {
            if (s != null) {
                try { s.close(); } catch (SQLException sqle) { }
            }
    }
    private void close(ResultSet rs) {
            if (rs != null) {
                try { rs.close(); } catch (SQLException sqle) { }
            }
    }
    private void close(Connection c) {
            if (c != null) {
                try { c.close(); } catch (SQLException sqle) { }
            }
    }
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
	return e.addElement("m:"+s+"Response",responseNamespace);
    }
   
    private double kegToOz(double keg) {
        return keg * 1920;
    }
 
    // BEGIN MIDDLEWARE MAIN METHODS
    
    private void processReading(Element toHandle, Element toAppend)
    throws HandlerException {

        // Constant to detect keg changes.  
        // If the reading difference is > the threshold, the reading is ignored
        final double IGNORE_THRESHOLD = 0.75;
        
        //Get a fresh batch of readings.
        Set<AuperReading> readings = AuperManager.getReadings();
   
        //Connect to the db
        Connection conn = HandlerUtils.connectTo(connectionURL);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
           
            for (AuperReading r: readings) {

                String select = 
                        "SELECT qtyInStock FROM readings WHERE pid=? ORDER BY date DESC";

                String insert = 
                        "INSERT INTO readings (pid,qtyInStock)" +
                        "VALUES (?,?)";
                
                String update = 
                        "UPDATE products SET " +
                        "qtyInStock=qtyInStock-?, " +
                        "qtyPoured=qtyPoured+? " +
                        "WHERE pid=?" ;
                
                //Check the previous reading and calculate the difference
                AuperKegs difference = null;
                stmt = conn.prepareStatement(select);
                stmt.setInt(1,r.getPid());
                rs = stmt.executeQuery();
                if (rs.next()) {
                    AuperReading lastRead = new AuperReading(r.getPid(),new AuperKegs(rs.getDouble(1)));
                    difference = AuperReading.difference(r,lastRead);
                }
                if (difference == null || Math.abs(difference.asKegs()) > IGNORE_THRESHOLD) {
                    difference = new AuperKegs(0.0);
                }                
                
                //Insert a reading record
                stmt = conn.prepareStatement(insert);
                stmt.setInt(1,r.getPid());
                stmt.setDouble(2,r.getKegs().asKegs());
                stmt.executeUpdate();
                
                //Update the products records
                stmt = conn.prepareStatement(update);
                stmt.setDouble(1, difference.asKegs());
                stmt.setDouble(2, difference.asOz());
                stmt.setInt(3, r.getPid());
                stmt.executeUpdate();
            
            }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
	    close(stmt);
	    close(conn);
            close(rs);
        }
        
    }
    
    
}


