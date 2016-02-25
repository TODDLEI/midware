/*
 * Database.java
 *
 * Created on September 7, 2005, 2:57 PM
 *
 */

package net.terakeet.soapware.handlers.auper;

import java.sql.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import org.apache.log4j.Logger;

/** Database
 *
 */
public class Database {
    
    static Logger logger = Logger.getLogger(Database.class.getName());
    static String mysqlConnectionURL = null;
    
    /** Creates a new instance of SQLAddHandler */
    public Database() {
         if (mysqlConnectionURL == null) {
            mysqlConnectionURL = "jdbc:mysql://69.67.249.67/usbn?user=root&password=Kern1ghaN";
            //mysqlConnectionURL = ""; // HandlerUtils.getSetting("auper.tdsPrefix") + "://" +
            //HandlerUtils.getSetting("auper.server") + "/" +
            //HandlerUtils.getSetting("auper.database") + "?" +
            //"user=" + HandlerUtils.getSetting("auper.user") + "&" +
            //"password=" + HandlerUtils.getSetting("auper.password");
        
        }
    }
    
     public static Connection connectTo (String dbURL)  {
	Connection conn = null;
	try {
	    logger.debug("Connecting to database: " + dbURL);
            Class.forName("com.mysql.jdbc.Driver");
	    DriverManager.setLoginTimeout(5);
	    conn = DriverManager.getConnection(dbURL);
	    logger.debug("Connection succeeded.");
	} catch (Exception e) {
            Util.debugPrint("Connection failed: "+e.toString());
	    logger.warn("Connection failed.");
	}
	return conn;
    }
    
     /** Add a number of line readings to the database table.
      * The readings come from the attached response of a Command00. A 
      * map is need to map line indices (0-15) to line ids.  
      *
      * @param c the line reading response, must have a reponse attached
      * @param lines an Integer,Integer map between line indices (0-15) and db line ids
      *
      */
    public void addReadings(Command00 c,Map<Integer,Integer> lines) {
        
        double IGNORE_THRESHOLD = 1000; 
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = connectTo(mysqlConnectionURL);
        Util.debugPrint("connected to mysql");      
        
        String select     = "SELECT value FROM reading WHERE line=? AND type = 0 ORDER BY date DESC";
        String insert     = "INSERT INTO reading (line,value) VALUES (?,?)";
        String updateLine = "UPDATE line SET " +
                            "ouncesPoured=ouncesPoured+? " +
                            "WHERE id=?" ;
        
        Iterator<Integer> i = lines.keySet().iterator();
        while (i.hasNext()) {
            Integer key = i.next();
            int lineIndex = key.intValue();
            int lineId = lines.get(key).intValue(); 
            double reading = c.getReading(lineIndex);
            
            try {
                
                //Check the previous reading and calculate the difference
                double difference = 0.0;
                stmt = conn.prepareStatement(select);
                stmt.setInt(1,lineId);
                rs = stmt.executeQuery();
                if (rs.next()) {                   
                    difference = reading - rs.getDouble(1);
                    if (Math.abs(difference) > IGNORE_THRESHOLD) {
                        difference = 0.0;
                    }  
                } else {
                    difference = reading;
                }                              
                
                //Insert a reading record
                stmt = conn.prepareStatement(insert);
                stmt.setInt(1,lineId);
                stmt.setDouble(2, c.getReading(lineIndex));
                Util.debugPrint("Adding value "+c.getReading(lineIndex)+" for ID#"+lineId);
                stmt.executeUpdate();                
                
                //Update the line record
                stmt = conn.prepareStatement(updateLine);
                stmt.setDouble(1, difference);
                stmt.setInt(2, lineId);
                stmt.executeUpdate();
                
                
            } catch (SQLException sqle) {
                Util.debugPrint("sql: "+sqle.toString());
            }

        }
        close(stmt);
        close(rs);
        close(conn);
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
    
    public static Map<Integer,Integer> kittyHoynesMap(int sysId) {
        Map<Integer,Integer> result = new HashMap<Integer,Integer>(16);
        if (sysId == 0) {
            for (int i=0; i<16; i++) {
                result.put(new Integer(i),new Integer(i+7));
            }
        } else if (sysId == 1) {
            for (int i=0; i<3; i++) {
                result.put(new Integer(i),new Integer(i+24));
            }
        } else {
            throw new IllegalArgumentException("unknown sysId: "+sysId);
        }
        return result;
        
    }
}
