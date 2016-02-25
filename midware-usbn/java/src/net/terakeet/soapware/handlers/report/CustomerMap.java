/*
 * CustomerMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** CustomerMap
 *
 */
public class CustomerMap {
    
    static final String DEFAULT_NAME = "Unknown Customer";
    static Logger logger = Logger.getLogger(CustomerMap.class.getName());
    private HashMap<Integer,String> map;
    
    /** Creates a new instance of CustomerMap */
    public CustomerMap(RegisteredConnection conn) {
        map = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name FROM customer");
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
            }
            logger.debug("Built CustomerMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in CustomerMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasCustomer(Integer p) {
        return map.containsKey(p);
    }
    
    public String getCustomer(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getCustomer(int p) {
        return getCustomer(new Integer(p));
    }
    
    public boolean hasCustomer(int p) {
        return hasCustomer(new Integer(p));
    }
    
    public static String staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Customer";
        try {
            ps = conn.prepareStatement("SELECT name FROM Customer WHERE id=?");
            ps.setInt(1,p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
            rs.close();
        } catch (SQLException sqle) { }
        //logger.debug("CustomerMap.staticLookup("+p+") returning {"+name+"}");
        return name;
    }

    public static int staticLookupByLocation(int location, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int id = 0;
        try {
            ps = conn.prepareStatement("SELECT customer FROM location WHERE id=?");
            ps.setInt(1,location);
            rs = ps.executeQuery();
            if (rs.next()) {
                id = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException sqle) { }
        //logger.debug("CustomerMap.staticLookup("+b+") returning {"+id+"}");
        return id;
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
    
}
