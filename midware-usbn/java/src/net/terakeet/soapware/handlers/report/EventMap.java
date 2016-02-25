/*
 * EventMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** EventMap
 *
 */
public class EventMap {
    
    static final String DEFAULT_DATE = "2005-01-01";
    static final String DEFAULT_DESC = "Unknown Event";
    static Logger logger = Logger.getLogger(EventMap.class.getName());
    private HashMap<Integer,String> dateMap;
    private HashMap<Integer,String> descMap;
    
    /** Creates a new instance of EventMap */
    public EventMap(RegisteredConnection conn) {
        dateMap = new HashMap<Integer,String>();
        descMap = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id, date, eventDesc FROM eventHours");
            rs = stmt.executeQuery();
            while (rs.next()) {
                dateMap.put(new Integer(rs.getInt(1)),rs.getString(2));
                descMap.put(new Integer(rs.getInt(1)),rs.getString(3));
            }
        } catch (SQLException sqle) {
            logger.warn("Database exception in EventMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasEventDate(Integer g) {
        return dateMap.containsKey(g);
    }
    
    public String getEventDate(Integer g) {
        String result = dateMap.get(g);
        if (result == null) {
            result = DEFAULT_DESC;
        }
        return result;
    }

    public boolean hasEventDesc(Integer g) {
        return descMap.containsKey(g);
    }

    public String getEventDesc(Integer g) {
        String result = descMap.get(g);
        if (result == null) {
            result = DEFAULT_DESC;
        }
        return result;
    }
    
    public String getEventDate(int p) {
        return getEventDate(new Integer(p));
    }
    
    public boolean hasEventDate(int p) {
        return hasEventDate(new Integer(p));
    }

    public String getEventDesc(int p) {
        return getEventDesc(new Integer(p));
    }

    public boolean hasEventDesc(int p) {
        return hasEventDesc(new Integer(p));
    }
    
    public static String staticEventDateLookup(int g, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String date = "2005-01-01";
        try {
            ps = conn.prepareStatement("SELECT id, date FROM eventHours WHERE id=?");
            ps.setInt(1,g);
            rs = ps.executeQuery();
            if (rs.next()) {
                date = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("EventMap.staticLookup("+g+") returning {"+date+"}");
        return date;
    }

    public static String staticEventDescLookup(int g, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String description = "Unknown Event";
        try {
            ps = conn.prepareStatement("SELECT id, date FROM eventHours WHERE id=?");
            ps.setInt(1,g);
            rs = ps.executeQuery();
            if (rs.next()) {
                description = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("EventMap.staticLookup("+g+") returning {"+description+"}");
        return description;
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
