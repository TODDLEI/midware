/*
 * SegmentMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** SegmentMap
 *
 */
public class SegmentMap {
    
    static final String DEFAULT_NAME = "Unknown Segment";
    static Logger logger = Logger.getLogger(SegmentMap.class.getName());
    private HashMap<Integer,String> map;
    
    /** Creates a new instance of SegmentMap */
    public SegmentMap(RegisteredConnection conn) {
        map = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name FROM segment");
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
            }
            logger.debug("Built SegmentMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in SegmentMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasSegment(Integer s) {
        return map.containsKey(s);
    }
    
    public String getSegment(Integer s) {
        String result = map.get(s);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getSegment(int s) {
        return getSegment(new Integer(s));
    }
    
    public boolean hasSegment(int s) {
        return hasSegment(new Integer(s));
    }
    
    public static String staticLookup(int s, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Segment";
        try {
            ps = conn.prepareStatement("SELECT name FROM segment WHERE id=?");
            ps.setInt(1,s);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("SegmentMap.staticLookup("+s+") returning {"+name+"}");
        return name;
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
