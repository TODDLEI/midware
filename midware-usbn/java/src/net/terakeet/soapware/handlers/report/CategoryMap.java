/*
 * CategoryMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** CategoryMap
 *
 */
public class CategoryMap {
    
    static final String DEFAULT_NAME = "Unknown Category";
    static Logger logger = Logger.getLogger(CategoryMap.class.getName());
    private HashMap<Integer,String> map;
    
    /** Creates a new instance of CategoryMap */
    public CategoryMap(RegisteredConnection conn) {
        map = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name FROM category");
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
            }
            logger.debug("Built CategoryMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in CategoryMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasCategory(Integer c) {
        return map.containsKey(c);
    }
    
    public String getCategory(Integer c) {
        String result = map.get(c);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getCategory(int c) {
        return getCategory(new Integer(c));
    }
    
    public boolean hasCategory(int c) {
        return hasCategory(new Integer(c));
    }
    
    public static String staticLookup(int c, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Category";
        try {
            ps = conn.prepareStatement("SELECT name FROM category WHERE id=?");
            ps.setInt(1,c);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("CategoryMap.staticLookup("+c+") returning {"+name+"}");
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
