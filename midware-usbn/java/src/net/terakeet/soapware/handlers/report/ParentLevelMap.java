/*
 * ParentLevelMap.java
 *
 * Created on August 17, 2009, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** ParentLevelMap
 *
 */
public class ParentLevelMap {
    
    static final String DEFAULT_NAME = "Unknown Parent Level";
    static Logger logger = Logger.getLogger(ParentLevelMap.class.getName());
    private HashMap<Integer,String> map;
    
    /** Creates a new instance of ChildLevelMap */
    public ParentLevelMap(RegisteredConnection conn, int parentLevel) {
        map = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT id, name FROM ";
        
        switch(parentLevel){
            case 1:
                select += " user ";
                break;
            case 2:
                select += " region ";
                break;
            case 3: 
                select += " county ";
                break;
            case 4:
                select += " location ";
                break;
            default:
                select += " location ";
                break;
        }
        
        
        try {
            stmt = conn.prepareStatement(select);
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
            }
            logger.debug("Built ParentLevelMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in ParentLevelMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasParentLevel(Integer p) {
        return map.containsKey(p);
    }
    
    public String getParentLevel(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getParentLevel(int p) {
        return getParentLevel(new Integer(p));
    }
    
    public boolean hasParentLevel(int p) {
        return hasParentLevel(new Integer(p));
    }
    
    public static String staticLookup(int p, RegisteredConnection conn, int parentLevel) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Parent Level";
       
        String select = "SELECT id, name FROM ";
        
        switch(parentLevel){
            case 1:
                select += " user ";
            case 2:
                select += " region ";
            case 3: 
                select += " county ";
            default:
                select += " location ";
        }
        
        try {
            select = " WHERE id=? ";
            ps = conn.prepareStatement(select);
            ps.setInt(1,p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("ParentLevel.staticLookup("+p+") returning {"+name+"}");
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
