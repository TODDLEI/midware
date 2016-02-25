/*
 * ChildLevelMap.java
 *
 * Created on August 17, 2009, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** ChildLevelMap
 *
 */
public class ChildLevelMap {
    
    static final String DEFAULT_NAME = "Unknown Child Level";
    static Logger logger = Logger.getLogger(ChildLevelMap.class.getName());
    private HashMap<Integer,String> map;
    
    /** Creates a new instance of ChildLevelMap */
    public ChildLevelMap(RegisteredConnection conn, int parentLevel) {
        map = new HashMap<Integer,String>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT id, name FROM ";
        
        switch(parentLevel){
            case 1:
                select += " region ";
                break;
            case 2:
                select += " county ";
                break;
            case 3: 
                select += " location ";
                break;
            case 4: 
                select += " productCategory ";
                break;
            case 5:
                select += " bar ";
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
            logger.debug("Built ChildLevelMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in ChildLevelMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasChildLevel(Integer p) {
        return map.containsKey(p);
    }
    
    public String getChildLevel(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getChildLevel(int p) {
        return getChildLevel(new Integer(p));
    }
    
    public boolean hasChildLevel(int p) {
        return hasChildLevel(new Integer(p));
    }
    
    public static String staticLookup(int p, RegisteredConnection conn, int parentLevel) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Child Level";
       
        String select = "SELECT id, name FROM ";
        
        switch(parentLevel){
            case 1:
                select += " county ";
            case 2:
                select += " zipList ";
            case 3: 
                select += " location ";
            default:
                select += " bar ";
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
        logger.debug("ChildLevel.staticLookup("+p+") returning {"+name+"}");
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
