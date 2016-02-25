/*
 * StationMap.java
 *
 * Created on August 17, 2009, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.util.MidwareLogger;
/** StationMap
 *
 */
public class StationMap {
    
    static final String DEFAULT_NAME = "Unknown Station";
    static final int DEFAULT_VALUE = 0;
    private MidwareLogger logger = new MidwareLogger(StationMap.class.getName());;
    private HashMap<Integer,String> map;
    private HashMap<Integer,Integer> barMap;
    
    /** Creates a new instance of StationMap */
    public StationMap(RegisteredConnection conn, int parentLevel, int value) {
        map = new HashMap<Integer,String>();
        barMap = new HashMap<Integer,Integer>();
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT st.id, st.name, st.bar FROM station st ";
        
        switch(parentLevel){
            case 0:
                select += " WHERE st.id > ? ";
                break;
            case 1:
                select += " LEFT JOIN bar b ON b.id = st.bar LEFT JOIN location lo ON lo.id = b.location " +
                        " WHERE lo.customer = ? ";
                break;
            case 2:
                select += " LEFT JOIN bar b ON b.id = st.bar WHERE b.location = ? ";
                break;
            case 3: 
                select += " LEFT JOIN bar b ON b.id = st.bar WHERE b.zone = ? ";
                break;
            case 4:
                select += " WHERE st.bar = ? ";
                break;
            case 5:
                select += " WHERE st.id = ? ";
                break;
            default:
                select += " WHERE st.id = ? ";
                break;
        }
        
        
        try {
            stmt = conn.prepareStatement(select);
            stmt.setInt(1, value);
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
                barMap.put(new Integer(rs.getInt(1)),new Integer(rs.getInt(3)));
            }
            logger.debug("Built StationMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in StationMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasStation(Integer p) {
        return map.containsKey(p);
    }
    
    public String getStation(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getStation(int p) {
        return getStation(new Integer(p));
    }
    
    public boolean hasStation(int p) {
        return hasStation(new Integer(p));
    }

    public boolean hasBar(Integer p) {
        return barMap.containsKey(p);
    }

    public int getBar(Integer p) {
        int result = barMap.get(p);
        if (String.valueOf(result) == null) {
            result = DEFAULT_VALUE;
        }
        return result;
    }

    public int getBar(int p) {
        return getBar(new Integer(p));
    }

    public boolean hasBar(int p) {
        return hasBar(new Integer(p));
    }
    
    public static String staticLookup(int p, RegisteredConnection conn, int parentLevel) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Station";
       
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
