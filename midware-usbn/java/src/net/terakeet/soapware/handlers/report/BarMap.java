/*
 * BarMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.util.MidwareLogger;
/** BarMap
 *
 */
public class BarMap {
    
    static final String DEFAULT_NAME = "Unknown Bar";
    private HashMap<Integer,String> map;
    private HashMap<Integer,Integer> locationMap;
    private MidwareLogger logger = new MidwareLogger(BarMap.class.getName());;
    /** Creates a new instance of BarMap */
    public BarMap(RegisteredConnection conn) {
        map = new HashMap<Integer,String>();
        locationMap = new HashMap<Integer,Integer>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name,location FROM bar");
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
                locationMap.put(new Integer(rs.getInt(1)),rs.getInt(1));
            }
            logger.debug("Built BarMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in BarMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    public BarMap(RegisteredConnection conn, int parentLevel, int value, int type) {
        map = new HashMap<Integer,String>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT b.id, b.name FROM bar b ";

        switch(parentLevel){
            case 0:
                select += " LEFT JOIN location lo ON lo.id = b.location WHERE lo.type = 2 AND b.id > ? ";
                break;
            case 1:
                select += " LEFT JOIN location lo ON lo.id = b.location WHERE lo.customer = ? ";
                break;
            case 2:
                select += " WHERE b.location = ? ";
                break;
            case 3:
                select += " WHERE b.zone = ? ";
                break;
            case 4:
                select += " WHERE b.id = ? ";
                break;
            case 5:
                select += " LEFT JOIN station st ON st.bar = b.id WHERE st.id = ? ";
                break;
            default:
                select += " WHERE b.id = ? ";
                break;
        }

        if (type > 0) {
            select += " AND b.type = ? ";
        }

        try {
            stmt = conn.prepareStatement(select);
            stmt.setInt(1, value);
            if (type > 0) {
                stmt.setInt(2, type);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
            }
            logger.debug("Built BarMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in BarMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasBar(Integer p) {
        return map.containsKey(p);
    }
    
    public String getBar(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public boolean hasLocation(Integer l) {
        return locationMap.containsKey(l);
    }

    public int getLocation(Integer l) {
        int result = locationMap.get(l);
        if (String.valueOf(result) == null) {
            result = 0;
        }
        return result;
    }
    
    public String getBar(int p) {
        return getBar(new Integer(p));
    }
    
    public boolean hasBar(int p) {
        return hasBar(new Integer(p));
    }

    public int getLocation(int l) {
        return getLocation(new Integer(l));
    }

    public boolean hasLocation(int l) {
        return hasLocation(new Integer(l));
    }

    public int getSize() {
        return map.size();
    }
    
    public static String staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Bar";
        try {
            ps = conn.prepareStatement("SELECT name FROM bar WHERE id=?");
            ps.setInt(1,p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
            rs.close();
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
