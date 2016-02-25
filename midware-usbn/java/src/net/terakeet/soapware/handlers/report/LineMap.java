/*
 * LineMap.java
 *
 * Created on April 18, 2011, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.util.MidwareLogger;
/** LineMap
 *
 */
public class LineMap {
    
    static final String DEFAULT_NAME        = "Unknown Line";
    private HashMap<Integer,Integer> map;
    private MidwareLogger logger            = new MidwareLogger(LineMap.class.getName());;
    /** Creates a new instance of LineMap */

    public LineMap(RegisteredConnection conn, int parentLevel, int value, int type) {
        map = new HashMap<Integer,Integer>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        String select = "SELECT l.id, b.type FROM line l LEFT JOIN bar b ON b.id = l.bar ";

        switch(parentLevel){
            case 0:
                select += " WHERE l.id = ? ";
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
                map.put(new Integer(rs.getInt(1)),rs.getInt(2));
            }
            logger.debug("Built LineMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in LineMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasLine(Integer p) {
        return map.containsKey(p);
    }
    
    public int getLine(Integer p) {
        return map.get(p);
    }
    
    public int getLine(int p) {
        return getLine(new Integer(p));
    }
    
    public boolean hasLine(int p) {
        return hasLine(new Integer(p));
    }

    public int getSize() {
        return map.size();
    }
    
    public static int staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int name = 2;
        try {
            ps = conn.prepareStatement("SELECT l.id, b.type FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE l.id=?");
            ps.setInt(1,p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getInt(1);
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
