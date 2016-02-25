/*
 * LocationMap.java
 *
 * Created on February 23rd, 2008, 10:00 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import java.util.Date;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.RegisteredConnection;
/** LocationMap
 *
 */
public class LocationMap {
    
    static final String DEFAULT_NAME = "Unknown Location";
    static final Double DEFAULT_OFFSET = 0.0;
    private MidwareLogger logger = new MidwareLogger(LocationMap.class.getName());
    private HashMap<Integer,ReportDateSet> map;
    private HashMap<Integer,Integer> customerMap;
    private HashMap<Integer,String> nameMap;
    private HashMap<Integer,Integer> idMap;
    private HashMap<Integer,Double> offsetMap;
    
    /** Creates a new instance of LocationMap */
    public LocationMap(RegisteredConnection conn) {
        nameMap = new HashMap<Integer,String>();
        customerMap = new HashMap<Integer,Integer>();
        idMap = new HashMap<Integer,Integer>();
        offsetMap = new HashMap<Integer,Double>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name,easternOffset,customer FROM location");
            rs = stmt.executeQuery();
            while (rs.next()) {
                nameMap.put(new Integer(rs.getInt(1)),rs.getString(2));
                offsetMap.put(new Integer(rs.getInt(1)),rs.getDouble(3));
                customerMap.put(new Integer(rs.getInt(1)),rs.getInt(4));
            }
            stmt = conn.prepareStatement("SELECT b.id, b.location FROM bar b");
            rs = stmt.executeQuery();
            while (rs.next()) {
                idMap.put(new Integer(rs.getInt(1)),rs.getInt(2));
            }
            logger.debug("Built LocationMap with idMap "+idMap.size()+" entries and nameMap "+nameMap.size()+" entries and offsetMap "+offsetMap.size()+" entries");
        } catch (SQLException sqle) {
            logger.debug("Database exception in LocationMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /** Creates a new instance of LocationMap */
    public LocationMap(RegisteredConnection conn, int periodShift, int customer, String businessDate) {
        nameMap = new HashMap<Integer,String>();
        idMap = new HashMap<Integer,Integer>();
        offsetMap = new HashMap<Integer,Double>();
        map = new HashMap<Integer,ReportDateSet>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name,easternOffset FROM location WHERE customer = ?");
            stmt.setInt(1, customer);
            rs = stmt.executeQuery();
            while (rs.next()) {
                nameMap.put(new Integer(rs.getInt(1)),rs.getString(2));
                offsetMap.put(new Integer(rs.getInt(1)),rs.getDouble(3));
                map.put(rs.getInt(1), ReportDateSet.staticLookup(conn, periodShift, rs.getInt(1), businessDate));
            }
            stmt = conn.prepareStatement("SELECT b.id,l.id FROM bar b LEFT JOIN location l ON b.location = l.id WHERE l.customer = ?");
            stmt.setInt(1, customer);
            rs = stmt.executeQuery();
            while (rs.next()) {
                idMap.put(new Integer(rs.getInt(1)),rs.getInt(2));
            }
            logger.debug("Built LocationMap with idMap "+idMap.size()+" entries and nameMap "+nameMap.size()+
                    " entries and offsetMap "+offsetMap.size()+" entries and dateMap "+nameMap.size()+" entries");
        } catch (SQLException sqle) {
            logger.debug("Database exception in LocationMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /** Creates a new instance of LocationMap */
    public LocationMap(RegisteredConnection conn, int periodShift, String specificLocation, String businessDate) {
        nameMap = new HashMap<Integer,String>();
        idMap = new HashMap<Integer,Integer>();
        offsetMap = new HashMap<Integer,Double>();
        map = new HashMap<Integer,ReportDateSet>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name,easternOffset FROM location WHERE id IN (" + specificLocation + ")");
            rs = stmt.executeQuery();
            while (rs.next()) {
                nameMap.put(new Integer(rs.getInt(1)),rs.getString(2));
                offsetMap.put(new Integer(rs.getInt(1)),rs.getDouble(3));
                map.put(rs.getInt(1), ReportDateSet.staticLookup(conn, periodShift, rs.getInt(1), businessDate));
            }
            stmt = conn.prepareStatement("SELECT b.id, b.location FROM bar b WHERE b.location IN (" + specificLocation + ")");
            rs = stmt.executeQuery();
            while (rs.next()) {
                idMap.put(new Integer(rs.getInt(1)),rs.getInt(2));
            }
            logger.debug("Built LocationMap with idMap "+idMap.size()+" entries and nameMap "+nameMap.size()+
                    " entries and offsetMap "+offsetMap.size()+" entries and dateMap "+nameMap.size()+" entries");
        } catch (SQLException sqle) {
            logger.debug("Database exception in LocationMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasCustomer(Integer l) {
        return customerMap.containsKey(l);
    }

    public int getCustomer(Integer l) {
        int result = customerMap.get(l);
        if (String.valueOf(result) == null) {
            result = 0;
        }
        return result;
    }

    public boolean hasLocation(Integer l) {
        return nameMap.containsKey(l);
    }
    
    public String getLocation(Integer l) {
        String result = nameMap.get(l);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }

    public Double getLocationOffset(Integer l) {
        Double result = offsetMap.get(l);
        if (result == null) {
            result = DEFAULT_OFFSET;
        }
        return result;
    }

    public Double getLocationOffset(int l) {
        return getLocationOffset(new Integer(l));
    }

    public int getCustomer(int l) {
        return getCustomer(new Integer(l));
    }

    public boolean hasCustomer(int l) {
        return hasCustomer(new Integer(l));
    }
    
    public String getLocation(int l) {
        return getLocation(new Integer(l));
    }
    
    public boolean hasLocation(int l) {
        return hasLocation(new Integer(l));
    }
    
    public boolean hasLocationByBar(Integer b) {
        return idMap.containsKey(b);
    }
    
    public int getLocationByBar(Integer b) {
        int result = idMap.get(b);
        if (String.valueOf(result) == null) {
            result = 0;
        }
        return result;
    }

    public int getLocationByBar(int b) {
        return getLocationByBar(new Integer(b));
    }

    public boolean hasLocationByBar(int b) {
        return hasLocationByBar(new Integer(b));
    }

    public boolean hasLocationDateSet(Integer b) {
        return map.containsKey(b);
    }

    public ReportDateSet getLocationDateSet(Integer b) {
        ReportDateSet result = map.get(b);
        if (result == null) {
            result = new ReportDateSet(new Date(), new Date());
        }
        return result;
    }
    
    public ReportDateSet getLocationDateSet(int b) {
        return getLocationDateSet(new Integer(b));
    }
    
    public boolean hasLocationDateSet(int b) {
        return hasLocationDateSet(new Integer(b));
    }
    
    public static String staticLookup(int l, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Location";
        try {
            ps = conn.prepareStatement("SELECT name FROM Location WHERE id=?");
            ps.setInt(1,l);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
            rs.close();
        } catch (SQLException sqle) { }
        //logger.debug("LocationMap.staticLookup("+l+") returning {"+name+"}");
        return name;
    }
    
    public static int staticLookupByBar(int b, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int id = 0;
        try {
            ps = conn.prepareStatement("SELECT location FROM bar WHERE id=?");
            ps.setInt(1,b);
            rs = ps.executeQuery();
            if (rs.next()) {
                id = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException sqle) { }
        //logger.debug("LocationMap.staticLookup("+b+") returning {"+id+"}");
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
