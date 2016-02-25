/*
 * ProductMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.soapware.RegisteredConnection;
/** ProductMap
 *
 */
public class ProductMap {
    
    static final String DEFAULT_NAME = "Unknown Product";
    static final int DEFAULT_CATEGORY = 0;
    static final int DEFAULT_SEGMENT = 0;
    static Logger logger = Logger.getLogger(ProductMap.class.getName());
    private HashMap<Integer,String> map;
    private HashMap<Integer,Integer> categoryMap;
    private HashMap<Integer,Integer> segmentMap;
    
    /** Creates a new instance of ProductMap */
    public ProductMap(RegisteredConnection conn) {
        map         = new HashMap<Integer,String>();
        categoryMap = new HashMap<Integer,Integer>();
        segmentMap  = new HashMap<Integer,Integer>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT id,name,category,segment FROM product");
            rs = stmt.executeQuery();
            while (rs.next()) {
                map.put(new Integer(rs.getInt(1)),rs.getString(2));
                categoryMap.put(new Integer(rs.getInt(1)),rs.getInt(3));
                segmentMap.put(new Integer(rs.getInt(1)),rs.getInt(4));
            }
            //logger.debug("Built ProductMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.warn("Database exception in ProductMap: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasProduct(Integer p) {
        return map.containsKey(p);
    }
    
    public String getProduct(Integer p) {
        String result = map.get(p);
        if (result == null) {
            result = DEFAULT_NAME;
        }
        return result;
    }
    
    public String getProduct(int p) {
        return getProduct(new Integer(p));
    }
    
    public boolean hasProduct(int p) {
        return hasProduct(new Integer(p));
    }

    public boolean hasCategory(Integer c) {
        return categoryMap.containsKey(c);
    }

    public int getCategory(Integer c) {
        int result = categoryMap.get(c);
        if (String.valueOf(result) == null) {
            result = DEFAULT_CATEGORY;
        }
        return result;
    }

    public int getCategory(int c) {
        return getCategory(new Integer(c));
    }

    public boolean hasCategory(int c) {
        return hasCategory(new Integer(c));
    }

    public boolean hasSegment(Integer s) {
        return segmentMap.containsKey(s);
    }

    public int getSegment(Integer s) {
        int result = segmentMap.get(s);
        if (String.valueOf(result) == null) {
            result = DEFAULT_SEGMENT;
        }
        return result;
    }

    public int getSegment(int s) {
        return getSegment(new Integer(s));
    }

    public boolean hasSegment(int s) {
        return hasSegment(new Integer(s));
    }
    
    public static String staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Product";
        try {
            ps = conn.prepareStatement("SELECT name FROM product WHERE id=?");
            ps.setInt(1,p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("ProductMap.staticLookup("+p+") returning {"+name+"}");
        return name;
    }

    public static int staticLookupCategory(int c, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int category = 0;
        try {
            ps = conn.prepareStatement("SELECT category FROM product WHERE id=?");
            ps.setInt(1,c);
            rs = ps.executeQuery();
            if (rs.next()) {
                category = rs.getInt(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("ProductMap.staticLookup("+c+") returning {"+category+"}");
        return category;
    }

    public static int staticLookupSegment(int s, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int segment = 0;
        try {
            ps = conn.prepareStatement("SELECT segment FROM product WHERE id=?");
            ps.setInt(1,s);
            rs = ps.executeQuery();
            if (rs.next()) {
                segment = rs.getInt(1);
            }
        } catch (SQLException sqle) { }
        logger.debug("ProductMap.staticLookup("+s+") returning {"+segment+"}");
        return segment;
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
