/*
 * RegionProductMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */
package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.RegisteredConnection;

/** RegionProductMap
 *
 */
public class RegionProductMap {

    private MidwareLogger logger = new MidwareLogger(RegionProductMap.class.getName());
    static final String DEFAULT_NAME = "Unknown Product";
    //static Logger logger = Logger.getLogger(RegionProductMap.class.getName());
    private HashMap<Integer, String> map;
    String subQuery = "0";

    /** Creates a new instance of RegionProductMap */
    public RegionProductMap(RegisteredConnection conn, int userId, int parentLevel, int value,String state) {
        map = new HashMap<Integer, String>();
        PreparedStatement stmt = null;
        ResultSet rs = null, rs1 = null;

        String select = " SELECT GROUP_CONCAT(DISTINCT pSM.product ORDER BY pSM.product SEPARATOR ', ') FROM region r " +
                " LEFT JOIN regionProductSet rPS ON rPS.region = r.id" +
                " LEFT JOIN productSetMap pSM ON pSM.productSet = rPS.productSet" +
                " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                " LEFT JOIN groupRegionMap gRM ON gRM.id = r.regionGroup ";
        String selectState  = "SELECT GROUP_CONCAT(DISTINCT pSM.product ORDER BY pSM.product SEPARATOR ', ') FROM region r LEFT JOIN location l ON l.addrCity=r.name"
                            + " LEFT JOIN regionProductSet rPS ON rPS.region = r.id LEFT JOIN productSetMap pSM ON pSM.productSet = rPS.productSet ";


        switch (parentLevel) {
            case 2:
                select += " WHERE uRM.user = ? AND r.id = ? LIMIT 1 ";
                break;
            case 3:
                select += " LEFT JOIN regionCountyMap rCM ON rCM.region = gRM.regionMaster WHERE uRM.user = ? AND rCM.county = ? ";
                break;
            case 4:
                select += " LEFT JOIN regionCountyMap rCM ON rCM.region = gRM.regionMaster " +
                        " LEFT JOIN location l ON l.countyIndex = rCM.county WHERE uRM.user = ? AND l.id = ? ";
                break;
            default:
                select += " WHERE uRM.user = ? AND r.id = ? LIMIT 1 ";
                break;
        }
        try {
            
            String subQuery = "-1";
            if(state!=null && !state.equals("")) {
             
                 selectState    +="WHERE l.id IN (" +state+")";
             
            stmt = conn.prepareStatement(selectState);            
            rs = stmt.executeQuery();
            while (rs.next()) {
                subQuery    += ", " + rs.getString(1);
            }                
            } else {
            stmt = conn.prepareStatement(select);
            stmt.setInt(1, userId);
            stmt.setInt(2, value);
            rs = stmt.executeQuery();
            while (rs.next()) {
                subQuery    += ", " + rs.getString(1);
            }
            }
            stmt = conn.prepareStatement("SELECT id, name FROM product WHERE id IN (" + subQuery + ")");
            rs1 = stmt.executeQuery();
            while (rs1.next()) {
                map.put(new Integer(rs1.getInt(1)), rs1.getString(2));
            }
            //logger.debug("Built RegionProductMap with " + map.size() + " entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in RegionProductMap: " + sqle.toString());
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

    public static String staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Product";
        try {
            ps = conn.prepareStatement("SELECT name FROM product WHERE id=?");
            ps.setInt(1, p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) {
        }
        //logger.debug("RegionProductMap.staticLookup(" + p + ") returning {" + name + "}");
        return name;
    }

    private void close(Statement s) {
        if (s != null) {
            try {
                s.close();
            } catch (SQLException sqle) {
            }
        }
    }

    private void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException sqle) {
            }
        }
    }
}
