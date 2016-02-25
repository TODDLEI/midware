/*
 * RegionExclusionMap.java
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

/** RegionExclusionMap
 *
 */
public class RegionExclusionMap {

    private MidwareLogger logger = new MidwareLogger(RegionExclusionMap.class.getName());
    static final String DEFAULT_NAME = "Unknown Value";
    static final int DEFAULT_VALUE = 0;
    //static Logger logger = Logger.getLogger(RegionExclusionMap.class.getName());
    private HashMap<Integer, Integer> map;

    /** Creates a new instance of RegionExclusionMap */
    public RegionExclusionMap(RegisteredConnection conn, int userId, int parentLevel, int value) {
        map = new HashMap<Integer, Integer>();
        PreparedStatement stmt = null;
        ResultSet rs = null, rs1 = null;

        String selectExclusion = " SELECT e.tables, e.field, e.value FROM exclusion e LEFT JOIN regionExclusionMap rEM ON rEM.exclusion = e.id " +
                " LEFT JOIN region r ON r.id = rEM.region " +
                " LEFT JOIN userRegionMap uRM ON uRM.region = r.id " +
                " LEFT JOIN groupRegionMap gRM ON gRM.id = r.regionGroup ";

        switch (parentLevel) {
            case 1:
                selectExclusion += " WHERE uRM.user = ? AND uRM.region = ? ";
                break;
            case 2:
                selectExclusion += " LEFT JOIN regionCountyMap rCM ON rCM.region = gRM.regionMaster WHERE uRM.user = ? AND rCM.county = ? ";
                break;
            case 3:
                selectExclusion += " LEFT JOIN regionCountyMap rCM ON rCM.region = gRM.regionMaster LEFT JOIN location l ON l.countyIndex = rCM.county WHERE uRM.user = ? AND l.id = ? ";
                break;
            case 4:
                selectExclusion = " SELECT e.tables, e.field, e.value FROM exclusion e LEFT JOIN userExclusionMap uEM ON uEM.exclusion = e.id WHERE e.type = 1 AND uEM.user = ?";
                break;
            default:
                selectExclusion += " WHERE rEM.region IS NULL ";
                break;
        }
        try {
            stmt = conn.prepareStatement(selectExclusion);
            stmt.setInt(1, userId);
            if (parentLevel != 4) {
                stmt.setInt(2, value);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                stmt = conn.prepareStatement("SELECT id FROM " + rs.getString(1) + " WHERE " + rs.getString(2) + " IN ( " + rs.getString(3) + " ) ");
                rs1 = stmt.executeQuery();
                int i = 0;
                while (rs1.next()) {
                    map.put(new Integer(rs1.getInt(1)), i++);
                }
            }
            logger.debug("Built RegionExclusionMap with " + map.size() + " entries");
        } catch (SQLException sqle) {
            logger.dbError("Database exception in RegionExclusionMap: " + sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }

    public boolean hasValue(Integer p) {
        return map.containsKey(p);
    }

    public int getValue(Integer p) {
        int result = map.get(p);
        if (String.valueOf(result) == null) {
            result = DEFAULT_VALUE;
        }
        return result;
    }

    public int getValue(int p) {
        return getValue(new Integer(p));
    }

    public boolean hasValue(int p) {
        return hasValue(new Integer(p));
    }

    public static String staticLookup(int p, RegisteredConnection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        String name = "Unknown Value";
        try {
            ps = conn.prepareStatement("SELECT name FROM product WHERE id=?");
            ps.setInt(1, p);
            rs = ps.executeQuery();
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) {
        }
        //logger.debug("RegionExclusionMap   .staticLookup(" + p + ") returning {" + name + "}");
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
