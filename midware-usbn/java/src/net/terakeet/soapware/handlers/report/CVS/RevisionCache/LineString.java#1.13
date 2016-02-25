/*
 * LineString.java
 *
 * Created on Aptril 08, 2014, 11:32 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.RegisteredConnection;

/** LineString
 *
 */

public class LineString {
    
    StringBuilder DEFAULT_LINE_STRING       = new StringBuilder("0,");
    private static MidwareLogger logger     = new MidwareLogger(LineString.class.getName());
    
    /** Creates a new instance of ChildLevelMap */
    public LineString(RegisteredConnection conn, int parentLevel, int paramId, int bType, int product, ReportPeriod period, String specificLocationsString) {
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String select                       = "SELECT l.id FROM line l LEFT JOIN bar b ON b.id = l.bar ";
        
        switch(parentLevel){
            case 1:
                select                      += " LEFT JOIN location lo ON lo.id = b.location WHERE lo.customer = ? ";
                break;
            case 2:
                select                      += " WHERE b.location = ? ";
                break;
            case 3: 
                select                      += " WHERE b.zone = ? ";
                break;
            case 4:
                select                      += " WHERE b.id = ? ";
                break;
            case 5:
                select                      += " WHERE l.station = ? ";
                break;
            case 6:
                select                      += " WHERE b.location IN ( " + specificLocationsString + " ) ";
                paramId                     = 0;
                break;
            case 7:
                select                      += " WHERE l.id = ? ";
                break;
            default:
                select                      += " WHERE l.id = 0 ";
                paramId                     = 0;
                break;
        }

        if (bType > 0) {
            select                          += " AND b.type = ? ";
        }
        if (product > 0) {
            select                          += " AND l.product = ? ";
        }
        select                              += " AND l.lastPoured > ? ";
        //logger.debug(select);
        try {
            int fieldCount                  = 0;
            stmt                            = conn.prepareStatement(select);
            if (paramId > 0) {
                stmt.setInt(++fieldCount, paramId);
            }
            if (bType > 0) {
                stmt.setInt(++fieldCount, bType);
            }
            if (product > 0) {
                stmt.setInt(++fieldCount, product);
            }
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(period.getStartDate().getTime()));
            //logger.debug(paramId + ":" + bType + ":" + product + ":" + period.getStartDate().toString());
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                DEFAULT_LINE_STRING.append(rs.getString(1) + ",");
            }
            DEFAULT_LINE_STRING.setLength(DEFAULT_LINE_STRING.length() - 1);
        } catch (SQLException sqle) {
            logger.dbError("Database exception in LineString: "+sqle.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public StringBuilder getLineString() {
        return DEFAULT_LINE_STRING;
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
