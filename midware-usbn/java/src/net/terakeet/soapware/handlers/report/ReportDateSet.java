/*
 * ReportDateSet.java
 *
 * Created on Jan 07, 2010, 10:06 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.Date;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.RegisteredConnection;
import java.text.SimpleDateFormat;

/** ReportDateSet
 *  An immutable object that contains information about a report's date-interval.  
 */
public class ReportDateSet {
    
    private final Date sd; //start date
    private final Date ed; //end date
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private MidwareLogger logger = new MidwareLogger(LineOffsetMap.class.getName());
    
    /** Creates a new instance of ReportDateSet */
    public ReportDateSet(Date startDate, Date endDate) {
        
        if (startDate == null || endDate == null ) {
            throw new NullPointerException("Tried to construct ReportPeriod with one or more null arguments");
        }
        
        sd = startDate;
        ed = endDate;
    }

    /** Creates a overrride instance of ReportDateSet */
    public static ReportDateSet staticLookup(RegisteredConnection conn, int periodShift, int location, String businessDate) {
        ReportDateSet dateSet = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;

        try {
             switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT DATE_SUB(?, INTERVAL lo.easternOffset HOUR), DATE_SUB((? + INTERVAL 1 DAY), INTERVAL lo.easternOffset HOUR) FROM location lo WHERE lo.id = ?;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setInt(3, location);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(1)), newDateFormat.parse(rs.getString(2)));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR) PreOpen, " +
                            " DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " lo.easternOffset eO" +
                            " FROM location lo LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE lo.id=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setInt(5, location);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(1)), newDateFormat.parse(rs.getString(2)));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                            " DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.openSun,8)" +
                            " WHEN 2 THEN Right(lH.openMon,8)" +
                            " WHEN 3 THEN Right(lH.openTue,8)" +
                            " WHEN 4 THEN Right(lH.openWed,8)" +
                            " WHEN 5 THEN Right(lH.openThu,8)" +
                            " WHEN 6 THEN Right(lH.openFri,8)" +
                            " WHEN 7 THEN Right(lH.openSat,8) END open," +
                            " CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " lo.easternOffset eO" +
                            " FROM location lo LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE lo.id=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    stmt.setInt(6, location);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(1)), newDateFormat.parse(rs.getString(2)));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, " +
                            " DATE_SUB(Concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR) PreOpen" +
                            " FROM (Select CASE DAYOFWEEK(?)" +
                            " WHEN 1 THEN Right(lH.closeSun,8)" +
                            " WHEN 2 THEN Right(lH.closeMon,8)" +
                            " WHEN 3 THEN Right(lH.closeTue,8)" +
                            " WHEN 4 THEN Right(lH.closeWed,8)" +
                            " WHEN 5 THEN Right(lH.closeThu,8)" +
                            " WHEN 6 THEN Right(lH.closeFri,8)" +
                            " WHEN 7 THEN Right(lH.closeSat,8) END close," +
                            " CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY))" +
                            " WHEN 1 THEN Right(lH.preOpenSun,8)" +
                            " WHEN 2 THEN Right(lH.preOpenMon,8)" +
                            " WHEN 3 THEN Right(lH.preOpenTue,8)" +
                            " WHEN 4 THEN Right(lH.preOpenWed,8)" +
                            " WHEN 5 THEN Right(lH.preOpenThu,8)" +
                            " WHEN 6 THEN Right(lH.preOpenFri,8)" +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen," +
                            " lo.easternOffset eO" +
                            " FROM location lo LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE lo.id=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    stmt.setInt(6, location);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(1)), newDateFormat.parse(rs.getString(2)));
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT SUBDATE(CONCAT(LEFT(?,11),IFNULL(x.open,'11:00:00')), INTERVAL x.eO HOUR) Open, " +
                            " SUBDATE(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'05:00:00')), INTERVAL x.eO HOUR) PreOpen " +
                            " FROM " +
                            " (SELECT " +
                            " CASE DAYOFWEEK(?) " +
                            " WHEN 1 THEN Right(lH.openSun,8) " +
                            " WHEN 2 THEN Right(lH.openMon,8) " +
                            " WHEN 3 THEN Right(lH.openTue,8) " +
                            " WHEN 4 THEN Right(lH.openWed,8) " +
                            " WHEN 5 THEN Right(lH.openThu,8) " +
                            " WHEN 6 THEN Right(lH.openFri,8) " +
                            " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                            " CASE DAYOFWEEK(ADDDATE(?,INTERVAL 1 DAY)) " +
                            " WHEN 1 THEN Right(lH.preOpenSun,8) " +
                            " WHEN 2 THEN Right(lH.preOpenMon,8) " +
                            " WHEN 3 THEN Right(lH.preOpenTue,8) " +
                            " WHEN 4 THEN Right(lH.preOpenWed,8) " +
                            " WHEN 5 THEN Right(lH.preOpenThu,8) " +
                            " WHEN 6 THEN Right(lH.preOpenFri,8) " +
                            " WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen, " +
                            " l.easternOffset eO " +
                            " FROM location l LEFT JOIN locationHours lH ON lH.location=l.id " +
                            " WHERE lo.id=? ) AS x";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setInt(5, location);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(1)), newDateFormat.parse(rs.getString(2)));
                    }
                    break;
                default:
                    break;
            }
        } catch (SQLException sqle) { }
        catch (Exception pe) { }
        
        return dateSet;
    }
    
    
    public Date getStartDate() {
        return sd;
    }
    
    public Date getEndDate() {
        return ed;
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
