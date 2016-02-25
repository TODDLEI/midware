/*
 * LineOffsetMap.java
 *
 * Created on October 14, 2005, 2:32 PM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;
import net.terakeet.util.MidwareLogger;
import net.terakeet.soapware.RegisteredConnection;
import java.text.SimpleDateFormat;
/** LineOffsetMap
 *
 */
public class LineOffsetMap {
    
    static final Double DEFAULT_VALUE = 0.00;
    private MidwareLogger logger = new MidwareLogger(LineOffsetMap.class.getName());
    private HashMap<Integer,ReportDateSet> map;
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /** Creates a new instance of LineOffsetMap */
    public LineOffsetMap(RegisteredConnection conn, int periodShift, int customer, String specificLocation, String businessDate) {
        map = new HashMap<Integer,ReportDateSet>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;
        logger.debug(businessDate);
        String selectionString = (customer > 0 ? " WHERE lo.customer=? " : " WHERE lo.id IN (" + specificLocation + ") ");
        
        try {
             switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT l.id, DATE_SUB(?, INTERVAL lo.easternOffset HOUR), DATE_SUB((? + INTERVAL 1 DAY), INTERVAL lo.easternOffset HOUR) " +
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location " + selectionString;
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    if (customer > 0) {
                        stmt.setInt(3, customer);
                    }
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ReportDateSet dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                        map.put(new Integer(rs.getInt(1)),dateSet);
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT x.Line, DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR), " +
                            " DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    if (customer > 0) {
                        stmt.setInt(5, customer);
                    }
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ReportDateSet dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                        map.put(new Integer(rs.getInt(1)),dateSet);
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT x.Line, DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                            " DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    if (customer > 0) {
                        stmt.setInt(6, customer);
                    }
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ReportDateSet dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                        map.put(new Integer(rs.getInt(1)),dateSet);
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT x.Line, DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, " +
                            " DATE_SUB(Concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" + selectionString +
                            " ) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    if (customer > 0) {
                        stmt.setInt(6, customer);
                    }
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ReportDateSet dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                        map.put(new Integer(rs.getInt(1)),dateSet);
                    }
                    break;
                case 4:
                    sqlQuery = "SELECT x.Line, SUBDATE(CONCAT(LEFT(?,11),IFNULL(x.open,'11:00:00')), INTERVAL x.eO HOUR) Open, " +
                            " SUBDATE(CONCAT(LEFT(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.preOpen,'05:00:00')), INTERVAL x.eO HOUR) PreOpen " +
                            " FROM " +
                            " (SELECT l.id Line, " +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id " + selectionString +
                            " ) AS x";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    if (customer > 0) {
                        stmt.setInt(5, customer);
                    }
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        ReportDateSet dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                        map.put(new Integer(rs.getInt(1)),dateSet);
                    }
                    break;
                default:
                    break;
            }
            logger.debug("Built LineOffsetMap with "+map.size()+" entries");
        } catch (SQLException sqle) {
            logger.debug("Database exception in LineOffsetMap: "+sqle.toString());
        } catch (Exception pe) {
            logger.debug("Exception in LineOffsetMap: "+pe.toString());
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
    public boolean hasLineOffset(Integer p) {
        return map.containsKey(p);
    }
    
    public ReportDateSet getLineOffset(Integer p) {
        ReportDateSet result = map.get(p);
        if (result == null) {
            result = new ReportDateSet(new Date(), new Date());
        }
        return result;
    }
    
    public ReportDateSet getLineOffset(int p) {
        return getLineOffset(new Integer(p));
    }
    
    public boolean hasLineOffset(int p) {
        return hasLineOffset(new Integer(p));
    }
    
    public static ReportDateSet staticLookup(int periodShift, int p, String businessDate, RegisteredConnection conn) {

        ReportDateSet dateSet = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sqlQuery = null;

        try {
             switch (periodShift) {
                case 0:
                    sqlQuery = "SELECT l.id, DATE_SUB(?, INTERVAL lo.easternOffset HOUR), DATE_SUB((? + INTERVAL 1 DAY), INTERVAL lo.easternOffset HOUR) FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location WHERE lo.customer = ?;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setInt(3, p);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                    }
                    break;
                case 1:
                    sqlQuery = "SELECT x.Line, DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR), " +
                            " DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE lo.customer=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setInt(5, p);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                    }
                    break;
                case 2:
                    sqlQuery = "SELECT x.Line, DATE_SUB(Concat(left(?,11),IFNULL(x.open,'11:00:00')), INTERVAL eO HOUR) Open, " +
                            " DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE lo.customer=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    stmt.setInt(6, p);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                    }
                    break;
                case 3:
                    sqlQuery = "SELECT x.Line, DATE_SUB(If(IFNULL(x.close,'02:00:00')>'12:0:0',concat(left(?,11),IFNULL(x.close,'02:00:00')),concat(left(ADDDATE(?,INTERVAL 1 DAY),11),IFNULL(x.close,'02:00:00'))), INTERVAL eO HOUR) Close, " +
                            " DATE_SUB(Concat(left(?,11),IFNULL(x.preOpen,'07:00:00')), INTERVAL eO HOUR)" +
                            " FROM (Select l.id Line, " +
                            " CASE DAYOFWEEK(?)" +
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
                            " FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN location lo ON lo.id = b.location" +
                            " LEFT JOIN locationHours lH ON lH.location = lo.id" +
                            " WHERE l.id=?) AS x;";
                    stmt = conn.prepareStatement(sqlQuery);
                    stmt.setString(1, businessDate);
                    stmt.setString(2, businessDate);
                    stmt.setString(3, businessDate);
                    stmt.setString(4, businessDate);
                    stmt.setString(5, businessDate);
                    stmt.setInt(6, p);
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        dateSet = new ReportDateSet(newDateFormat.parse(rs.getString(2)), newDateFormat.parse(rs.getString(3)));
                    }
                    break;
                default:
                    break;
            }
        } catch (SQLException sqle) {}
         catch (Exception pe) {}
        return dateSet;
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
