/*
 * SQLQueryExtractor.java
 */

package net.terakeet.usbn;

import net.terakeet.soapware.RegisteredConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class SQLQueryExtractor {

    private static final String selectBuisnessHours = "SELECT CONCAT(?, ' 0', TRUNCATE((7 - l.easternoffset),0), ':00:00') Open, " +
                " CONCAT(ADDDATE(?, INTERVAL 1 DAY), ' 0', TRUNCATE((7 - l.easternoffset),0), ':00:00') Close " +
                " FROM location l WHERE l.id = ?;";
    
    private static final String selectAfterHours = "SELECT " +
                "DATE_SUB(If(x.close>'12:0:0',concat(left(?,11),x.close),concat(left(adddate(?,INTERVAL 1 DAY),11),x.close)), INTERVAL eO HOUR) Close, " +
                "DATE_SUB(Concat(left(adddate(?,1),11),x.Open), INTERVAL eO HOUR) Open FROM " +
                "(Select CASE DAYOFWEEK(?) " +
                "WHEN 1 THEN Right(lH.closeSun,8) " +
                "WHEN 2 THEN Right(lH.closeMon,8) " +
                "WHEN 3 THEN Right(lH.closeTue,8) " +
                "WHEN 4 THEN Right(lH.closeWed,8) " +
                "WHEN 5 THEN Right(lH.closeThu,8) " +
                "WHEN 6 THEN Right(lH.closeFri,8) " +
                "WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                "CASE DAYOFWEEK(adddate(?,INTERVAL 1 DAY)) " +
                "WHEN 1 THEN Right(lH.preOpenSun,8) " +
                "WHEN 2 THEN Right(lH.preOpenMon,8) " +
                "WHEN 3 THEN Right(lH.preOpenTue,8) " +
                "WHEN 4 THEN Right(lH.preOpenWed,8) " +
                "WHEN 5 THEN Right(lH.preOpenThu,8) " +
                "WHEN 6 THEN Right(lH.preOpenFri,8) " +
                "WHEN 7 THEN Right(lH.preOpenSat,8) END open, " +
                "l.easternOffset eO " +
                "FROM locationHours lH JOIN location l ON lH.location=l.id " +
                "WHERE l.id=?) AS x;";
    private static final String selectOpenHours = "SELECT " +
                " DATE_SUB(Concat(left(?,11),x.open), INTERVAL eO HOUR) Open, " +
                " DATE_SUB(If(x.close>'12:0:0',concat(left(?,11),x.close),concat(left(subdate(?,INTERVAL 1 DAY),11),x.close)), INTERVAL eO HOUR) Close " +
                " FROM (Select CASE DAYOFWEEK(?) " +
                " WHEN 1 THEN Right(lH.openSun,8) " +
                " WHEN 2 THEN Right(lH.openMon,8) " +
                " WHEN 3 THEN Right(lH.openTue,8) " +
                " WHEN 4 THEN Right(lH.openWed,8) " +
                " WHEN 5 THEN Right(lH.openThu,8) " +
                " WHEN 6 THEN Right(lH.openFri,8) " +
                " WHEN 7 THEN Right(lH.openSat,8) END open, " +
                " CASE DAYOFWEEK(?) " +
                " WHEN 1 THEN Right(lH.closeSun,8) " +
                " WHEN 2 THEN Right(lH.closeMon,8) " +
                " WHEN 3 THEN Right(lH.closeTue,8) " +
                " WHEN 4 THEN Right(lH.closeWed,8) " +
                " WHEN 5 THEN Right(lH.closeThu,8) " +
                " WHEN 6 THEN Right(lH.closeFri,8) " +
                " WHEN 7 THEN Right(lH.closeSat,8) END close, " +
                " l.easternOffset eO " +
                " FROM locationHours lH JOIN location l ON lH.location=l.id " +
                " WHERE l.id=?) AS x; ";
    private static final String selectPreOpenHours = "SELECT " +
                "DATE_SUB(Concat(left(?,11),x.preOpen), INTERVAL eO HOUR) preOpen, " +
                "DATE_SUB(Concat(left(?,11),x.Open), INTERVAL eO HOUR) Open FROM " +
                "(Select CASE DAYOFWEEK(?) " +
                "WHEN 1 THEN Right(lH.preOpenSun,8) " +
                "WHEN 2 THEN Right(lH.preOpenMon,8) " +
                "WHEN 3 THEN Right(lH.preOpenTue,8) " +
                "WHEN 4 THEN Right(lH.preOpenWed,8) " +
                "WHEN 5 THEN Right(lH.preOpenThu,8) " +
                "WHEN 6 THEN Right(lH.preOpenFri,8) " +
                "WHEN 7 THEN Right(lH.preOpenSat,8) END preOpen, " +
                "CASE DAYOFWEEK(?) " +
                "WHEN 1 THEN Right(lH.openSun,8) " +
                "WHEN 2 THEN Right(lH.openMon,8) " +
                "WHEN 3 THEN Right(lH.openTue,8) " +
                "WHEN 4 THEN Right(lH.openWed,8) " +
                "WHEN 5 THEN Right(lH.openThu,8) " +
                "WHEN 6 THEN Right(lH.openFri,8) " +
                "WHEN 7 THEN Right(lH.openSat,8) END open, " +
                "l.easternOffset eO " +
                "FROM locationHours lH JOIN location l ON lH.location=l.id " +
                "WHERE l.id=?) AS x;";

    final int queryNumber;

     public SQLQueryExtractor(int queryIdentifier) {
        if (queryIdentifier > 0) {
            queryNumber = queryIdentifier;
        } else {
            queryNumber = 0;
        }
    }

    /** Creates a permission level from a permission int */
    public String SQLQueryExtractor() {
        String sqlQuery = "";
        switch (queryNumber) {
            case 0: return sqlQuery;
            case 1: return selectPreOpenHours;
            case 2: return selectOpenHours;
            case 3: return selectAfterHours;
            default:
                    return sqlQuery;
        }
    }
   
}
