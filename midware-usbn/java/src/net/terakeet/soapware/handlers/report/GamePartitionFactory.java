/*
 * GameDatePartitionFactory.java
 *
 * Created on September 1, 2005, 9:36 AM
 *
 */

package net.terakeet.soapware.handlers.report;
import net.terakeet.soapware.RegisteredConnection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import net.terakeet.soapware.HandlerException;
import java.sql.SQLException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Date;
import java.util.Iterator;

/** GameDatePartitionFactory
 *
 */
public class GamePartitionFactory {
    
    /** Static class, not instantiable */
    private GamePartitionFactory() {
    }
    
    /**  for testing.  Preferred method is createPartitions(ReportPeriod rp)
     */
    private static SortedSet<GamePartition> createPartitions(Date startDate,Date endDate, int location, RegisteredConnection conn) throws HandlerException {
        TreeSet<GamePartition> ts = new TreeSet<GamePartition>();
        int indexCounter = 0;

        String sql =
                " SELECT e.id, e.date, e.eventDesc " +
                " FROM eventHours e " +
                " WHERE e.location = ? AND e.date BETWEEN ? AND ? " +
                " ORDER BY e.date, e.id ";

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            int fieldCount = 0;
            stmt.setInt(++fieldCount, location);
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(startDate.getTime()));
            stmt.setTimestamp(++fieldCount, new java.sql.Timestamp(endDate.getTime()));
            rs = stmt.executeQuery();
            while (rs.next()){
                ts.add(new GamePartition(rs.getInt(1),indexCounter++));
            }
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        }
        
        return ts;
    }
    
    /** create a set of DatePartitions for a report period */
    public static SortedSet<GamePartition> createPartitions(GameTimeResults gtr) throws HandlerException {
        TreeSet<GamePartition> ts = new TreeSet<GamePartition>();
        int indexCounter = 0;

        try {
            while (gtr.next()) {
                ts.add(new GamePartition(gtr.getGameId(),indexCounter++));
            }
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        } finally {
        }
        return ts;
    }
    
    public static void printPartitions(SortedSet<GamePartition> gps) {
        System.out.println(partitionReport(gps));
    }
    
    public static String partitionReport(SortedSet<GamePartition> gps) {
        StringBuilder result = new StringBuilder();
        Iterator<GamePartition> i = gps.iterator();

        while (i.hasNext()) {
            GamePartition gp = i.next();
            result.append(gp.toString());
            result.append("\n");
        }
        return result.toString();
    }
    
}
