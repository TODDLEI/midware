/*
 */

package net.terakeet.util;

/**
 *
 * @author Sundar
 */


import java.io.File;
import java.sql.*;
import java.text.BreakIterator;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.*;
import org.dom4j.Element;

public class BevBoxGetRequests {
    private RegisteredConnection conn = null;
    static final String connName = "report";
    private MidwareLogger logger = new MidwareLogger(BevBoxGetRequests.class.getName());
    private static SimpleDateFormat newDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
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
    
    public List<Map<String, Object>> SelectLocationType(int locationId) throws HandlerException {
        
        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String selectType                   = "SELECT type, harpagonOffset, glanola FROM location WHERE id = ?";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;
        
        try {
            stmt                            = conn.prepareStatement(selectType);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            while (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> CheckBevBox(int locationId, int startSystem) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String selectType                   = "SELECT id, name, alert FROM bevBox WHERE location = ? AND startSystem = ?";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(selectType);
            stmt.setInt(1, locationId);
            stmt.setInt(2, startSystem);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            while (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> SelectCooler(int locationId, int system) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String select                       = "SELECT id, alertPoint, (NOW() > SUBDATE(lastDate, INTERVAL 15 MINUTE)) b FROM cooler WHERE location = ? AND system = ?";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(select);
            stmt.setInt(1, locationId);
            stmt.setInt(2, system);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            if (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> SelectLastPoured(int locationId, int system) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String select                       = "SELECT lastPoured, lineCleaning, count FROM system WHERE location=? AND systemId=?";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(select);
            stmt.setInt(1, locationId);
            stmt.setInt(2, system);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            if (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> SelectLines(int locationType, int locationId, int systemId) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String select                       = "SELECT line.lineIndex, line.id, line.ouncesPoured, line.lastPoured, line.lastType, " +
                                            " inventory.id inv, inventory.kegSize, inventory.qtyOnHand, inventory.minimumQty FROM line " +
                                            " LEFT JOIN system ON line.system = system.id LEFT JOIN inventory ON inventory.location = system.location " +
                                            " AND inventory.product = line.product WHERE system.location = ? AND system.systemId = ? " +
                                            " AND line.status = 'RUNNING' AND line.product > 0 AND inventory.id IS NOT NULL;";
        String selectWithKegLine            = "SELECT line.lineIndex, line.id, line.ouncesPoured, line.lastPoured, line.lastType, " +
                                            " inventory.id inv, inventory.kegSize, inventory.qtyOnHand, inventory.minimumQty FROM line " +
                                            " LEFT JOIN system ON line.system = system.id LEFT JOIN kegLine kl ON line.kegLine = kl.id " +
                                            " LEFT JOIN inventory ON inventory.kegLine = kl.id AND inventory.location = system.location " +
                                            " WHERE system.location = ? AND system.systemId = ? AND line.status = 'RUNNING' AND line.product > 0" +
                                            " AND inventory.id IS NOT NULL;";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(locationType == 2 ? selectWithKegLine : select);
            stmt.setInt(1, locationId);
            stmt.setInt(2, systemId);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            while (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> SelectInventory(int locationType, int locationId, int systemId, int maxLineIndex) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String selectInventory              = "SELECT line.id, inv.id inv, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM inventory AS inv " +
                                            " LEFT JOIN line ON inv.product = line.product LEFT JOIN system ON line.system = system.id AND inv.location = system.location " +
                                            " WHERE system.systemId = ? AND inv.location = ? AND line.status = 'RUNNING' AND line.product > 0 ";
        String selectInventoryWithKegLine   = "SELECT line.id, inv.id inv, inv.kegSize, inv.qtyOnHand, inv.minimumQty FROM line " +
                                            " LEFT JOIN kegLine kl ON line.kegLine = kl.id LEFT JOIN inventory inv ON inv.kegLine = kl.id " +
                                            " LEFT JOIN system ON line.system = system.id AND inv.location = system.location " +
                                            " WHERE system.systemId = ? AND inv.location = ? AND line.status = 'RUNNING' AND line.product > 0 " +
                                            " AND line.lineIndex " + (maxLineIndex > 7 ? " > 7 " : " < 8 ");
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(locationType == 2 ? selectInventoryWithKegLine : selectInventory);
            stmt.setInt(1, systemId);
            stmt.setInt(2, locationId);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }

    public List<Map<String, Object>> SelectCoolerException(int cooler, Timestamp start, Timestamp end) throws HandlerException {

        conn                                = DatabaseConnectionManager.getNewConnection(connName, " (BevBoxGetRequests)");
        List<Map<String, Object>> data      = new ArrayList<Map<String, Object>>();
        String select                       = "SELECT id FROM coolerException WHERE cooler = ? AND start BETWEEN ? AND ?";
        ResultSet rs                        = null;
        PreparedStatement stmt              = null;

        try {
            stmt                            = conn.prepareStatement(select);
            stmt.setInt(1, cooler);
            stmt.setTimestamp(2, start);
            stmt.setTimestamp(3, end);
            rs                              = stmt.executeQuery();
            int numColumns                  = rs.getMetaData().getColumnCount();
            if (rs.next())
            {
                Map<String, Object> row     = new LinkedHashMap<String, Object>();
                for (int i = 0; i < numColumns; ++i)
                {
                    String column           = rs.getMetaData().getColumnName(i+1);
                    Object value            = rs.getObject(i+1);
                    row.put(column, value);
                }
                data.add(row);
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.toString());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
        conn.close();
        return data;
    }
    
}
