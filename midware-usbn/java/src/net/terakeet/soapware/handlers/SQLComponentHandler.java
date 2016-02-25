    /*
     * SQLComponentHandler.java
     *
     * Created on February 23, 2011, 15:00 
     *
     */

package net.terakeet.soapware.handlers;

import net.terakeet.soapware.*;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.text.*;
import java.text.SimpleDateFormat;
import net.terakeet.soapware.handlers.report.*;

public class SQLComponentHandler implements Handler{
    
    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    /**
     * Creates a new instance of SQLComponentHandler
     */
    public SQLComponentHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLBOSSHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }
    
    public void handle(Element toHandle, Element toAppend) throws HandlerException{
        
        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");
        
        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);
        
        logger                              = new MidwareLogger(SQLBeerBoardMobileHandler.class.getName(), function);
        logger.debug("SQLComponentHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());
        
        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLComponentHandler)");
        
        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if ("getComponentStatus".equals(function)) {
                getComponentStatus(toHandle, responseFor(function, toAppend));
            } else if ("updateComponentStatus".equals(function)) {
                updateComponentStatus(toHandle, responseFor(function, toAppend));
            } else if ("getDraftBeerData".equals(function)) {
                getDraftBeerData(toHandle, responseFor(function, toAppend));
            } else if ("getBottleBeerData".equals(function)) {
                getBottleBeerData(toHandle, responseFor(function, toAppend));
            } else if ("resetInventoryOnHand".equals(function)) {
                resetInventoryOnHand(toHandle, responseFor(function, toAppend));
            } else {
                logger.generalWarning("Unknown function '" + function + "'.");
            }
        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in ReportHandler: "+e.toString());
                logger.midwareError("XML: " + toHandle.asXML());
                throw new HandlerException(e);
            }
        } finally {
            // Log database use
            int queryCount                  = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " report quer" + (queryCount == 1 ? "y" : "ies"));

            // Log transacctional database use
            queryCount                      = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " transactional quer" + (queryCount == 1 ? "y" : "ies"));

            transconn.close();
        }
        
        logger.xml("response: " + toAppend.asXML());
        
    }
    
    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }
    
    
    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
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
    private void close(Connection c) {
        if (c != null) {
            try { c.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(RegisteredConnection c) {
        c.close();
    }
    
    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }
    private int getCallerId(Element toHandle) throws HandlerException {
        return HandlerUtils.getRequiredInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");
    }
    
    private boolean checkForeignKey(String table, int value) throws SQLException, HandlerException {
        return checkForeignKey(table,"id",value);
    }
    
    private boolean checkForeignKey(String table, String field, int value) throws SQLException, HandlerException {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;
        
        String select = "SELECT " + field + " FROM " + table +
                " WHERE " + field + " = ?";
        
        stmt = transconn.prepareStatement(select);
        stmt.setInt(1, value);
        rs = stmt.executeQuery();
        result = rs.next();
        
        close(rs);
        close(stmt);
        return result;
    }

    /**
     * Deativating all alerts from the night before
     */
    private void getComponentStatus(Element toHandle, Element toAppend) throws HandlerException {

        int component                       = HandlerUtils.getOptionalInteger(toHandle, "component");

        String sql                          = " SELECT c.id, c.name, c.description, cS.date FROM component c LEFT JOIN componentStatus cS ON cS.component = c.id ";
        if (component > 0) {
            sql                             += " WHERE c.id = ? ";
        }

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
            stmt                            = transconn.prepareStatement(sql);
            if (component > 0) {
                stmt.setInt(1, component);
            }
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Element ComponentE1         = toAppend.addElement("component");
                ComponentE1.addElement("componentId").addText(String.valueOf(rs.getInt(1)));
                ComponentE1.addElement("componentName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                ComponentE1.addElement("componentDesc").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                ComponentE1.addElement("componentStatus").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void updateComponentStatus(Element toHandle, Element toAppend) throws HandlerException {
        int componentId                     = HandlerUtils.getRequiredInteger(toHandle, "componentId");

        java.util.Date timestamp            = new java.util.Date();
        String updateComponentStatus        = " UPDATE componentStatus SET date = ? WHERE component = ? ";
        String insertComponentStatus        = " INSERT INTO componentStatus (date, component) VALUES (?,?) ";
        String checkComponentStatus         = " SELECT id FROM componentStatus WHERE component = ? ";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            stmt                            = transconn.prepareStatement(checkComponentStatus);
            stmt.setInt(1, componentId);
            rs                              = stmt.executeQuery();
            if (rs.next()) {
                stmt                        = transconn.prepareStatement(updateComponentStatus);
            } else {
                stmt                        = transconn.prepareStatement(insertComponentStatus);
            }
            stmt.setTimestamp(1, toSqlTimestamp(timestamp));
            stmt.setInt(2, componentId);
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            logger.dbError("Database error in updateComponentStatus: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /** Converts a java.util.Date to a java.sql.Date
     */
    private java.sql.Timestamp toSqlTimestamp(java.util.Date d) {
        return new java.sql.Timestamp(d.getTime());
    }

    /**
     */
    private void getDraftBeerData(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String startStr                     = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endStr                       = HandlerUtils.getRequiredString(toHandle, "endDate");

        String sql                          = " SELECT l.easternOffset FROM location l WHERE l.id = ? ";
        String getActiveProducts            = " SELECT p.id, p.name, i.qtyOnHand, i.minimumQty FROM inventory i LEFT JOIN product p ON p.id = i.product " +
                                            " WHERE p.pType = 1 AND i.isActive = 1 AND i.location = ? ";
        String getActiveLines               = " SELECT l.id, l.product FROM line l LEFT JOIN bar b ON b.id = l.bar WHERE b.location = ? ";
        String getLocationBeverages         = " SELECT b.plu, GROUP_CONCAT(CONCAT(i.product, ':', i.ounces)) FROM beverage b LEFT JOIN ingredient i ON b.id = i.beverage " +
                                            " WHERE b.pType = 1 AND b.location = ? GROUP BY b.id ORDER BY b.id ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        Map<Integer, String> productMap     = new HashMap<Integer, String>();
        Map<Integer, Integer> lineProductMap= new HashMap<Integer, Integer>();
        Map<String, Map<Integer, Double>> beverageMap
                                            = new HashMap<String, Map<Integer, Double>>();
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm:ss");

        try {
            int offset                      = 0;
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                offset                      = rs.getInt(1);
            }

            stmt                            = transconn.prepareStatement(getActiveProducts);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                productMap.put(rs.getInt(1), rs.getString(2));
                Element elProd              = toAppend.addElement("inventory");
                elProd.addElement("productId").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                elProd.addElement("qtyOnHand").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getDouble(3))));
                elProd.addElement("alertPoint").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getDouble(4))));
            }
            
            String activeLines              ="0";
            stmt                            = transconn.prepareStatement(getActiveLines);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                lineProductMap.put(rs.getInt(1), rs.getInt(2));
                activeLines                 += ", " + rs.getString(1);
            }

            String getPouredQty             = "SELECT line, SUM(quantity) FROM reading WHERE line IN (" + activeLines + ") AND date BETWEEN ? AND ? AND type = ? " +
                                            " GROUP BY line ORDER BY line";
            
            java.util.Date start            = timeParse.parse(startStr);
            java.util.Date end              = timeParse.parse(endStr);

            PeriodType periodType           = PeriodType.parseString("Daily");
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + "Daily");
            }
            //For Testing purposes change the below date.
            //start                           = timeParse.parse("2009-06-09 08:00:00");
            //end                             = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, String.valueOf(Math.round(8 - offset)), start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            stmt                            = transconn.prepareStatement(getPouredQty);
            stmt.setTimestamp(1, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(2, new java.sql.Timestamp(period.getEndDate().getTime()));
            stmt.setInt(3, 0);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                int productId               = lineProductMap.get(rs.getInt(1));
                Element elProd              = toAppend.addElement("poured");
                elProd.addElement("productId").addText(HandlerUtils.nullToEmpty(String.valueOf(productId)));
                elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(productMap.get(productId)));
                elProd.addElement("value").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getDouble(2))));
            }

            stmt                            = transconn.prepareStatement(getLocationBeverages);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Map<Integer, Double> productOuncesMap
                                            = new HashMap<Integer, Double>();
                String[] beverageIng        = rs.getString(2).split(",");
                for (String bevIng : beverageIng) {
                    String[] ingProduct     = bevIng.split(":");
                    productOuncesMap.put(Integer.parseInt(ingProduct[0]), Double.parseDouble(ingProduct[1]));
                }
                beverageMap.put(rs.getString(1), productOuncesMap);
            }


            Map<Integer, Double> productSoldMap
                                            = new HashMap<Integer, Double>();
            String getSoldQty               = "SELECT pluNumber, SUM(quantity) FROM sales WHERE location = ? AND date BETWEEN ? AND ? " +
                                            " GROUP BY pluNumber ORDER BY pluNumber ";
            stmt                            = transconn.prepareStatement(getSoldQty);
            stmt.setInt(1, locationId);
            stmt.setTimestamp(2, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(3, new java.sql.Timestamp(period.getEndDate().getTime()));
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                if (beverageMap.containsKey(rs.getString(1))) {
                    Map<Integer, Double> productOuncesMap
                                            = beverageMap.get(rs.getString(1));
                    double quantitySold     = rs.getDouble(2);
                    for (Integer productId : productOuncesMap.keySet()) {
                        if (productSoldMap.containsKey(productId)) {
                            double existingOunces
                                            = productSoldMap.get(productId);
                            productSoldMap.put(productId, existingOunces + (productOuncesMap.get(productId) * quantitySold));
                        } else {
                            productSoldMap.put(productId, productOuncesMap.get(productId) * quantitySold);
                        }
                    }
                }
            }
           
            for (Integer productId : productSoldMap.keySet()) {
                Element p                   = toAppend.addElement("sold");
                p.addElement("productId").addText(HandlerUtils.nullToEmpty(String.valueOf(productId)));
                p.addElement("productName").addText(HandlerUtils.nullToEmpty(productMap.get(productId)));
                p.addElement("value").addText(HandlerUtils.nullToEmpty(String.valueOf(productSoldMap.get(productId))));
            }

        } catch (ParseException pe) {
            String badDate                  = (null == startStr) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     */
    private void getBottleBeerData(Element toHandle, Element toAppend) throws HandlerException {

        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String startStr                     = HandlerUtils.getRequiredString(toHandle, "startDate");
        String endStr                       = HandlerUtils.getRequiredString(toHandle, "endDate");

        String sql                          = " SELECT l.easternOffset FROM location l WHERE l.id = ? ";
        String getActiveProducts            = " SELECT p.id, p.name, i.qtyOnHand, i.minimumQty FROM inventory i LEFT JOIN product p ON p.id = i.product " +
                                            " WHERE p.pType = 3 AND i.isActive = 1 AND i.location = ? ";
        String getLocationBeverages         = " SELECT b.plu, GROUP_CONCAT(CONCAT(i.product, ':', i.ounces)) FROM beverage b LEFT JOIN ingredient i ON b.id = i.beverage " +
                                            " WHERE b.pType = 3 AND b.location = ? GROUP BY b.id ORDER BY b.id ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        Map<Integer, String> productMap     = new HashMap<Integer, String>();
        Map<Integer, Integer> lineProductMap= new HashMap<Integer, Integer>();
        Map<String, Map<Integer, Double>> beverageMap
                                            = new HashMap<String, Map<Integer, Double>>();
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm:ss");

        try {
            int offset                      = 0;
            stmt                            = transconn.prepareStatement(sql);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                offset                      = rs.getInt(1);
            }

            stmt                            = transconn.prepareStatement(getActiveProducts);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                productMap.put(rs.getInt(1), rs.getString(2));
                Element elProd              = toAppend.addElement("inventory");
                elProd.addElement("productId").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getInt(1))));
                elProd.addElement("productName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                elProd.addElement("qtyOnHand").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getDouble(3))));
                elProd.addElement("alertPoint").addText(HandlerUtils.nullToEmpty(String.valueOf(rs.getDouble(4))));
            }
            java.util.Date start            = timeParse.parse(startStr);
            java.util.Date end              = timeParse.parse(endStr);

            PeriodType periodType           = PeriodType.parseString("Daily");
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + "Daily");
            }
            //For Testing purposes change the below date.
            //start                           = timeParse.parse("2009-06-09 08:00:00");
            //end                             = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, String.valueOf(Math.round(8 - offset)), start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            stmt                            = transconn.prepareStatement(getLocationBeverages);
            stmt.setInt(1, locationId);
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                Map<Integer, Double> productOuncesMap
                                            = new HashMap<Integer, Double>();
                String[] beverageIng        = rs.getString(2).split(",");
                for (String bevIng : beverageIng) {
                    String[] ingProduct     = bevIng.split(":");
                    productOuncesMap.put(Integer.parseInt(ingProduct[0]), Double.parseDouble(ingProduct[1]));
                }
                beverageMap.put(rs.getString(1), productOuncesMap);
            }

            Map<Integer, Double> productSoldMap
                                            = new HashMap<Integer, Double>();
            String getSoldQty               = "SELECT pluNumber, SUM(quantity) FROM sales WHERE location = ? AND date BETWEEN ? AND ? " +
                                            " GROUP BY pluNumber ORDER BY pluNumber ";
            stmt                            = transconn.prepareStatement(getSoldQty);
            stmt.setInt(1, locationId);
            stmt.setTimestamp(2, new java.sql.Timestamp(period.getStartDate().getTime()));
            stmt.setTimestamp(3, new java.sql.Timestamp(period.getEndDate().getTime()));
            rs                              = stmt.executeQuery();
            while (rs.next()) {
                if (beverageMap.containsKey(rs.getString(1))) {
                    Map<Integer, Double> productOuncesMap
                                            = beverageMap.get(rs.getString(1));
                    double quantitySold     = rs.getDouble(2);
                    for (Integer productId : productOuncesMap.keySet()) {
                        if (productSoldMap.containsKey(productId)) {
                            double existingOunces
                                            = productSoldMap.get(productId);
                            productSoldMap.put(productId, existingOunces + (productOuncesMap.get(productId) * quantitySold));
                        } else {
                            productSoldMap.put(productId, productOuncesMap.get(productId) * quantitySold);
                        }
                    }
                }
            }

            for (Integer productId : productSoldMap.keySet()) {
                Element p                   = toAppend.addElement("sold");
                p.addElement("productId").addText(HandlerUtils.nullToEmpty(String.valueOf(productId)));
                p.addElement("productName").addText(HandlerUtils.nullToEmpty(productMap.get(productId)));
                p.addElement("value").addText(HandlerUtils.nullToEmpty(String.valueOf(productSoldMap.get(productId))));
            }
        } catch (ParseException pe) {
            String badDate                  = (null == startStr) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    private void resetInventoryOnHand(Element toHandle, Element toAppend) throws HandlerException {
        int locationId                      = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        int productId                       = HandlerUtils.getOptionalInteger(toHandle, "productId");
        int lineId                          = HandlerUtils.getOptionalInteger(toHandle, "lineId");
        double resetValue                   = HandlerUtils.getRequiredDouble(toHandle, "resetValue");
        PreparedStatement stmt              = null;

        try {
            if (productId >= 0) {
                stmt                        = transconn.prepareStatement("UPDATE inventory SET qtyOnHand = ? WHERE location = ? AND product = ?;");
                stmt.setDouble(1, resetValue);
                stmt.setInt(2, locationId);
                stmt.setInt(3, productId);
                stmt.executeUpdate();
            } else if (lineId >= 0) {
                stmt                        = transconn.prepareStatement("UPDATE inventory SET qtyOnHand = ? WHERE location = ? AND kegLine = ?");
                stmt.setDouble(1, resetValue);
                stmt.setInt(2, locationId);
                stmt.setInt(3, lineId);
                stmt.executeUpdate();
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error in resetInventoryOnHand: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
        }
    }
}
