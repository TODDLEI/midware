/*
 * ResultSets.java
 *
 * Created on September 1, 2005, 1:51 PM
 *
 */


package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.text.*;

/**
 *  A wrapper class for a java.sql.ResultSet
 * @author Administrator
 */
public class ResultSets {
    
    private static MidwareLogger logger = new MidwareLogger(ResultSets.class.getName());

    public static Map<Integer, ProductData> getPoured(Integer tableId, Integer tableType, String periodStr, String periodDetail, boolean byProduct, String startTime, String endTime, RegisteredConnection conn)
            throws HandlerException {

        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
        Map<Integer, ProductData> productSet= null;
        String conditionString              = "";

        logger.portalAction("getPoured");

        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period             = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            SortedSet<DatePartition> dps    = DatePartitionFactory.createPartitions(period);
            //logger.debug("Created partitions: \n"+DatePartitionFactory.partitionReport(dps));
            DatePartitionTree dpt           = new DatePartitionTree(dps);

            ReportResults rrs               = null;

            switch (tableType) {
                case 2:
                    conditionString         = " WHERE b.location = ? ";
                    rrs                     = ReportResults.getResultsByLocation(period, 0, byProduct, false, tableId, 0, conn);
                    break;
                case 3:
                    conditionString         = " WHERE b.id = ? ";
                    rrs                     = ReportResults.getResultsByBar(period, 0, byProduct, tableId, 0, conn);
                    break;
                case 6:
                    conditionString         = " WHERE b.id = ? ";
                    rrs                     = ReportResults.getResultsByBar(period, 0, byProduct, tableId, 0, conn);
                    break;
                default:
                    break;
            }
            PeriodStructure pss[]           = null;
            PeriodStructure ps              = null;
            int dpsSize                     = dps.size();
            int index;
            if (dpsSize > 0) {
                pss                         = new PeriodStructure[dpsSize];
                //this is a temporary array of the DatePartitions used to construct the PeriodStructures start dates
                Object[] dpa                = dps.toArray();
                for (int i = 0; i < dpsSize; i++) {
                    // create a new PeriodStructure and link it to the previous one (or null for the first)
                    ps                      = new PeriodStructure(ps, ((DatePartition) dpa[i]).getDate());
                    pss[i]                  = ps;
                }
                int debugCounter = 0;
                while (rrs.next()) {
                    index                   = dpt.getIndex(rrs.getDate());
                    pss[index].addReading(rrs.getLine(), rrs.getValue(), rrs.getDate(), rrs.getQuantity());
                    debugCounter++;
                    //logger.debug("#"+debugCounter+" ["+index+"]: "+"L "+rrs.getLine()+" V: "+rrs.getValue()+" D: "+rrs.getDate().toString());
                }
                rrs.close();
                //logger.debug("Processed " + debugCounter + " readings");
                for (int i = 0; i < dpsSize; i++) {
                    pss[i].setData(conn, tableId, conditionString);
                    Map<Integer, Double> lineMap
                                            = pss[i].getValues(conn);
                    if (null != lineMap && lineMap.size() > 0) {
                        Map<Integer, String> locationMap
                                            = ps.getLocation(conn);
                        productSet          =  ReportResults.linesToProducts(locationMap, lineMap, conn);
                    }
                }
                //logger.debug("Total Poured = " + twoDForm.format(totalPoured));
                ReportResults.clearLineCache();
            }
        } catch (ParseException pe) {
            String badDate                  = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        }
        return productSet;
    }

    public static Map<Integer, ReconciliationRecord> getSold(Integer tableId, Integer tableType, String periodStr, String periodDetail, boolean byProduct, String startTime, String endTime, RegisteredConnection conn)
            throws HandlerException {

        java.text.DecimalFormat twoDForm    = new java.text.DecimalFormat("#.##");
        java.text.DateFormat timeParse      = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
        int location                        = 0, bar = 0;
        String selectCostCenter             = "SELECT b.location FROM costCenter c LEFT JOIN bar b ON b.id = c.bar ";

        // A cache of beverage ingredient sets (maps PLU -> RRecSet)
        Map<String, Set<ReconciliationRecord>> ingredCache
                                            = new HashMap<String, Set<ReconciliationRecord>>();

        // Maps product ids to RRecs (oz values);
        Map<Integer, ReconciliationRecord> productSet = new HashMap<Integer, ReconciliationRecord>();

        logger.portalAction("getSold");

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        try {
            java.util.Date start            = timeParse.parse(startTime);
            java.util.Date end              = timeParse.parse(endTime);

            PeriodType periodType           = PeriodType.parseString(periodStr);
            if (null == periodType) {
                throw new HandlerException("Invalid period type: " + periodStr);
            }
            //For Testing purposes change the below date.
            //start = timeParse.parse("2009-06-09 08:00:00");
            //end = timeParse.parse("2009-06-10 08:00:00");
            ReportPeriod period = null;
            try {
                period                      = new ReportPeriod(periodType, periodDetail, start, end);
            } catch (IllegalArgumentException e) {
                throw new HandlerException(e.getMessage());
            }

            OldSalesResults srs             = null;

            switch (tableType) {
                case 2:
                    location                = tableId;
                    selectCostCenter        += " WHERE c.location = ? ";
                    stmt                    = conn.prepareStatement(selectCostCenter);
                    stmt.setInt(1, location);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        srs                 = OldSalesResults.getResultsByLocationCostCenter(period, location, conn);
                    } else {
                        srs                 = OldSalesResults.getResultsByLocationPlu(period, location, conn);
                    }
                    break;
                case 3:
                    bar                     = tableId;
                    selectCostCenter        += " WHERE c.bar = ? ";
                    stmt                    = conn.prepareStatement(selectCostCenter);
                    stmt.setInt(1, tableId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        location            = rs.getInt(1);
                        srs                 = OldSalesResults.getResultsByBarCostCenters(period, location, bar, conn);
                    } else {
                        srs                 = OldSalesResults.getResultsByBarPlu(period, bar, conn);
                    }
                    break;
                case 6:
                    bar                     = tableId;
                    selectCostCenter        += " WHERE c.bar = ? ";
                    stmt                    = conn.prepareStatement(selectCostCenter);
                    stmt.setInt(1, tableId);
                    rs                      = stmt.executeQuery();
                    if (rs.next()) {
                        location            = rs.getInt(1);
                        srs                 = OldSalesResults.getResultsByBarCostCenters(period, location, bar, conn);
                    } else {
                        srs                 = OldSalesResults.getResultsByBarPlu(period, bar, conn);
                    }
                    break;
                default:
                    break;
            }

            int totalRecords                = 0;
            while (srs.next()) {
                String product              = srs.getPlu();
                double value                = srs.getValue();

                Set<ReconciliationRecord> baseSet = null;
                if (ingredCache.containsKey(product)) {
                    baseSet                 = ingredCache.get(product);
                } else { // we need to do a db lookup and add the ingredients to the cache
                    baseSet                 = ReconciliationRecord.recordByPlu(product, location, bar, 1.0, conn);
                    ingredCache.put(product, baseSet);
                }
                Set<ReconciliationRecord> rSet
                                            = ReconciliationRecord.recordByBaseSet(baseSet, value);
                totalRecords                += rSet.size();
                // loop through all RRs and add them to the product set.
                for (ReconciliationRecord rr : rSet) {
                    Integer key             = new Integer(rr.getProductId());
                    ReconciliationRecord existingRecord
                                            = productSet.get(key);
                    if (existingRecord != null) {
                        existingRecord.add(rr);
                    } else {
                        productSet.put(key, rr);
                    }
                }
            }
            logger.debug("Processed " + totalRecords + " reconciliation record(s)");
            // use the product set to create the XML to return.
        } catch (ParseException pe) {
            String badDate                  = (null == startTime) ? "start" : "end";
            throw new HandlerException("Could not parse " + badDate + " date.");
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } 
        return productSet;
    }
    
}
