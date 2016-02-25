/*
 * XmlFormatter.java
 *
 * Created on January 2, 2007, 4:23 PM
 *
 * (c) 2007 Terakeet Corp.
 */

package net.terakeet.soapware.handlers.report;


import org.dom4j.Element;
import java.util.*;
import net.terakeet.soapware.RegisteredConnection;
import java.sql.*;
import java.text.DecimalFormat;
/**
 */
public class XmlFormatter {
    Map<GroupingKey, GroupBox> mapMember;
    ReportDescriptor rd;
    ArrayList<ElapsedTime> timers;
    RegisteredConnection conn;
    static final DecimalFormat twoPlaces = new DecimalFormat("0.00");
    
    public XmlFormatter(ReportDescriptor descriptor, Map<GroupingKey,GroupBox> map, RegisteredConnection connection) {
        mapMember = map;
        rd = descriptor;
        conn = connection;
        timers = new ArrayList<ElapsedTime>();
    }
    
    /**
     *   Arrange the data into XML Elements and add them to toAppend. 
     *   If the 'attachLegend' flag is set, this will also return a mapping of ids
     *   to names for any grouping types that need legends (and are supported)
     *   Supported Legend types
     *      PRODUCT
     *      LOCATION
     *   Planned Future Supported Types:
     *      CUSTOMER
     */
    public void format(Element toAppend, boolean attachLegend){
        Set<Integer> productList = new HashSet<Integer>();
        Set<Integer> locationList = new HashSet<Integer>();
        ElapsedTime formatTimer = new ElapsedTime("XML formatting time");
        
        toAppend.addElement("reportDescriptor").addText(rd.toString());
        toAppend.addElement("grouping").addText(rd.getPrimaryGrouping().toString());
        toAppend.addElement("valueGrouping").addText(rd.getValueGrouping().toString());
        Set<GroupingKey> keys = mapMember.keySet();
        for(GroupingKey key : keys){
            Element groupEl = toAppend.addElement("group");
            groupEl.addElement("key").addText(key.getValue());
            addToLegendListIfNeeded(key,GroupingType.PRODUCT,attachLegend,productList);
            addToLegendListIfNeeded(key,GroupingType.LOCATION,attachLegend,locationList);
            GroupBox box = mapMember.get(key);
            Set<GroupingKey> boxKeys = box.keySet();
            for(GroupingKey boxKey : boxKeys){
                ValueBox vBox = box.get(boxKey);
                Element valueEl = groupEl.addElement("value");
                valueEl.addElement("key").addText(boxKey.getValue());
                if (rd.includePoured()) {
                    valueEl.addElement("poured").addText(twoPlaces.format(vBox.get(OunceType.POURED)));
                }
                if (rd.includeSold()) {
                    valueEl.addElement("sold").addText(twoPlaces.format(vBox.get(OunceType.SOLD)));
                }  
                addToLegendListIfNeeded(boxKey,GroupingType.PRODUCT,attachLegend,productList);
                addToLegendListIfNeeded(boxKey,GroupingType.LOCATION,attachLegend,locationList);
                //valueEl.addElement("contents").addText(vBox.toString());
                //toAppend.addElement("Test").addElement(key.getValue()).addElement(boxKey.getValue()).addText(vBox.toString());
            }
        }
        if (attachLegend) {
            ElapsedTime legendTimer = new ElapsedTime("XML formatting (legend) subset");
            Element legend = toAppend.addElement("legend");
            attachProductLegend(productList,legend);
            attachLocationLegend(locationList,legend);
            legendTimer.stopTimer();
            timers.add(legendTimer);
        }
        formatTimer.stopTimer();
        timers.add(formatTimer);
        
    }
    
    /** Adds the keys value to a legend list if we are attaching a legend and the type matches
     */
    private void addToLegendListIfNeeded(GroupingKey key, GroupingType type, boolean attachLegend, Set<Integer> legendList) {
         if (attachLegend && key.getType()==type) {
            try {
                legendList.add(new Integer(Integer.parseInt(key.getValue()))); 
            } catch (NumberFormatException nfe) { 
                //ignore
            }
         }
    }
    
    /**  Product legend format:
     *    <product>
     *      <id>
     *      <name>
     *    </product>
     */
    public void attachProductLegend(Set<Integer> productIds, Element legend) {
        if (productIds != null && productIds.size() > 0) {
            HashMap<Integer,String> productNames = new HashMap<Integer,String>(productIds.size());
            PreparedStatement stmt = null;
            // Build our name lookup hash
            try {
                stmt = conn.prepareStatement("SELECT id,name FROM product");
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Integer id = new Integer(rs.getInt(1));
                    String name = rs.getString(2);
                    productNames.put(id,name);
                }
            } catch (SQLException sqle) {
                //ignore
            } finally {
                try { stmt.close(); } catch (Exception e) {}
            }
            // create the legend
            for (Integer toLookup : productIds) {
                String lookup = productNames.get(toLookup);
                if (lookup == null || lookup == "") {
                    lookup = "Unknown Product";
                }
                Element product = legend.addElement("product");
                product.addElement("id").addText(String.valueOf(toLookup));
                product.addElement("name").addText(String.valueOf(lookup));
            }
        }        
    }
 
    /**  Product legend format:
     *    <location>
     *      <id>
     *      <name>
     *    </location>
     */
    public void attachLocationLegend(Set<Integer> locationIds, Element legend) {
        final int SINGLE_LOOKUP_CUTOFF = 1;
        HashMap<Integer,String> locationNames = new HashMap<Integer,String>(locationIds.size());
        if (locationIds != null && locationIds.size() > 0) {
            PreparedStatement stmt = null;
            // Three cases: 
            // (1)   Location ID below a threshold:  single lookups
            // (2)   Multiple IDs, descriptor customer filter:  lookup all locations for this customer
            // (3)   Multiple IDs, no customer filter:  lookup all locations
            if (locationIds.size() <= SINGLE_LOOKUP_CUTOFF) {
                //  CASE ONE
                try {
                    stmt = conn.prepareStatement("SELECT name FROM location WHERE id=?");
                    for (Integer toLookup : locationIds) {
                        stmt.setInt(1,toLookup.intValue());
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                            String name = rs.getString(1);
                            locationNames.put(toLookup,name);
                        }
                    }
                } catch (SQLException sqle) {
                    //ingore
                } finally {
                    try { stmt.close(); } catch (Exception e) {}
                }
            } else {
                //  CASE TWO
                if (rd.getFilterType() == FilterType.CUSTOMER) {
                    String customerId = rd.getFilter();
                    try {
                        stmt = conn.prepareStatement("SELECT id,name FROM location WHERE customer=?");
                        stmt.setString(1,customerId);
                        ResultSet rs = stmt.executeQuery();
                        while (rs.next()) {
                            Integer id = new Integer(rs.getInt(1));
                            String name = rs.getString(2);
                            locationNames.put(id,name);
                        }
                    } catch (SQLException sqle) {
                        //ignore
                    }
                //  CASE THREE
                } else {
                    try {
                        stmt = conn.prepareStatement("SELECT id,name FROM location");
                        ResultSet rs = stmt.executeQuery();
                        while (rs.next()) {
                            Integer id = new Integer(rs.getInt(1));
                            String name = rs.getString(2);
                            locationNames.put(id,name);
                        }
                    } catch (SQLException sqle) {
                        //ignore
                    }                    
                }
            }
            
            // create the legend from the hash we built
            for (Integer toLookup : locationIds) {
                String lookup = locationNames.get(toLookup);
                if (lookup == null || lookup == "") {
                    lookup = "Unknown Location";
                }
                Element product = legend.addElement("location");
                product.addElement("id").addText(String.valueOf(toLookup));
                product.addElement("name").addText(String.valueOf(lookup));
            }
        }        
    }
    
    public ArrayList<ElapsedTime> getBenchmarkData(){
        return timers;
    }
    
}
