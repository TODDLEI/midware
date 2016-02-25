/*
 * ReconciliationRecord.java
 *
 * Created on October 3, 2005, 10:12 AM
 *
 */

package net.terakeet.soapware.handlers.report;

import java.sql.*;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.handlers.ReportHandler;
import net.terakeet.util.MidwareLogger;

/** ReconciliationRecord
 *  A reconcilliation record is a product, and a value in ounces.
 */
public class ReconciliationRecord {

    private int indentifierType;
    private int indentifier;
    private int locationId;
    private int productId;
    private double value;
    private String name;
    private static MidwareLogger logger = new MidwareLogger(ReconciliationRecord.class.getName());
    
    /** Creates a new instance of ReconciliationRecord */
    private ReconciliationRecord(int iType, int iValue, int lid, int pid, String n, double v) {
        indentifierType = iType;
        indentifier = iValue;
        locationId = lid;
        productId = pid;
        name = n;
        value = v;
    }
    
    public boolean equals(Object o) {
        if (o instanceof ReconciliationRecord) {
            ReconciliationRecord rr = (ReconciliationRecord) o;
            if (this.productId == (rr.productId)) {
                return true;
            }
        } 
        return false;
    }
    
    public boolean add(ReconciliationRecord rr) {
        if (this.equals(rr)) {
            this.value += rr.value;
            return true;
        }
        return false;
    }

    public double getValue() {
        return this.value;
    }
    
    public void setValue(double d) {
        this.value = d;
    }
    
    public int hashCode() {
        return productId * 23;
    }

    public int getIdentifier() {
        return indentifier;
    }

    public int getIdentifierType() {
        return indentifierType;
    }

    public int getLocation() {
        return locationId;
    }
    
    public int getProductId() {
        return productId;
    }
    
    public String getName() {
        return name;
    }

    /** Creates a new ReconciliationRecord using the database ID of a product.
     *  @param id the database product id
     *  @param val the quantity in ounces
     *  @param conn an open database connection
     */
    public static ReconciliationRecord recordById(int iType, int iValue, int lid, int pid, double val, RegisteredConnection conn) {
        String productName                  = ProductMap.staticLookup(pid, conn);
        return new ReconciliationRecord(iType, iValue, lid, pid, productName, val);
    }
    
    /** Creates a set of Records from a "base set" of ingredients at quantity 1.  Basically, 
     *  this method returns a new cloned set with each value equal to base.value * val. 
     */
    public static Set<ReconciliationRecord> recordByBaseSet(Set<ReconciliationRecord> baseSet, double val) {
        Set<ReconciliationRecord> result = new HashSet<ReconciliationRecord>();
        for (ReconciliationRecord rec : baseSet) {
            result.add(new ReconciliationRecord(rec.indentifierType, rec.indentifier, rec.locationId, rec.productId, rec.name, rec.value*val));
        }
        return result;
    }
    
    
    public static Set<ReconciliationRecord> recordByPlu(String plu, int location, double val, RegisteredConnection conn) {
        return recordByPlu(plu,location,0,val,conn);
    }
    
    
    /** Creates a new ReconciliationRecord using the product PLU.  This requires
     * a database lookup, so an active db connection is required.
     */
    public static Set<ReconciliationRecord> recordByPlu(String plu, int location, int costCenter, double val, RegisteredConnection conn) {
        String name = "Unknown Product";
         Set<ReconciliationRecord> result = new HashSet<ReconciliationRecord>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        ResultSet costCenterRS = null;
         try {                         
           
        String selectBev        = " SELECT product.name , product.id, ing.ounces, IFNULL(bev.bar,0) " +
                    " FROM beverage as bev " +
                " LEFT JOIN ingredient AS ing ON bev.id = ing.beverage " +
                " LEFT JOIN product ON ing.product = product.id " +
                " WHERE bev.pType = 1 AND bev.location=? AND bev.plu=?";
       
       
            stmt = conn.prepareStatement(selectBev);
            int count = 1;
            stmt.setInt(count++,location);
            stmt.setString(count++, plu);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int identifierType = 0;
                int identifierValue = 0;
                String resultName = rs.getString(1);
                int resultPid = rs.getInt(2);
                double resultOz = val * rs.getDouble(3);
                int barId = rs.getInt(4);
                if (costCenter == 0) {
                    if (barId == 0) {
                        identifierType = 1;
                        identifierValue = location;
                    } else {
                        identifierType = 2;
                        identifierValue = barId;
                    }
                } else {
                    identifierType = 3;
                    identifierValue = costCenter;
                }
                result.add(new ReconciliationRecord(identifierType, identifierValue, location, resultPid, resultName, resultOz));
            }
        } catch (SQLException sqle) { }
         catch (Exception e) {
             
         }
        finally {
            if (costCenterRS != null) { try {costCenterRS.close();} catch (Exception e) {} }
            if (rs != null) { try {rs.close();} catch (Exception e) {} }
            if (stmt != null) { try {stmt.close();} catch (Exception e) {} }
        }
        return result;
    }
    
    
    
}
