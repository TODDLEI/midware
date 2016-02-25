/*
 * CustomerKeyFactory.java
 *
 * Created on January 12, 2007, 4:04 PM
 *
 * (c) 2007 Terakeet Corp.
 */
package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.RegisteredConnection;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This generates the GroupingKey for the CUSTOMER type.
 */
public class CustomerKeyFactory extends GroupingKeyFactory {

    int customer;
    HashMap<Integer, Integer> locToCust;
    PreparedStatement lookupStmt;

    public CustomerKeyFactory(ReportDescriptor rd, RegisteredConnection conn) {
        locToCust = new HashMap<Integer, Integer>();
        if (rd.getFilterType() == FilterType.CUSTOMER) {
            customer = Integer.parseInt(rd.getFilter());
        } else {
            customer = 0;
        }
        try {
            lookupStmt = conn.prepareStatement("SELECT customer FROM location WHERE id=?");
            if (customer > 0) {
                PreparedStatement stmt = conn.prepareStatement("SELECT id FROM location WHERE customer=?");
                stmt.setInt(1, customer);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    locToCust.put(new Integer(rs.getInt(1)), new Integer(customer));
                }
                rs.close();
                stmt.close();
            }
        } catch (SQLException sqle) {
            // ignore
        }

    }

    public CustomerKeyFactory(RegisteredConnection conn, int tableType, int tableId) {
        locToCust = new HashMap<Integer, Integer>();
        try {
            switch (tableType) {
                case 1:
                    lookupStmt = conn.prepareStatement("SELECT id FROM customer WHERE id=?");
                    break;
                case 2:
                    lookupStmt = conn.prepareStatement("SELECT customer FROM location WHERE id=?");
                    break;
                case 3:
                    lookupStmt = conn.prepareStatement("SELECT l.customer FROM bar b LEFT JOIN location l ON l.id = b.location WHERE b.id=?");
                    break;
                default:
                    lookupStmt = conn.prepareStatement("SELECT id FROM customer WHERE id=?");
                    break;
            }
        } catch (SQLException sqle) {
            // ignore
        }

    }

    /** Location to Customer Database lookup */
    private int databaseLookup(int parameter) {
        int result = -1;
        try {
            lookupStmt.setInt(1, parameter);
            ResultSet rs = lookupStmt.executeQuery();
            result = rs.getInt(1);
            rs.close();
        } catch (SQLException sqle) {
            //ignore
        }
        return result;
    }

    public GroupingKey getKey(ReportSummary rs) {
        int location = rs.getLocation();
        int locInt = new Integer(location);
        Integer cachedCust = locToCust.get(locInt);
        if (cachedCust == null) {
            cachedCust = new Integer(databaseLookup(location));
            locToCust.put(locInt, cachedCust);
        }
        return new GroupingKey(GroupingType.CUSTOMER, cachedCust.toString());
    }
}
