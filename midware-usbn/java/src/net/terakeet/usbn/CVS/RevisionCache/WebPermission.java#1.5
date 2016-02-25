/*
 * WebPermission.java
 *
 * Created on October 25, 2006, 10:00 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package net.terakeet.usbn;

import net.terakeet.soapware.RegisteredConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**  A web permission represents the different access levels a user can have on
 *  the USBN manager site.
 *
 * @author Ryan Garver
 */
public class WebPermission {
    
    /**  Permission Levels:
     *  1:  SuperAdmin
     *  3:  Customer Admin
     *  5:  Manager
     *  7:  Read Only
     *  10: No Access
     */
    private static final int SUPERADMIN = 1;
    private static final int CUST_ADMIN = 3;
    private static final int MANAGER = 5;
    private static final int READ_ONLY = 7;
    private static final int IT_ADMIN = 9;
    private static final int NO_ACCESS = 10;   
    
    final int level;
    
    /** Creates a permission level from a permission int */
    public WebPermission(int permissionLevel) {
        if (permissionLevel > 0) {
            level = permissionLevel;
        } else {
            level = NO_ACCESS;
        }
    }
    /** Create a permission level from a string.  The string must be a valid integer,
     * otherwise this permission level will be 'no access'
     */
    public WebPermission(String permissionLevel) {
        int parsedLevel = 10;
        try {
            parsedLevel = Integer.parseInt(permissionLevel);
        } catch (Exception e) {}
        level = parsedLevel;
    }
    
    /** returns true if this permission has read-access */
    public boolean canRead() {
        return level <= READ_ONLY;
    }
    /** returns true if this permission has write-access (manager) */
    public boolean canWrite() {
        return level <= MANAGER;
    }
    /** returns true if this permission has read-access */
    public boolean canCustomerAdmin() {
        return level <= CUST_ADMIN;
    }
    /** returns true if this permission has write-access (manager) */
    public boolean canSuperAdmin() {
        return level <= SUPERADMIN;
    }
    /** returns true if this permission has IT Admin access */
    public boolean canITAdmin() {
        return level <= IT_ADMIN;
    }
    
    /** returns the numerical permission level, for use in the database */
    public int getLevel() {
        return level;
    }
    
    /** Create a new read-only permission */
    public static WebPermission instanceOfReadOnly() {
        return new WebPermission(READ_ONLY);
    }
    /** Create a new manager (read/write) permission */
    public static WebPermission instanceOfManager() {
        return new WebPermission(MANAGER);
    }
    /** Create a permission level with no access */
    public static WebPermission instanceOfNoAccess() {
        return new WebPermission(NO_ACCESS);
    }
    /** Create a permission level with no access */
    public static WebPermission instanceOfCustomerAdmin() {
        return new WebPermission(CUST_ADMIN);
    }
    /** Create a permission level with no access */
    public static WebPermission instanceOfUsbnAdmin() {
        return new WebPermission(SUPERADMIN);
    }
    
    /** Create a permission level with no access */
    public static WebPermission instanceOfITAdmin() {
        return new WebPermission(IT_ADMIN);
    }
    
    /**  Find the web permission for a user at a specfic location.  This requires
     * an open db connection
     *  @param user the user to check
     *  @param location the location to check
     *  @conn an open db connection
     *  @returns the permission level for this user at this location, or an
     *      instance of NO ACCESS if the user or location cannot be found
     */
    public static WebPermission permissionAt(int user, int location, RegisteredConnection conn) throws SQLException {
        
        PreparedStatement stmt;
        ResultSet rs;
        
        String userInfo = " SELECT customer,isManager FROM user WHERE id=? ";
        String locInfo = " SELECT customer FROM location WHERE id=? ";
        String mapAccess = "SELECT securityLevel FROM userMap WHERE user=? AND location=? ";
        
        int userCustomer;
        int userManager;
        
        // check for super user
        
        stmt = conn.prepareStatement(userInfo);
        stmt.setInt(1,user);
        rs = stmt.executeQuery();
        if (rs.next()) {
            userCustomer = rs.getInt(1);
            userManager = rs.getInt(2);
            if (userCustomer == 0) {
                // user is a superuser
                return instanceOfUsbnAdmin();
            }
        } else {
            return instanceOfNoAccess();
        }
        // check for customer admin
        if (userManager > 0) {
            stmt = conn.prepareStatement(locInfo);
            stmt.setInt(1,location);
            rs = stmt.executeQuery();
            if (rs.next()) {
                int locCustomer = rs.getInt(1);
                if (locCustomer == userCustomer) {
                    return instanceOfCustomerAdmin();
                }
            }
        }
        // check for map-access
        stmt = conn.prepareStatement(mapAccess);
        stmt.setInt(1,user);
        stmt.setInt(2,location);
        rs = stmt.executeQuery();
        if (rs.next()) {
            return new WebPermission(rs.getInt(1));
        }
        
        // if we didn't return anything yet, then the user has no access
        return instanceOfNoAccess();
    }
    
    public String toString() {
        String label = "Unknown";
        if (canSuperAdmin()) {
            label = "Super Admin";
        } else if (canCustomerAdmin()) {
            label = "Customer Admin";
        } else if (canWrite()) {
            label = "Manager";
        } else if (canRead()) {
            label = "Read Only";
        } else {
            label = "No Access";
        }
        return label+" ("+level+")";
    }
}
