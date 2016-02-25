/*
 * SecureSession.java
 *
 * Created on February 21, 2006, 11:05 AM
 *
 */

package net.terakeet.soapware.security;

import java.util.StringTokenizer;

/** SecureSession
 *
 */
public class SecureSession {
    
    public final static int MIDWARE_CLASS = 2;
    
    private final SecurityLevel level;
    private final int location;
    private final int client;
    
    /** Creates a new instance of SecureSession */
    private SecureSession(SecurityLevel lev, int loc, int cli) {
        level = lev;
        location = loc;
        client = cli;
    }
    
    
    public SecurityLevel getSecurityLevel() {
        return level;
    }
    
    public int getLocation() {
        return location;
    }
    
    public int getClientId() {
        return client;
    }
    
//    public static SecureSession getInstance(String s) {
//        if (s == null || s.length() == 0) {
//            return new SecureSession(SecurityLevel.NO_ACCESS,-1,0);
//        }
//        try {
//            StringTokenizer values = new StringTokenizer(s,"_");
//            int loc = Integer.parseInt(values.nextToken());
//            int cli = Integer.parseInt(values.nextToken());
//            int lev = Integer.parseInt(values.nextToken());
//            return new SecureSession(SecurityLevel.getInstance(lev),loc,cli);
//        } catch (Exception e) {
//            return new SecureSession(SecurityLevel.NO_ACCESS,-1,0);
//        }
//    }
    
    public static SecureSession getInstance(String s) {
        if (s == null || s.length() == 0) {
            return new SecureSession(SecurityLevel.NO_ACCESS,-1,0);
        }
        try {
            int clazz = Integer.parseInt(s.substring(0,1));
            int dev = Integer.parseInt(s.substring(1,4));
            int sec = Integer.parseInt(s.substring(4,7));
            int loc = Integer.parseInt(s.substring(10));
            if (clazz == MIDWARE_CLASS) {
                return new SecureSession(SecurityLevel.getInstance(sec),loc,dev);
            }
        } catch (Exception e) {
            
        }
        return new SecureSession(SecurityLevel.NO_ACCESS,-1,0);
    }
    
    public String toString() {
        return "[L"+getLocation()+", C"+getClientId()+", "+getSecurityLevel().getName()+"]";
    }
}
