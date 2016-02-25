/*
 * SecurityLevel.java
 *
 * Created on February 21, 2006, 11:07 AM
 *
 */

package net.terakeet.soapware.security;

/** SecurityLevel
 *
 */
public enum SecurityLevel {
    NO_ACCESS(1,"No Access"),
    READ_ONLY (10, "Read Only"),
    REMOTE_WRITE(100, "Remote Write"),
    MANAGE(300, "Manage"),
    ADMIN(500, "Admin");
    
    private final int level;
    private final String name;
    SecurityLevel(int lev, String n) {
        this.level = lev;
        this.name = n;
    }
    
    public boolean canRead() {
        return (level >= 10);
    }
    
    public boolean canWrite() {
        return (level >= 100);
    }
    
    public boolean canManage() {
        return (level >= 300);
    }
    
    public boolean canAdmin() {
        return (level >= 500);
    }
    
    public String getName() {
        return name;
    }
    
    public static SecurityLevel getInstance(int i) {
        int minDiff = Integer.MAX_VALUE;
        SecurityLevel result = NO_ACCESS;
        for (SecurityLevel lev : SecurityLevel.values()) {
            int diff = i - lev.level;
            if (diff >= 0 && diff < minDiff) {
                minDiff = diff;
                result = lev;
            }
        }
        return result;
    }
}
