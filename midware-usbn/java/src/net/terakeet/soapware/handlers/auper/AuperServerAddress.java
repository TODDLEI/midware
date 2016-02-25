/*
 * AuperServerAddress.java
 *
 * Created on July 1, 2005, 4:09 PM
 */

package net.terakeet.soapware.handlers.auper;

/**
 *
 * @author Ryan Garver
 */
public class AuperServerAddress {
    
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String alias;
    
    /** Creates a new instance of AuperServerAddress */
    public AuperServerAddress(String host, int port, String username, String password, String alias) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.alias = alias;
    }
    
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getAlias() {return alias; }
    
    public static final AuperServerAddress AUPER_DOTCOM_OLD = new AuperServerAddress(
            "67.68.228.98",
            5207,
            "user1",
            "1234",
            "Auper.com OLD");
    public static final AuperServerAddress AUPER_DOTCOM = new AuperServerAddress(
            "auper.com",
            5207,
            "user1",
            "1234",
            "Auper.com");
    public static final AuperServerAddress KITTY = new AuperServerAddress(
            "24.97.121.67",
            5207,
            "patd",
            "1234",
            "Kitty Hoynes");   
   public static final AuperServerAddress DEBUG = new AuperServerAddress(
            "24.39.253.162",
            5107,
            "patd",
            "1234",
            "Kitty Hoynes Debug"); 
                
}
