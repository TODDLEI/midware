/*
 * DatabaseConnectionWrapper.java
 *
 * Created on September 1, 2005, 11:31 AM
 */

package net.terakeet.soapware;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 *
 * @author Administrator
 */
class DatabaseConnectionWrapper {
    
    private static Logger logger = Logger.getLogger(DatabaseConnectionWrapper.class.getName());
    
    /** Database connection url. */
    private String url;
    /** Database user name */
    private String user;
    /** Database password */
    private String password;
    /** Database connection property pairs */
    private Properties info;
    
    /**
     * Creates a new instance of DatabaseConnectionWrapper 
     */
    public DatabaseConnectionWrapper(String url, Properties info)
    {
        this.url = url;
        this.info = info;
    }

    /**
     * Creates a new instance of DatabaseConnectionWrapper 
     */
    public DatabaseConnectionWrapper(String url, String user, String password)
    {
        this.url = url;
        this.user = user;
        this.password = password;
    }
    
    /**
     * Creates a new instance of DatabaseConnectionWrapper 
     */
    public DatabaseConnectionWrapper(String url)
    {
        this(url, null);
    }
  
    /**
     * Gets a new connection to the database.
     */
    public Connection newConnection() throws HandlerException {
        Connection conn = null;
        try {
            logger.debug("New database connection");
            if (null != info) {
                //logger.debug("url, info:" + url + ", " + info.toString());
                conn = DriverManager.getConnection(url, info);
            } else if (null != user && null != password) {
                //logger.debug("url, user, password:" + url + ", " + user + ", " + password);
                conn = DriverManager.getConnection(url, user, password);
            } else {
                //logger.debug("url only:" + url);
                conn = DriverManager.getConnection(url);
            }
        } catch (SQLException sqle) {
            throw new HandlerException(sqle);
        }
        return conn;
    }
}
