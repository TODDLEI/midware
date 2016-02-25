/**
 * TimedError.java
 *
 * @author Alex Astle
 * @version $Id: TimedError.java,v 1.1 2008/04/30 15:30:43 aastle Exp $
 */
package net.terakeet.util;

import java.util.Date;

/**
 * Basic String, Date tuple for errors.
 */
public class TimedError {

    private String errorMessage;
    private Date errorTime;
    
    public TimedError(String message, Date time) {
        errorMessage = message;
        errorTime = time;
    }

    public TimedError(String message) {
        this(message, new Date());
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Date getErrorTime() {
        return errorTime;
    }
    
    @Override
    public boolean equals(Object obj) {
        return (obj instanceof TimedError) && errorMessage != null &&
                errorMessage.equals(((TimedError)obj).errorMessage);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + (errorMessage != null ? errorMessage.hashCode() : 0);
        return hash;
    }
}
