/*
 * Command64.java
 *
 * Created on June 30, 2005, 1:36 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.lang.StringBuffer;

/** From the DraftManager Specification for a 0x64 command:
 * <i>"Tell the Inv. Server to read all the Harpagon systems
 * connected to it and update the counters as well as its products inventory locally."</i>
 *
 * @author Ryan Garver
 */
public class Command64 extends AuperCommand {
    
    /** Creates a new instance of Command64 with a default sender and destination id */
    public Command64() {
        super();
        command.add(Util.charIt(0x64));
        
        responseResult = false;
        responseAttached = false;
    }
    
    /**  Attach a response message to this command.  The response must be in raw "stuffed"
     *  form with terminating characters and checksum bytes attached.  If the response is
     *  well formed, this method will return true.
     *  @param resp the response, must be stuffed with terminating characters and checksums intact
     *  @return true if the response is in the proper form with valid checksum, false otherwise
     */
    public boolean setResponse(Vector<Character> resp) {
        Vector<Character> response = unstuff(resp);
        
        if (response == null) {
            Util.debugPrint("Response is null");
            return false;
        } else if (response.size() != 9) {
            Util.debugPrint("Incorrect response size.  Wanted 9 bytes, got "+response.size());
            return false;
        } else {
            if (checksumAndPop(response)) {
                responseAttached = true;
                responseResult = (response.get(4).charValue() == (char)0x01 ? true : false);
                return true;               
            } else {
                return false;
            }
        }
    }
    

    
}
