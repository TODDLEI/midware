/*
 * Command65.java
 *
 * Created on June 30, 2005, 1:36 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.lang.StringBuffer;

/** From the DraftManager Specification for a 0x65 command:
 * <i>"Request the Inv. Server to send back its product inventory to the Client. "</i>
 *
 * @author Ryan Garver
 */
public class Command65 extends AuperCommand {
    
    private String xmlString;
    
    /** Creates a new instance of Command65 */
    public Command65() {
        super();
        command.add(Util.charIt(0x65));
        
        xmlString = "";
        responseAttached = false;
        responseResult = false;
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
        } else {
            if (checksumAndPop(response)) {
                responseAttached = true;
                responseResult = (response.get(4).charValue() == (char)0x01 ? true : false);
                if (responseResult) {
                    //remove the header
                    for (int i=0; i<9; i++) {
                        response.remove(0);
                    }
                    xmlString = Util.charsToString(response);       
                }
                return true;
            } else {
                return false;
            }
        }
    }
    
    /** Returns the product inventory in a string containing XML.  A response must be attached
     *  to this command to call this method. If the command failed, the string will be null.  
     *  Format:
     *  <?xml version="1.0" encoding="ISO-8859-1" ?>
     *  <InventoryReport>
     *    <Product index="00">
     *      <ProductName>Sample Beer</ProductName>
     *      <QtyInStock>1.50</QtyInStock>
     *      <MinimumQty>1.00<MinimumQty>
     *    </Product>
     *    <Product index="01">
     *      ...
     *    </Product>
     *  </InventoryReport>
     *
     */
    public String getXmlString() {
        if (!responseAttached) {
            throw new IllegalStateException("Response has not been attached");
        }
        return xmlString;
    }
    
}
