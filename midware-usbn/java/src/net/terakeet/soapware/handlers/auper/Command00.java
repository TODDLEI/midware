/*
 * Command65.java
 *
 * Created on Sept 7, 2005, 1:36 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.util.List;
import java.lang.StringBuffer;

/** From the DraftManager Specification for a 0x00 command:
 * <i>"Request the Inv. Server to send back its product inventory to the Client. "</i>
 *
 * @author Ryan Garver
 */
public class Command00 extends AuperCommand {
    
    private double[] reading;
    private double[] calibration;
    private Character systemStatus;
    private Character powerFailure;
    
    /** Creates a new instance of Command00 */
    public Command00() {
        super();
        command.add(Util.charIt(0x00));
        
        reading = new double[16];
        calibration = new double[16];
        responseAttached = false;
        responseResult = false;
        systemStatus = null;
        powerFailure = null;
    }
    
    /**  Attach a response message to this command.  The response must be in raw "stuffed"
     *  form with terminating characters and checksum bytes attached.  If the response is
     *  well formed, this method will return true.
     *  @param resp the response, must be stuffed with terminating characters and checksums intact
     *  @return true if the response is in the proper form with valid checksum, false otherwise
     */
    public boolean setResponse(Vector<Character> resp) {
        Vector<Character> response = unstuff(resp);
        // Util.debugPrint("Unstuffed response is: {"+Util.toHexString(response)+"}");
        if (response == null) {
            Util.debugPrint("Response is null");
            return false;
        } else {
            if (checksumAndPop(response)) {
                responseAttached = true;
                responseResult = (response.get(4).charValue() == (char)0x01 ? true : false);
                if (responseResult) {
                    try {
                        systemStatus = response.get(5);
                        powerFailure = response.get(6);
                        int index = 7;
                        //add the readings
                        for (int i=0; i<16; i++) {
                            List<Character> sub = response.subList(index,index+6);
                            // Util.debugPrint("Debug: Decoding Reading..."+Util.toHexString(sub));
                            reading[i] = Util.decodeBcd(sub,6);
                            index += 6;
                        }
                        //add the calibrations
                        for (int i=0; i<16; i++) {
                            List<Character> sub = response.subList(index,index+4);
                            // Util.debugPrint("Debug: Decoding Calibration..."+Util.toHexString(sub));
                            calibration[i] = Util.decodeBcd(sub,2);
                            index += 4;
                        }
                    } catch (Exception e) {
                        Util.debugPrint("Exception caught in setResponse: "+e.toString());
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }
    }
    
    
   /** Get the line reading for a particular index.
    *   This command must have a response attached to call this method.
    *   @param index must be 0-15
    *   @return the reading
    */
    public double getReading(int index) {
        assertResponseAttached();
        if (index < 0 || index >= reading.length) {
            throw new IllegalArgumentException("index must be 0-15");
        }
        return reading[index];
    }
    
    /** Get the line calibration reading for a particular index.
    *   This command must have a response attached to call this method.
    *   @param index must be 0-15
    *   @return the calibration
    */
    public double getCalibration(int index) {
        assertResponseAttached();
        if (index < 0 || index >= reading.length) {
            throw new IllegalArgumentException("index must be 0-15");
        }
        return calibration[index];
    }
    
    /** Get the power failure code for this box.
    *   This command must have a response attached to call this method.
    *   @return the code, a single unsigned byte (char).  Not really sure what this is...
    */
    public char getPowerFailure() {
        assertResponseAttached();
        return powerFailure.charValue();
    }
    
    /** Get the system status code for this box.
    *   This command must have a response attached to call this method.
    *   @return the code, a single unsigned byte (char).  Not really sure what this is...
    */
    public char getSystemStatus() {
        assertResponseAttached();
        return systemStatus.charValue();
    }
    
}
