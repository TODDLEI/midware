/*
 * AuperConnection.java
 *
 * Created on June 30, 2005, 12:29 PM
 */

package net.terakeet.soapware.handlers.auper;

import java.net.*;
import java.io.*;
import java.util.Vector;
import java.lang.StringBuffer;

/**  Establishes a connection to an auper server to send commands and receive responses.
 *
 * @author Ryan Garver
 */
public class AuperConnection {
    
    private Socket sock;
    private PrintWriter out;
    private BufferedReader in;
    
    private static final int timeout = 15 * 1000; // 5 second timeout

    private static void debugPrint(String s) {
        Util.debugPrint(s);
    }
    
    public AuperConnection() throws AuperException {
        this(AuperServerAddress.KITTY);
    }
    
    /** Connects to an auper server and establishes a communication object.
     *  At this time, the host/port/user/pass are all hardcoded within this class.
     *  This constructor will log-in to the server and the object will be ready to
     *  process commands.
     */
    public AuperConnection(AuperServerAddress server) throws AuperException{
             
        try {
            
            // Connect
            sock = new Socket(server.getHost(),server.getPort());
            sock.setSoTimeout(timeout);
            out = new PrintWriter(sock.getOutputStream());
            in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            debugPrint("Connected");
            char[] message = new char[6];
            int bs = in.read(message,0,5);
            String response = new String(message);

            // Log in
            debugPrint("Received Message ("+bs+ ") :{"+response+"}");
            String credentials = server.getUsername() + '\u0000' + server.getPassword() + '\u0000';
            debugPrint("Sending {"+credentials+"} "+credentials.length()+" chars");
            out.print(credentials);
            out.flush();
            bs = in.read(message,0,6);
            response = new String(message);
            debugPrint("Received Message ("+bs+ ") :{"+response+"}");
            if (message[0] == 'D') {
                debugPrint("Login DENIED");
                try {sock.close();} catch (Exception e) {}
                throw new AuperException("Login to Auper Server denied for user "+server.getUsername());
            }        
            
        } catch (UnknownHostException uhe) {
            debugPrint("Unknown Host Exception");
            try {sock.close();} catch (Exception e) {}
            throw new AuperException("Couldn't look up host "+server.getHost());
        } catch (IOException ioe) {       
            debugPrint("IO Exception");
            try {
                out.close();
                in.close();
            } catch (Exception e4) {}
            try {sock.close();} catch (Exception e5) {}
            throw new AuperException("Auper Connection Timed out ("+timeout+" millis)");
        }
    }
    
    
    /**  Read an Auper response.  This is an unlimited number of bytes terminated 
     *  by the sequence xFF x00
     *
     *  @param in the stream from the Auper socket
     *  @return the complete message including the terminating characters
     *  @throws IOException if the socket times out
     */
    private static Vector<Character> readResponse(BufferedReader in) throws IOException {        
        int chr = 0;
        int lastChr = 0;
        boolean done = false;
        Vector<Character> result = new Vector<Character>();
        debugPrint("readResponse - Began reading");
        // Read bytes until FF 00 is found
        while (!done) {
            chr = in.read();
            if (chr == 0x00 && lastChr == 0xFF) {
                done = true;
                debugPrint("readResponse - Escape sequence found");
            }
            result.add(Util.charIt(chr));
            lastChr = chr;
        }
        return result;
    }
    
    /**  Sends an Auper command to the server, and attaches a response to the command.
     *  @param command The command to send.  The response will be attached to this object
     *  @return true if the response is properly formed and accepted by the command object,
     *      false otherwise.  Note that if a legal response comes back to indicate that the 
     *      command was not accepted, this method will still return true.
     */
    public boolean sendCommand(AuperCommand command) throws AuperException{
        debugPrint("Sending Command "+Util.toHex(command.getCommandNumber()));
        
        if (sock.isConnected()) {
            String commandString = command.transmissionForm();
                    
            try {
                debugPrint("Sending Command {"+commandString+"}");
                out.print(commandString);
                out.flush();
                Vector<Character> response = readResponse(in);
                debugPrint("Response {"+Util.toHexString(response)+"}");
                return command.setResponse(response);
            } catch (Exception e) {
                debugPrint("Exception in sendCommmand "+e.toString());
                try {sock.close();} catch (Exception e2) {}
                try {in.close();} catch (Exception e2) {}
                try {out.close();} catch (Exception e2) {}
                throw new AuperException("Exception while sending command: "+e.getMessage());
            }
        } else {
            debugPrint("Socket is not connected!");           
        }
        return false;
    }
    
    /** Close the connection and clean up.
     */
    public void close() {
        debugPrint("Closing Connections");
        try {in.close();} catch (Exception e) {}
        try {out.close();} catch (Exception e) {}
        try {sock.close();} catch (Exception e) {}
    }
    

    
}
