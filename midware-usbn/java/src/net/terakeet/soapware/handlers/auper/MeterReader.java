/*
 * SimpleWrite.java
 *
 * Created on September 9, 2005, 11:24 AM
 *
 */

package net.terakeet.soapware.handlers.auper;

import java.util.*;
import javax.comm.*;
import java.io.*;

/** SimpleWrite
 *
 */
public class MeterReader extends TimerTask implements SerialPortEventListener { //, CommPortOwnershipListener {
    
    private static final int ACQUIRE_TIMEOUT = 4 * 1000;
    private static final int REPLY_TIMEOUT = 40 * 1000;
    
    private volatile boolean    aborted;
    private Enumeration         portList;
    private CommPortIdentifier  portId;
    private SerialPort          serialPort;
    private String              portName;
    
    private InputStream         in;
    private static int systemNum = 1;
    private static Object systemLock = new Object();
    
    public MeterReader(String portToUse) {
        in = null;
        checkAbort(-1);
        portName = "not found";
        portList = CommPortIdentifier.getPortIdentifiers();
        while (portList.hasMoreElements()){
            portId = (CommPortIdentifier) portList.nextElement();
            System.out.println(portId.getName());
            if (portId.getPortType() == CommPortIdentifier.PORT_SERIAL){
                if (portId.getName().equals(portToUse)){
                    portName = portToUse;
                    break;
                }
            }
        }
    }
    
    public MeterReader() {
        this("COM3");
    }
    
    private void cleanup() {
        if (checkAbort(1)) {
            System.out.println("cleanup detected abort");
        } 
        System.out.println("Cleaning up...");
        if (serialPort != null) {
            try {
                serialPort.removeEventListener();
            } catch (Exception e) {}
            try {
                serialPort.close();
            } catch (Exception e) {}
//            try {
//                CommPortIdentifier.getPortIdentifier(serialPort).removePortOwnershipListener(this);
//            } catch (Exception e) {}
            try {
            in.close();
            } catch (Exception e) {}
        }
        
        serialPort = null;
        in = null;
    }
    
    private synchronized boolean checkAbort(int i) {
        boolean result = aborted;
        if (i < 0) {
            aborted = false;
        } else if ( i > 0) {
            aborted = true;
        }
        return result;
    }
    
    private int getSystem() {
        synchronized(systemLock) {
            return systemNum;
        }
    }
    
    private void flipSystem() {
        synchronized(systemLock) {
            if (systemNum == 1) {
                systemNum = 0;
            } else {
                systemNum = 1;
            }
        }
    }
    
    private boolean openCommPort() {
        boolean result = false;
        if (!checkAbort(0)) {
            try{
                serialPort = (SerialPort) portId.open("rs232c", ACQUIRE_TIMEOUT);
                System.out.println(portName + " Port opened: " + serialPort);
                serialPort.setSerialPortParams(	9600,
                        SerialPort.DATABITS_8,
                        SerialPort.STOPBITS_1,
                        SerialPort.PARITY_NONE);
                serialPort.setFlowControlMode(SerialPort.FLOWCONTROL_NONE);
                // listen for requests to relinquish the port
//                try {
//                    CommPortIdentifier.getPortIdentifier(serialPort).addPortOwnershipListener(this);
//                } catch (Exception e) {
//                    System.out.println("Exception while adding PortOwnershipListener: "+e.toString());
//                }
                result = true;
            } catch (PortInUseException e){
                System.out.println("Unable to acquire comm port after "+ACQUIRE_TIMEOUT+" millis: "+e.toString());
            } catch (UnsupportedCommOperationException e) {
                System.out.println("Caught in openCommPort: "+e.toString());
            }
        } else {
            System.out.println("openCommPort detected the abort flag");
        }
        
        return result;
    }
    
    public void run() {
        OutputStream out = null;
        checkAbort(-1);
        flipSystem();
        try{
            java.util.Calendar cal = new java.util.GregorianCalendar();
            System.out.println(cal.getTime().toString()+": System "+getSystem()+", opening comm port");
            if (openCommPort()) {
                System.out.println("port open");
                //set up to receive
                if (!checkAbort(0)) {
                    try {
                        serialPort.addEventListener(this);
                    } catch (TooManyListenersException e) {
                        System.out.println("Too many listeners on this port");
                    }
                    serialPort.notifyOnDataAvailable(true);
                    in = serialPort.getInputStream();
                    out = serialPort.getOutputStream();
                    
                    // old way: byte[] commandBytes={0,-2,0,-2,0,-1,0};
                    //          {/0x0,0x0,0xfe,0x0,0x0,0xfe,0x0,0x0,0xff,0x0};
                    // test way:
                    String systemCommand;
                    int currentSystem = getSystem();
                    if (currentSystem == 1) {
                        systemCommand = "01FE00FFFF00FF00";
                    } else if (currentSystem == 0) {
                        systemCommand = "00FE00FE00FF00";
                    } else {
                        throw new RuntimeException("Invalid System ID: "+currentSystem);
                    }
                    byte[] commandBytes = Util.convertHexStringtoByteArray(systemCommand);
                    
                    out.write(commandBytes);
                    System.out.println(commandBytes.toString() + " has been sent" );
                    
                    //sit and wait
                    Thread.sleep(REPLY_TIMEOUT);
                    
                } else {
                    System.out.println("Detected abort, and skipped write block");
                }
                
            } else {
                System.out.println("Unopen to open comm port");
            }
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            cleanup();
        }
        System.out.println("run done");
        return;
    }
    
    
    public void serialEvent(SerialPortEvent event) {
        if (!checkAbort(0)) {
            switch(event.getEventType()) {
                case SerialPortEvent.BI:
                case SerialPortEvent.OE:
                case SerialPortEvent.FE:
                case SerialPortEvent.PE:
                case SerialPortEvent.CD:
                case SerialPortEvent.CTS:
                case SerialPortEvent.DSR:
                case SerialPortEvent.RI:
                case SerialPortEvent.OUTPUT_BUFFER_EMPTY:
                    break;
                case SerialPortEvent.DATA_AVAILABLE:
                    byte[] readBuffer = new byte[300];
                    
                    try {
                        int numBytes = 0;
                        while (in.available() > 0) {
                            numBytes = in.read(readBuffer);
                        }
                        //System.out.print(new String(readBuffer));
                        //rs232c.done=true;
                        //close the serial port
                        serialPort.removeEventListener();
                        serialPort.close();
                        System.out.println("comm port closed naturally");
// Paste Decode and MySQL methods
                        Vector<Character> raw = new Vector<Character>(300);
                        for(int i=0; i<numBytes; i++) {
                            raw.add(Util.charIt(readBuffer[i]));
                        }
                        //System.out.print("Attaching raw response: {"+Util.toHexString(raw)+"}");
                        Command00 command = new Command00();
                        boolean success = command.setResponse(raw);
                        if (success) {
                            System.out.println("attached OK");
                            Database mysql = new Database();
                            Map<Integer,Integer> lineMap = new HashMap<Integer,Integer>();
                            lineMap.put(new Integer(0),new Integer(3));
                            lineMap.put(new Integer(1),new Integer(4));
                            mysql.addReadings(command,mysql.kittyHoynesMap(getSystem()));
                        } else {
                            System.out.println("attach FAILED");
                        }
                        cleanup();
                        
                    } catch (IOException e) {}
                    break;
            }
        } else {
            System.out.println("serialEventAborted");
        }
    }
    
    
//    public void ownershipChange(int type) {
//        System.out.println("Ownership change event: "+type);
//        if (type == this.PORT_OWNERSHIP_REQUESTED) {
//                System.out.println("Relinquishing Ownership");
//                cleanup();
//        }
//    }
    
    public void finalize() {
        cleanup();
    }
    
    public static void main(String[] args) {
        MeterReader m = new MeterReader();
        m.run();
        if (m.checkAbort(0)) {
            System.out.println("MeterReader main method finished, abort flag detected");
        } else {
            System.out.println("MeterReader main method finished, NO abort detected");
        }
        m = null;
        
    }
    
}
