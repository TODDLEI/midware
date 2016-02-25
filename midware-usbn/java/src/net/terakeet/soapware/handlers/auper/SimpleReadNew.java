package net.terakeet.soapware.handlers.auper;

import java.io.*;
import java.util.*;
import javax.comm.*;

public class SimpleReadNew implements Runnable, SerialPortEventListener, CommPortOwnershipListener {
    static CommPortIdentifier portId;
    static Enumeration portList;
    static int totalReadings = 0;
    static final int MAX_READINGS = 3;
    
    InputStream inputStream;
    SerialPort serialPort;
    Thread readThread;
    String msg;
    public static void main(String[] args) {
        portList = CommPortIdentifier.getPortIdentifiers();
        
        while (portList.hasMoreElements()) {
            portId = (CommPortIdentifier) portList.nextElement();
            if (portId.getPortType() == CommPortIdentifier.PORT_SERIAL) {
                if (portId.getName().equals("COM1")) {
                    //  if (portId.getName().equals("/dev/term/a")) {
                    SimpleReadNew reader = new SimpleReadNew();
                }
            }
        }
    }
    
    
    public SimpleReadNew(SerialPort sp) {
        
        //   CommPortIdentifier portId=pId;
        //   System.out.println(portId.getName());
        serialPort=sp;
        
    /*    try {
            serialPort = (SerialPort) portId.open("SimpleReadApp", 2000);
        } catch (PortInUseException e) {e.printStackTrace();}
     
     */
        try {
            inputStream = serialPort.getInputStream();
        } catch (IOException e) {}
        try {
            serialPort.addEventListener(this);
        } catch (TooManyListenersException e) {}
        serialPort.notifyOnDataAvailable(true);
        try {
            serialPort.setSerialPortParams(9600,
                    SerialPort.DATABITS_8,
                    SerialPort.STOPBITS_1,
                    SerialPort.PARITY_NONE);
        } catch (UnsupportedCommOperationException e) {}
        readThread = new Thread(this);
        readThread.start();
        msg="";
    }
    
    public SimpleReadNew() {
        try {
            serialPort = (SerialPort) portId.open("SimpleReadApp", 2000);
        } catch (PortInUseException e) {}
        try {
            inputStream = serialPort.getInputStream();
        } catch (IOException e) {}
        try {
            serialPort.addEventListener(this);
        } catch (TooManyListenersException e) {}
        serialPort.notifyOnDataAvailable(true);
        try {
            serialPort.setSerialPortParams(9600,
                    SerialPort.DATABITS_8,
                    SerialPort.STOPBITS_1,
                    SerialPort.PARITY_NONE);
        } catch (UnsupportedCommOperationException e) {}
        readThread = new Thread(this);
        readThread.start();
    }
    
    public void run() {
        System.out.println("Running SimpleRead");
        try { Thread.sleep(20 * 1000); } catch (InterruptedException e) {}
        System.out.print("SimpleRead run() loop, closing ports");
        try {
            
        } catch (Exception e) { System.out.println ("Exception while closing comm port in SimpleReadNew.run(): "
                    +e.toString());
        }
    }
    
    public void serialEvent(SerialPortEvent event) {
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
                    while (inputStream.available() > 0) {
                        numBytes = inputStream.read(readBuffer);
                    }
                    System.out.print(new String(readBuffer));
                    //rs232c.done=true;
                    //close the serial port
                    serialPort.close();
// Paste Decode and MySQL methods
                    Vector<Character> raw = new Vector<Character>(300);
                    for(int i=0; i<numBytes; i++) {
                        raw.add(Util.charIt(readBuffer[i]));
                    }
                    System.out.print("Attaching raw response: {"+Util.toHexString(raw)+"}");
                    Command00 command = new Command00();
                    boolean success = command.setResponse(raw);
                    if (success) {
                        System.out.println("attached OK");
                        Database mysql = new Database();
                        Map<Integer,Integer> lineMap = new HashMap<Integer,Integer>();
                        lineMap.put(new Integer(0),new Integer(3));
                        lineMap.put(new Integer(1),new Integer(4));
                        mysql.addReadings(command,mysql.kittyHoynesMap(0));
                    } else {
                        System.out.println("attach FAILED");
                    }
                    
                    
                    // Problem Line 2. to get out of sleep
                    //	System.exit(1);
                    
                    
                } catch (IOException e) {}
                break;
        }
    }
    
        public void ownershipChange(int type) {
        
    }
}
