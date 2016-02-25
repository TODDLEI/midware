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
public class SimpleWrite extends TimerTask {
    
    private Enumeration         portList;
    private CommPortIdentifier  portId;
    private SerialPort          serialPort;
    private String              portName;
    
    /** Creates a new instance of SimpleWrite */
    public SimpleWrite(String portToUse) {
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
    
    public SimpleWrite() {
        this("COM3");
    }
    
    
    private boolean openCommPort(boolean loop) {
        boolean result = false;
        try{
            serialPort = (SerialPort) portId.open("rs232c", 3000);
            System.out.println(portName + " Port opened: " + serialPort);       
            serialPort.setSerialPortParams(	9600,
                    SerialPort.DATABITS_8,
                    SerialPort.STOPBITS_1,
                    SerialPort.PARITY_NONE);            
            serialPort.setFlowControlMode(SerialPort.FLOWCONTROL_NONE);
            result = true;
        } catch (PortInUseException e){
            System.out.println("Caught a PortInUseException "+e.toString());
            if (loop) {
                try { 
                    serialPort.close();
                    Thread.sleep(500);
                    return openCommPort(false);
                } catch (Exception e2) { }
            }
        } catch (UnsupportedCommOperationException e) {
            System.out.println("Caught in openCommPort: "+e.toString());
        }
        return result;
    }
    
    private boolean openCommPort() {
        return openCommPort(true);
    }
    
    public void run() {
        System.out.println("running SimpleWrite");
        OutputStream out = null;
        try{
            if (openCommPort()) {
                out = serialPort.getOutputStream();
                byte[] commandBytes={0,-2,0,-2,0,-1,0};
                //{/0x0,0x0,0xfe,0x0,0x0,0xfe,0x0,0x0,0xff,0x0};
                out.write(commandBytes);
                System.out.println(commandBytes.toString() + " has been sent" );

                SimpleReadNew sr = new SimpleReadNew(serialPort);
                
                
            //sit and wait
                // cancel(); maybe? or wait?
            // we never come back to this program. we just call the decode method.

            } else {
                System.out.println("Unopen to open comm port");
            }
        } catch(Exception e){
            e.printStackTrace();
            String description = "GOT a portinuseexception";
            outputMessage(description, "Error.txt", "GOT a portinuseexception", e.toString());
            return ;
        }
    }
    
    
    
    private void outputMessage(String s, String fileName, String method, String Ex){
        try{
            FileOutputStream log;
            log=new FileOutputStream(fileName,true);
            PrintStream ps=new PrintStream(log);
            ps.println("-------------------------Error Log----------------------------");
            ps.println("CLASS : Neteon_Form");
            ps.println("METHOD : "+method);
            ps.println("EXCEPTION : "+Ex);
            ps.println("DESCRIPTION : "+s);
            ps.println("TIME : "+(new Date().toString().substring(11,19)));
            ps.println("--------------------------------------------------------------");
            ps.println();
        } catch(Exception e){}
    }
    
    private String getComPortName(){
        return portName;
    }
    
}
