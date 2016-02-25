package net.terakeet.soapware.handlers.auper;

import java.io.*;
import javax.comm.*;
import java.util.*;
import java.lang.*;



public class rs232c {
    static CommPortIdentifier	portId;
    static Enumeration		portList;
    static volatile  boolean done=false;
    static String recieveddata="";
    SerialPort			serialPort;
    SerialPort			serialport;
    String 			description;
    PrintWriter			myWriter;
    BufferedReader		myReader;
    InputStream			inputStream;
    OutputStream		outputStream;
    static  int readingNumber;
    static String minutes="";
    static String hours="";
    
    public rs232c() {
    }
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting daemon...");
        rs232c neteon = new rs232c();
        TreeSet<String> hoursLista=new TreeSet<String>();
        TreeSet<String> hoursListb=new TreeSet<String>();
        neteon.doTask();
         /* for(;;) {
            Thread.sleep(10000);
            minutes=new Date().toString().substring(14,16);
            hours=new Date().toString().substring(11,13);
            
            
            if((minutes.equals("30")&&!hoursListb.contains(hours)   )
            ||
                    (minutes.equals("00") &&!hoursLista.contains(hours)))
                
                
            {
                readingNumber++;
                if(minutes.equals("00"))
                    hoursLista.add(hours);
                else
                    hoursListb.add(hours);
                
                System.out.println("Taking Reading number "+readingNumber);
                neteon.doTask();
            }
        }*/
	
	
    }
    
    
    public void openCommPort(String devPortName) {
        portList = CommPortIdentifier.getPortIdentifiers();
        while (portList.hasMoreElements()){
            portId = (CommPortIdentifier) portList.nextElement();
            System.out.println(portId.getName());
            if (portId.getPortType() == CommPortIdentifier.PORT_SERIAL){
                if (portId.getName().equals(devPortName)){
                    break;
                }
            }
        }
        
        try{
            serialPort = (SerialPort) portId.open("rs232c", 3000);
            System.out.println(devPortName + " Port opened: " + serialPort);
        } catch (PortInUseException e){
            System.out.println("GOT a portinuseexception");
        }
        try{
            serialPort.setSerialPortParams(	9600,
                    SerialPort.DATABITS_8,
                    SerialPort.STOPBITS_1,
                    SerialPort.PARITY_NONE);
            
            serialPort.setFlowControlMode(SerialPort.FLOWCONTROL_NONE);
        } catch (UnsupportedCommOperationException e) {}
    }
    
    public SerialPort getSerialPort(){
        return serialPort;
    }
    
    public void doTask() {
        String scannedInput = "";
        try{
            openCommPort("COM2");
            serialport = getSerialPort();
            try{
                
                outputStream= serialport.getOutputStream();
                byte[] commandBytes={0,-2,0,-2,0,-1,0};
                //{/0x0,0x0,0xfe,0x0,0x0,0xfe,0x0,0x0,0xff,0x0};
                outputStream.write(commandBytes);
                System.out.println(commandBytes.toString() + " has been sent" );
                
                SimpleRead sr = new SimpleRead(getSerialPort());
//sit and wait
                while(!done) {}
// we never come back to this program. we just call the decode method.
                
            }catch(Exception e){
                e.printStackTrace();
                description = "read port error";
                outputMessage(description, "Error.txt", "recieveddata = myReader.readLine()", e.toString());
                return ;
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
        return "";
    }
    
    
    
    
    
}
