import java.io.*;
import javax.comm.*;
import java.util.*;
import java.lang.*;

public class rs232c {
    static CommPortIdentifier	portId;
    static Enumeration		portList;
    static volatile  boolean done=false;
    SerialPort			serialPort;
    OutputStream		outputStream;
    
    public rs232c() {
    }
    
    public static void main(String[] args) {
        rs232c neteon = new rs232c();
        neteon.doTask();
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
        }
        catch (PortInUseException e){
            System.out.println("GOT a portinuseexception");
        }
        try{
            serialPort.setSerialPortParams(	9600,
            SerialPort.DATABITS_8,
            SerialPort.STOPBITS_2,
            SerialPort.PARITY_NONE);
            
            serialPort.setFlowControlMode(SerialPort.FLOWCONTROL_NONE);
        }
        catch (UnsupportedCommOperationException e) {}
    }
    
    public SerialPort getSerialPort(){
        return serialPort;
    }
    
    public void doTask() {
        try{
            openCommPort("COM2");
            serialPort = getSerialPort();
            try{
                outputStream = serialPort.getOutputStream();
                byte[] commandBytes={0,-2,0,-2,0,-1,0};
                outputStream.write(commandBytes);
                System.out.println(commandBytes.toString() + " has been sent" );
                
                SimpleRead sr = new SimpleRead(getSerialPort());
                //we never come back to this program.
                while(!done) {}
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
}
