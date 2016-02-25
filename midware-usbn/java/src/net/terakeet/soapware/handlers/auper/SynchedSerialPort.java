/*
 * SynchedSerialPort.java
 *
 * Created on September 15, 2005, 10:10 AM
 *
 */

package net.terakeet.soapware.handlers.auper;

import javax.comm.*;
import java.io.*;

/** SynchedSerialPort
 *
 */
public class SynchedSerialPort {
    
    private boolean gone;
    private Object lock;
    private String name;
    private SerialPort sp;
    private PortManager manager;
    
    /** Creates a new instance of SynchedSerialPort */
    public SynchedSerialPort(String name, SerialPort port, PortManager manager) {
        this.name = name;
        this.sp = port;
        gone = false;
        this.manager = manager;
    }
    
    public InputStream getInputStream() throws IOException, PortUnavailableException {
        synchronized (lock) {
            if (! isGone()) {
                return sp.getInputStream();
            } else {
                throw new PortUnavailableException("Port "+name+" is gone.");
            }
        }
    }
    
    public OutputStream getOutputSteam() throws IOException, PortUnavailableException{
        synchronized (lock) {
            if (! isGone()) {
                return sp.getOutputStream();
            } else {
                throw new PortUnavailableException("Port "+name+" is gone.");
            }
        }
    }
    
    public boolean isGone() {
        return gone;
    }
    
    public void makePortUnavailable() {
         synchronized (lock) {
             gone = true;
         }
    }
    
    public void addDataListener(SerialPortEventListener lstn)
    throws java.util.TooManyListenersException, PortUnavailableException {
        synchronized (lock) {
            if (! isGone()) {
                sp.addEventListener(lstn);
                sp.notifyOnDataAvailable(true);
            }
        }
    }
    
    public void setSerialPortParams(int baudrate, int dataBits, int stopBits, int parity)
    throws UnsupportedCommOperationException{
        synchronized (lock) {
            if (! isGone()) {
                sp.setSerialPortParams(baudrate, dataBits, stopBits, parity);
            }
        }
    }
    
    public void close() {
        synchronized (lock) {
            
        }
    }
}
