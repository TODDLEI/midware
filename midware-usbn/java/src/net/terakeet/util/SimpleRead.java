import java.io.*;

import java.util.*;

import javax.comm.*;



public class SimpleRead implements Runnable, SerialPortEventListener {

    static CommPortIdentifier portId;

    static Enumeration portList;



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

              //  if (portId.getName().equals("/dev/term/a")) { -- on solaris

                    //SimpleRead reader = new SimpleRead();

                }

            }

        }

    }



 public SimpleRead(SerialPort sp) {



                serialPort=sp;

//Since port is already open, just grab, dont open.

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

   

    }



    public void run() {

      try { 

            Thread.sleep(2000);

        } catch (InterruptedException e) {}

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

         	byte[] returnedBytes=new byte[300];

            try {

                while (inputStream.available() > 0) {

               int numRead = inputStream.read(returnedBytes);

                }

               rs232c.done=true;

		//  System.out.println("Received Data "+  (new String(readBuffer),1));

              System.out.println("Received Data "+ returnedBytes  );

                 System.exit(1);

       } catch (IOException e) {}

            break;

        }

    }

}

