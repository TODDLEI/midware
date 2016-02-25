/*
 * ls.java
 *
 * Created on August 31, 2004, 2:35 PM
 */

package net.terakeet.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.log4j.Logger;


/**
 *
 * @author  aastle
 */
public class PdfLatexProcess {
    
    static Logger logger = Logger.getLogger(PdfLatexProcess.class.getName());
    
    public PdfLatexProcess(String texName) {
        String[] cmd = new String[3];
        cmd[0] = "/bin/sh";
        cmd[1] = "-c";
        cmd[2] = "pdflatex " + texName;
        
        try {
            final Process pr = Runtime.getRuntime().exec(cmd);
            
            new Thread(new Runnable() {
                public void run() {
                    StringBuffer sb = new StringBuffer();
                    try {
                        BufferedReader br_in = new BufferedReader(
                            new InputStreamReader(pr.getInputStream()));
                        String buff = null;
                        while ((buff = br_in.readLine()) != null) {
                            sb.append(buff + "\n");
                            try {Thread.sleep(1000); } catch(Exception e) {}
                        }
                        br_in.close();
                        logger.debug(sb.toString());
                    } catch (IOException ioe) {
                        logger.error(ioe.toString());
                    }
                }
            }).start();

            new Thread(new Runnable() {
                public void run() {
                    StringBuffer sb = new StringBuffer();
                    try {
                        BufferedReader br_err = new BufferedReader(
                            new InputStreamReader(pr.getErrorStream()));
                        String buff = null;
                        while ((buff = br_err.readLine()) != null) {
                            sb.append(buff + "\n");
                            try {Thread.sleep(1000); } catch(Exception e) {}
                        }
                        br_err.close();
                        logger.error(sb.toString());
                    } catch (IOException ioe) {
                        logger.error(ioe.toString());
                    }
                }
            }).start();
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
