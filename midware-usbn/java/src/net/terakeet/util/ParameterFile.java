/**
 * ParameterFile.java
 *
 * @author Sundar Ravindran
 * @version $Id: ParameterFile.java,v 1.25 2016/01/05 15:59:14 sravindran Exp $
 */
package net.terakeet.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import net.terakeet.soapware.HandlerUtils;

public class ParameterFile  {
    private String paramPath                = ".", loadFrom = ".", fileName = "000000000000", plainContent = null;
    private static MidwareLogger logger     = new MidwareLogger("net.terakeet.soapware.handlers.SQLBOSSHandler");
    
    public ParameterFile (String fileName) throws Exception {

        this.paramPath                      = HandlerUtils.getSetting("bevBox.paramPath");
        if (null == paramPath) {
            this.paramPath                  = "/var/local/params";
        }

	if (fileName != null) {
	    this.fileName                   = fileName;
	}

        logger.debug("--Reading Parameter File--");
	try {
	    readFile();
	} catch (IOException ioe) {
	    throw new Exception("Problem loading parameter template: " + ioe.getMessage());
	}
    }

    public ParameterFile (String templateName, String fileName) throws Exception {
        this.loadFrom                       = HandlerUtils.getSetting("bevBox.templatePath");
	if (loadFrom == null) {
	    this.loadFrom                   = "./bevBox-templates";
	}

        this.paramPath                      = HandlerUtils.getSetting("bevBox.paramPath");
        if (null == paramPath) {
            this.paramPath                  = "/var/local/params";
        }

	if (fileName != null) {
	    this.fileName                   = fileName;
	}

        logger.debug("--Loading Parameter Template");
	try {
	    loadTemplate(templateName);
	} catch (IOException ioe) {
	    throw new Exception("Problem loading parameter template: " + ioe.getMessage());
	}
    }

    private void loadTemplate (String templateName) throws IOException {
	File plain                          = new File(loadFrom + File.separator + templateName + ".txt");
        //logger.debug(loadFrom + File.separator + templateName + ".txt");

	if (plain.canRead()) {
	    BufferedReader br               = new BufferedReader(new FileReader(plain));
	    StringBuffer sb                 = new StringBuffer();
	    String str;
	    while ((str = br.readLine()) != null) {
		sb.append(str);
		sb.append("\n");
	    }
	    br.close();
           this.plainContent                = sb.toString();
	}
    }

    public boolean doesExist() throws IOException {
	File plain                          = new File(paramPath + File.separator + fileName + ".params.txt");
        logger.debug(paramPath + File.separator + fileName + ".params.txt");
	if (plain.canRead()) {
            return true;
	} else {
            return false;
        }
    }

    private void readFile () throws IOException {
	File plain                          = new File(paramPath + File.separator + fileName + ".params.txt");
        //logger.debug(paramPath + File.separator + fileName + ".params.txt");
        
	if (plain.canRead()) {
	    BufferedReader br               = new BufferedReader(new FileReader(plain));
	    StringBuffer sb                 = new StringBuffer();
	    String str;
	    while ((str = br.readLine()) != null) {
		sb.append(str);
		sb.append("\n");
	    }
	    br.close();
           this.plainContent                = sb.toString();
	}
    }
    
    public String getLineCalibration (String line) throws Exception {

        String calibration                  = "600";
        if (! line.matches("^\\d*")) {
	    throw new Exception("Malformed field name for template replacement.");
	}
        int index                           = plainContent.indexOf("zonefour.sensorCalibration."+line) + ("zonefour.sensorCalibration."+line).length() + 3;
        //logger.debug(plainContent + ", startIndex: " + index);
    	calibration                         = plainContent.substring(index, index + 3).replaceAll("\\n","");
        return calibration;
    }

    /**
     * Performs string substitution on a named "field" in a template.
     * <em>Note: any backslashes preceding a dollar sign in the
     * <code>value</code> parameter are removed.</em>
     * @param fieldName a "field name" to search for in the template;
     * this is a string of alphabetical characters enclosed in
     * double-'@' signs, for example "@@NAME@@".
     * @param value the value with which to replace all occurrences of
     * the named field in the template.
     */
    public void setField (String fieldName, String value) throws MailException {

        if (! fieldName.matches("^[a-zA-Z]\\w*")) {
	    throw new MailException("Malformed field name for template replacement.");
	}

	/* remove any backslashes before a dollar sign, then replace
	 * the whole shebang with a double backslash followed by an
	 * escaped dollar sign, which is eventually interpreted as a
	 * dollar sign.  so the end result is to leave the original
	 * pattern unmolested *EXCEPT* that backslashes before a
	 * dollar sign are removed. */
	String replaceMe                    = "(\\\\)*\\$";
      	String replaceWith                  = "\\\\\\$";
      	value                               = value.replaceAll(replaceMe, replaceWith);
    	plainContent                        = plainContent.replaceAll("@@"+fieldName+"@@", value);
    }

    public void setFieldValue (String fieldName, String value) throws Exception {
	int startIndex                      = plainContent.indexOf(fieldName);
    	int endIndex                        = startIndex + plainContent.substring(startIndex).indexOf("\n");
        String fieldValue                   = plainContent.substring(startIndex, endIndex);
    	plainContent                        = plainContent.replaceAll(fieldValue, fieldName + " = " + value);
    }

    public void store () throws Exception {
	File plain                          = new File(paramPath + File.separator + fileName + ".params.txt");
        logger.debug(paramPath + File.separator + fileName + ".params.txt");
	try {
            plain.createNewFile();
            BufferedWriter out              = new BufferedWriter(new FileWriter(plain));
            out.write(plainContent);
            out.close();
	} catch (Exception e) {
	    throw new Exception((Throwable)e);
	}
    }
}
