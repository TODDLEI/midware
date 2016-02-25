/**
 * ApplicationSettings.java
 *
 * @author Ben Ransford
 * @version $Id: ApplicationSettings.java,v 1.1.1.1 2005/08/26 19:16:01 anonymous Exp $
 *
 * (c) Copyright 2003 Terakeet Corporation.
 */

package net.terakeet.util;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Handles <code>java.util.Properties</code>-style properties in a
 * slightly abstract way, the goal of which is to make it easy for an
 * individual part of an application to define, load, and store
 * settings that persist across various boundaries.
 */
public abstract class ApplicationSettings {
    private String propertiesFilename;
    private String propertiesDesc;
    protected Properties properties;
    private String errorMsg = null;

    /**
     * @param propertiesFilename the filename of the properties file
     * to load or store, excluding any path information,
     * e.g. <code>appPart.props</code>.
     * @param propertiesDesc an English justification of the existence
     * of the properties file.  This text is saved as a comment at the
     * top of the properties file.
     */
    protected ApplicationSettings (String propertiesFilename, String propertiesDesc) {
	this.propertiesFilename = propertiesFilename;
	this.propertiesDesc = propertiesDesc;
    }

    /**
     * @param defaultProps A default set of properties in which to set
     * default values.
     */
    protected abstract void setDefaults (Properties defaultProps);

    /**
     * Loads properties from a file in the user's home directory.  The
     * filename from which this method reads the properties is
     * determined by the <code>propertiesFilename</code> parameter to
     * the constructor.  If there is an error loading the properties
     * file (due to incorrect permissions on the file, perhaps), this
     * method sets an error message and causes this class's
     * <code>hasError()</code> condition to become true.  It is up to
     * the caller to check this, obviously, unless he or she doesn't
     * care.
     */
    protected void loadProperties () {
	this.properties = new Properties();
	setDefaults(properties);

	FileInputStream propsIn = null;
	try {
	    propsIn = new FileInputStream(this.propertiesFilename);
	    this.properties.load(propsIn);
	} catch (java.io.IOException e) {
	    setError("Error loading application preferences.");
	}

	importSettings();
    }

    /**
     * Saves properties to a file in the user's home directory.
     * @see #loadProperties()
     */
    protected void saveProperties () {
	exportSettings();

	FileOutputStream propsOut = null;
	try {
	    propsOut = new FileOutputStream(this.propertiesFilename);
	    this.properties.save(propsOut, propertiesDesc);
	} catch (java.io.IOException e) {
	    setError("Error saving application preferences.");
	}
    }

    /**
     * Copies properties from a <code>Properties</code> object into
     * various private fields (as determined by the class extending
     * this one.
     */
    protected abstract void importSettings ();

    /**
     * Copies various private fields (as determined by the class
     * extending this one) into a <code>Properties</code> object.
     */
    protected abstract void exportSettings ();

    /**
     * @param err the error message to be fetched later by an application
     * that checks <code>hasError()</code>.
     */
    private void setError (String err) {
	this.errorMsg = err;
    }

    /**
     * @return the implementing class' current error message (if any)
     * and readies it to contain another error.
     */
    public String getError () {
	String retval = this.errorMsg;
	this.errorMsg = null;
	return retval;
    }

    /**
     * @return <code>true</code> if the implementing class has no
     * error message to provide, <code>false</code> otherwise.
     */
    public boolean hasError () {
	return (errorMsg != null && !"".equals(errorMsg));
    }

    /**
     * @return the filename of the properties file.
     */
    public String getPropertiesFilename () {
	return this.propertiesFilename;
    }

}
