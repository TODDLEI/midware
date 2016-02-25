/*
 * HandlerResponse.java
 *
 * Created on August 6, 2004, 12:55 PM
 */

package net.terakeet.soapware;

import org.apache.log4j.Logger;
import org.dom4j.Element;

/**
 * Type-safe enumeration of response codes with a method to add them to an
 * <code>Element</code>.  The element name used for the response code is
 * "responseCode" and "responseMsg" holds an associated message, generally with
 * appropriate error information.  The method <code>addResponse</code> puts
 * these two elements in the specified element.
 * @author  aastle
 */
public class HandlerResponse {
    String rc;

    private HandlerResponse(String rc) {
        this.rc = rc;
    }

    public String toString() {
        return rc;
    }

    /* Message for a successful SOAP exchange. */
    public static final String MSG_SUCCESS = "OK";
    
    /* Return code for a successful request. */
    public static final HandlerResponse SUCCESS = new HandlerResponse("SUCCESS");
    /* Return code for when an internal error occured. */
    public static final HandlerResponse ERROR = new HandlerResponse("ERROR");
    /* Return code for a failure response is returned. The meaning is specified
     * in the documentation for each SOAP message.
     */
    public static final HandlerResponse FAILURE_1 = new HandlerResponse("FAILURE_1");
    /* Return code for a failure response is returned. The meaning is specified
     * in the documentation for each SOAP message.
     */
    public static final HandlerResponse FAILURE_2 = new HandlerResponse("FAILURE_2");
    /* Return code for a failure response is returned. The meaning is specified
     * in the documentation for each SOAP message.
     */
    public static final HandlerResponse FAILURE_3 = new HandlerResponse("FAILURE_3");
    /* Return code for a failure response is returned. The meaning is specified
     * in the documentation for each SOAP message.
     */
    public static final HandlerResponse FAILURE_4 = new HandlerResponse("FAILURE_4");

    /**
     * Adds response elements to a parent element.
     * @param toAppend the parent element to add the two response elements to
     * @param responseCode the type-safe enumerated response code in
     *   &lt;responseCode/&gt;
     * @param responseMsg a message providing information about the response
     *   in &lt;responseMsg/&gt;
     */
    public static void addResponse(Element toAppend,
        HandlerResponse responseCode, String responseMsg) {
        if (null == responseMsg) {
            responseMsg = "";
        }
        if (null == responseCode) {
            toAppend.addElement("responseCode").addText("NO RESPONSE CODE");
        } else {
            toAppend.addElement("responseCode").addText(responseCode.toString());
        }
        toAppend.addElement("responseMsg").addText(responseMsg);
    }
    
    /**
     * Adds success response elements to a parent element with
     * <code>SUCCESS</code> for &lt;responseCode/&gt;
     * and <code>MSG_SUCCESS</code> for &lt;responseMsg/&gt;
     * @param toAppend the parent element to add the two response elements to
     */
    public static void addSuccessResponse(Element toAppend) {
        addResponse(toAppend, SUCCESS, MSG_SUCCESS);
    }
    
    /**
     * Adds error response elements to a parent element with
     * <code>ERROR</code> for &lt;responseCode/&gt;
     * @param toAppend the parent element to add the two response elements to
     * @param errorMsg the error msg to return in &lt;responseMsg/&gt;
     */
    public static void addErrorResponse(Element toAppend, String errorMsg) {
        addResponse(toAppend, ERROR, errorMsg);
    }
    
    /**
     * Adds error response elements to a parent element with
     * <code>ERROR</code> for &lt;responseCode/&gt;
     * @param toAppend the parent element to add the two response elements to
     * @param error the throwable that will be used to generate the error
     *   message in &lt;responseMsg/&gt;
     */
    public static void addErrorResponse(Element toAppend, Throwable error) {
        if (null != error.getMessage()) {
            addErrorResponse(toAppend, error.getMessage());
        } else {
            addErrorResponse(toAppend, error.toString());
        }
    }
}
