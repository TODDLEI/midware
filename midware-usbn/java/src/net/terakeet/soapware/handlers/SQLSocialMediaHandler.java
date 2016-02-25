/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.terakeet.soapware.handlers;

/**
 *
 * @author suba
 */

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.Permission;
import net.terakeet.soapware.*;
import net.terakeet.soapware.security.*;
import net.terakeet.util.MidwareLogger;
import org.apache.log4j.Logger;
import org.dom4j.Element;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.text.*;
import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import javax.imageio.ImageIO;
import javax.net.ssl.HttpsURLConnection;
import net.terakeet.soapware.handlers.report.*;
import net.terakeet.util.*;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.util.URIUtil;



import twitter4j.IDs;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.auth.BasicAuthorization;
import twitter4j.auth.RequestToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.media.ImageUpload;
import twitter4j.media.ImageUploadFactory;
import twitter4j.media.MediaProvider;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

// URLShortner
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.api.client.googleapis.GoogleHeaders;
import com.google.api.client.googleapis.GoogleTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonHttpContent;
import com.google.api.client.json.JsonHttpParser;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.Key;
import java.net.MalformedURLException;


import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import static net.terakeet.soapware.handlers.SQLBeerBoardMobileHandler.signHttpsCertificate;



public class SQLSocialMediaHandler  implements Handler{

    private MidwareLogger logger;
    private static final String transConnName
                                            = "auper";
    private RegisteredConnection transconn;
    private SecureSession ss;
    private DecimalFormat cf;
    private LocationMap locationMap;
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Creates a new instance of SQLSocialMediaHandler
     */
    public SQLSocialMediaHandler() throws HandlerException {
        HandlerUtils.initializeClientKeyManager();
        logger                              = new MidwareLogger(SQLSocialMediaHandler.class.getName());
        transconn                           = null;
        locationMap                         = null;
        cf                                  = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    }

    public void handle(Element toHandle, Element toAppend) throws HandlerException{

        String function                     = toHandle.getName();
        String responseNamespace            = (String)SOAPMessage.getURIMap().get("tkmsg");

        String clientKey                    = HandlerUtils.getOptionalString(toHandle,"clientKey");
        ss                                  = ClientKeyManager.getSession(clientKey);

        logger                              = new MidwareLogger(SQLBeerBoardMobileHandler.class.getName(), function);
        logger.debug("SQLSocialMediaHandler processing method: "+function);
        logger.xml("request: " + toHandle.asXML());

        transconn                           = DatabaseConnectionManager.getNewConnection(transConnName, function + " (SQLSocialMediaHandler)");

        cf.applyPattern("#.####");
        try {
            // All methods require an admin client key
            if (ss.getLocation() == 0 && ss.getClientId() == 1 && ss.getSecurityLevel().canAdmin()) {
                if ("updateLatLong".equals(function)) {
                    updateLatLong(toHandle, responseFor(function,toAppend));
                } else if ("createURLShortner".equals(function)) {
                    createURLShortner(toHandle, responseFor(function,toAppend));
                } else if ("socialGetLocation".equals(function)) {
                    socialGetLocation(toHandle, responseFor(function,toAppend));
                }  else {
                    logger.generalWarning("Unknown function '" + function + "'.");
                }
            }  else if ("addUpdateFacebookData".equals(function)) {
                    addUpdateFacebookData(toHandle, responseFor(function,toAppend));
            } else if ("addUpdateFacebookPage".equals(function)) {
                    addUpdateFacebookPage(toHandle, responseFor(function,toAppend));
            } else if ("getFacebookToken".equals(function)) {
                    getFacebookToken(toHandle, responseFor(function,toAppend));
            } else if ("getFacebookPageMap".equals(function)) {
                    getFacebookPageMap(toHandle, responseFor(function,toAppend));
            } else if ("deleteFacebookToken".equals(function)) {
                    deleteFacebookToken(toHandle, responseFor(function,toAppend));
            } else if ("facebookPost".equals(function)) {
                    facebookPost(toHandle, responseFor(function,toAppend));
            } else if ("addUpdateTwitterData".equals(function)) {
                    addUpdateTwitterData(toHandle, responseFor(function,toAppend));
            } else if ("addUpdateFacebookPageGroup".equals(function)) {
                    addUpdateFacebookPageGroup(toHandle, responseFor(function,toAppend));
            } else if ("checkFacebookToken".equals(function)) {
                    checkFacebookToken(toHandle, responseFor(function,toAppend));
            } else {
                // access violation
                addErrorDetail(toAppend, "Access violation: This method is not available with your client key");
                logger.portalAccessViolation("Tried to call '"+function+"' with key "+ss.toString());
            }
        } catch (Exception e) {
            if (e instanceof HandlerException) {
                throw (HandlerException) e;
            } else {
                logger.midwareError("Non-handler exception thrown in ReportHandler: "+e.toString());
                logger.midwareError("XML: " + toHandle.asXML());
                throw new HandlerException(e);
            }
        } finally {
            // Log database use
            int queryCount                  = transconn.getQueryCount();
            logger.dbAction("Executed " + queryCount + " report quer" + (queryCount == 1 ? "y" : "ies"));
            transconn.close();
        }

        logger.xml("response: " + toAppend.asXML());

    }

    private Element responseFor(String s, Element e) {
        String responseNamespace = (String)SOAPMessage.getURIMap().get("tkmsg");
        return e.addElement("m:"+s+"Response",responseNamespace);
    }


    private String nullToEmpty(String s) {
        return (null == s) ? "" : s;
    }

    private void close(Statement s) {
        if (s != null) {
            try { s.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(Connection c) {
        if (c != null) {
            try { c.close(); } catch (SQLException sqle) { }
        }
    }
    private void close(RegisteredConnection c) {
        c.close();
    }

    private void addErrorDetail(Element toAppend, String message) {
        toAppend.addElement("error").addElement("detail").addText(message);
    }
    private int getCallerId(Element toHandle) throws HandlerException {
        return HandlerUtils.getRequiredInteger(HandlerUtils.getRequiredElement(toHandle,"caller"),"callerId");
    }

    private boolean checkForeignKey(String table, int value) throws SQLException, HandlerException {
        return checkForeignKey(table,"id",value);
    }

    private boolean checkForeignKey(String table, String field, int value) throws SQLException, HandlerException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean result = false;

        String select = "SELECT " + field + " FROM " + table +
                " WHERE " + field + " = ?";

        stmt = transconn.prepareStatement(select);
        stmt.setInt(1, value);
        rs = stmt.executeQuery();
        result = rs.next();

        close(rs);
        close(stmt);
        return result;
    }

    /**     Beer Board security levels:
     *  All users are associated with a Beer Board Location
     *
     *  Each user has a Manager flag.
     *  A manager can access admin functions for the location.  They can also set
     *  up subordinate accounts for their location, and set other permissions.
     *
     * If a user is not a manager, the user is considered to be the beer board page
     *
     * See also: net.terakeet.usbn.WebPermssion
     */
   


   private String getMarketingCSS() throws HandlerException {

         String  css                        = "@charset 'utf-8'; " +
                                            " @font-face { font-family: 'ChalkDust'; src: url('file:///android_asset/fonts/chalkdust.ttf'); } " +
                                            " @font-face { font-family: 'PPETRIAL'; src: url('file:///android_asset/fonts/PPETRIAL.otf'); } " +
                                            " body { background-image: url('http://beerboard.tv/USBN.BeerBoard.UI/Images/beerboard.jpg'); background-position: left top; background-repeat: repeat; fieldset border-style:none; }" +
                                            " .beer_loc_title{ font-family: 'PPETRIAL'; font-size:25px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_loc_title1{ font-family: 'PPETRIAL'; font-size:22px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +    
                                            " .beer_list_title{ font-family: 'ChalkDust'; font-size:28px; font-weight:bold; color: #ffffff; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_abv { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } "+
                                            "  .percentage { font-family: 'PPETRIAL'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_desc { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } " +
                                            " .beer_list_Season { font-family: 'ChalkDust'; font-size:15px; font-weight:bold; color: #ff9900; overflow:hidden;  white-space:nowrap; } ";
        return css;
   }


  


    private void addUpdateFacebookData(Element toHandle, Element toAppend) throws Exception {

        String user_id                      = HandlerUtils.getRequiredString(toHandle, "user_id");
        String access_token                 = HandlerUtils.getRequiredString(toHandle, "access_token");
        String expires                      = HandlerUtils.getRequiredString(toHandle, "expires");
        String user_name                    = HandlerUtils.getOptionalString(toHandle, "user_name");
        String email                        = HandlerUtils.getOptionalString(toHandle, "email");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectToken                  = " SELECT  access_token FROM usbnFacebook  WHERE  user_id = ? ";

        try {
            stmt                            = transconn.prepareStatement(selectToken);
            stmt.setString(1, user_id);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                stmt                        = transconn.prepareStatement("UPDATE usbnFacebook SET access_token = ? WHERE user_id = ? ; ");
                stmt.setString(1, user_id);
                stmt.setString(2, access_token+"&"+expires);
                stmt.executeUpdate();
                Element facebookData        = toAppend.addElement("facebookData");
                facebookData.addElement("insertId").addText(user_id);
            } else {
                int id                      = 0;
                String getLastId            = " SELECT LAST_INSERT_ID()";
                stmt                        = transconn.prepareStatement("INSERT INTO usbnFacebook (user_id, access_token, user_name,email) VALUES (?, ?, ?, ?)");
                stmt.setString(1, user_id);
                stmt.setString(2, access_token+"&"+expires);
                stmt.setString(3, user_name);
                stmt.setString(4, email);
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    id                      = rs.getInt(1);
                }
                Element facebookData        = toAppend.addElement("facebookData");
                facebookData.addElement("insertId").addText(Integer.toString(id));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }


     private void getFacebookToken(Element toHandle, Element toAppend) throws Exception {
        String user_id                      = HandlerUtils.getRequiredString(toHandle, "user_id");
        String access_token                 = null;
        String expires                      = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectToken                  = " SELECT  access_token FROM usbnFacebook  WHERE  user_id = ? ";
        try {
        stmt                                = transconn.prepareStatement(selectToken);
            stmt.setString(1, user_id);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                access_token                = rs.getString(1);
                String split[]              = access_token.split("&");
                if(split.length > 0) {
                    access_token            = split[0];
                    expires                 = split[1];
                }
                Element facebookData        = toAppend.addElement("facebookToken");
                facebookData.addElement("access_token").addText(access_token);
                facebookData.addElement("expires").addText(expires);
            }
            else {
                Element facebookData        = toAppend.addElement("facebookToken");
                facebookData.addElement("access_token").addText("NOT");
                facebookData.addElement("expires").addText("NOT");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }



    }


      private void facebookPost(Element toHandle, Element toAppend) throws Exception {
        String user_id                      = HandlerUtils.getRequiredString(toHandle, "user_id");
        String access_token                 = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectToken                  = " SELECT  access_token FROM usbnFacebook  WHERE  user_id = ? ";
        try {
        stmt                                = transconn.prepareStatement(selectToken);
        stmt.setString(1, user_id);
        rs                                  = stmt.executeQuery();
        if(rs.next()) {
            access_token                    = rs.getString(1);
        }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(SELECT+target_id+FROM+connection+WHERE+source_id="+user_id+"+AND+is_following=1)+&"+access_token;
                URL u = new URL(g);
                signHttpsCertificate();
                URLConnection c             = u.openConnection();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        String pagename     = fa1.getJSONObject(i).getString("name");
                        String fancount     = fa1.getJSONObject(i).getString("fan_count");
                        String pic          = fa1.getJSONObject(i).getString("pic");
                        String msg          ="testmessageSuba";
                        String tar          ="{'countries':['US']}";
                        String url          = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                        // System.out.println(f.getData(new URL(url)));
                        url                 = "https://graph.facebook.com/"+pageid+"/tabs?access_token="+accesstoken;
                       logger.debug(url);
                        u                   = new URL(url);
                        c                   = u.openConnection();
                        conn                = (HttpsURLConnection) u.openConnection();
                        in                  = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        b                   = new StringBuffer();

                        while ((inputLine = in.readLine()) != null)
                            b.append(inputLine + "\n");

                        in.close();
                        graph               = b.toString();
                        json                = new JSONObject(graph);
                        JSONArray fa2       = json.getJSONArray("data");                       
                        for(int n = 0; n < fa2.length(); n++) {
                            JSONObject object = fa2.getJSONObject(n);
                            if(object.toString().contains("application")) {
                                JSONObject fa3 = fa2.getJSONObject(n).getJSONObject("application");                                
                                //object = fa3.getJSONObject(n);
                                System.out.println("Application"+fa3.toString());
                                if(fa3.toString().contains("USBeverageNet")) {
                                    url = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                                    String albumurl="https://graph.facebook.com/"+pageid+"/albums?access_token="+accesstoken+"&name=bbtv&message=BeerBoardTv&method=post";
                                    //  json = new JSONObject(albumid);
                                    String imageurl="https://graph.facebook.com/520776424610813/photos?access_token="+accesstoken+"&method=post&url=http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/Arbor%20Brewing%20Company.png&message=Arbor%20Brewing%20Company&feed_targeting="+tar;
                                    logger.debug("Post Photos");
                                    postPhotos(pageid,accesstoken );
                                    
                                    
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.dbError("getJson error: "+e.toString());
                e.printStackTrace();
            }
        }
      }

      private void deleteFacebookToken(Element toHandle, Element toAppend) throws Exception {
        String user_id                      = HandlerUtils.getRequiredString(toHandle, "user_id");
        String access_token                 = null;
        String expires                      = null;
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectToken                  = "SELECT  access_token FROM usbnFacebook  WHERE  user_id = ? ";

        try {
            stmt                            = transconn.prepareStatement(selectToken);
            stmt.setString(1, user_id);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                stmt                        = transconn.prepareStatement("DELETE FROM usbnFacebook  WHERE user_id = ? ; ");
                stmt.setString(1, user_id);
                stmt.executeUpdate();
                Element facebookData        = toAppend.addElement("facebookToken");
                facebookData.addElement("access_token").addText("NOT");
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        }

    }

    public void writeHtml(String boardName,String location, String city, String zip,  String product, String abv, String orgin, String style, String brewery) {
        try {
        StringBuffer sb                     = new StringBuffer();
        String brewVisibility               = "none";
        String glassVisibility              = "none";
        String brewUrl                      = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBlogo/"+brewery;
        String glassUrl                     = "http://beerboard.tv/USBN.BeerBoard.UI/Images/FBglass/" + style + ".png";
        if(!saveImage(brewUrl, "/home/midware/facebook/brewlogo.png")) {
            brewVisibility                  = "hidden";
        }
        if(!saveImage(glassUrl, "/home/midware/facebook/glasslogo.png")) {
            glassVisibility                 = "hidden";
        }
        
        sb.append("<html><head><style type=\"text/css\">");
        sb.append(getMarketingCSS());
        sb.append("body { }</style></head>");
        sb.append("<body>");
        sb.append("<table width=\"500\" height=\"20\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 5px;'>");
        sb.append("<tr width=\"500\" height=\"20\"><td valign=\"top\" align=\"center\"><span class='beer_loc_title'>Now on Tap");
        sb.append("</span><p><span class='beer_loc_title'>");
        sb.append(boardName);
        sb.append("</span></p><p><span class='beer_loc_title1'>");
        sb.append(location+", "+city+" - "+zip);
        sb.append("</span></p><p><span class='beer_list_title'>    </span></p><p><span class='beer_list_title'>");
        sb.append(product);
        sb.append("</span></p></td></tr></table>");
        sb.append("<table width=\"500\" height=\"200\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>");
        sb.append("<tr><td valign=\"top\" align=\"center\"><img src=\"brewlogo.png");
        sb.append("\" width=\"500\" height=\"200\" alt=\"" + brewery.replaceAll("%20", " ") + "\" style=\"visibility:"+brewVisibility+"\"/></td></tr></table>");
        sb.append("<table width=\"500\" height=\"180\" border='0' cellspacing='0' cellpadding='0' style='padding-left: 15px;'>  ");
        sb.append("<tr><td width=\"250\" align=\"left\" valign=\"middle\"><span class='beer_list_abv'>"
                + "Style: " + style.replaceAll("%20", " ") + "</span><p><span class='beer_list_abv'>ABV: "+abv+"</span><span class='percentage'>%</span></p>"
                + "<p><span class=\"beer_list_abv\">Origin: "+orgin + "</span></p></td>");
        sb.append("<td width=\"250\" align=\"center\" valign=\"middle\"><img src=\"glasslogo.png");
        sb.append("\" width=\"100\" height=\"225\" alt=\"" + style.replaceAll("%20", " ") + "\" style=\"visibility:"+glassVisibility+"\"/></td>");
        sb.append("</tr></table>");
        sb.append("<table width=\"500\" height=\"100\" border='0' cellspacing='0' cellpadding='0' style='padding-top: 15px;'>");
        sb.append("<tr><td valign=\"top\" align=\"right\" valign=\"bottom\"><img src=\"beerboardlogo.png");
        sb.append("\" width=\"140\" height=\"40\" alt=\"Beer Board TV\"/></td></tr></table></body></html>");
        
        
         File file                          = new File("/home/midware/facebook/productHTML.htm");
         BufferedWriter bw                  = new BufferedWriter(new FileWriter(file));
         bw.write(sb.toString());
         bw.close();
         //logger.debug(sb.toString());
        }catch (Exception e) {
                logger.dbError("Html: "+e.toString());
                e.printStackTrace();
        }
    }
    
    private boolean  saveImage(String imageUrl, String destinationFile)  {
        boolean visible                     = false;
        try {
            URL url                         = new URL(imageUrl);
            InputStream is                  = url.openStream();
            OutputStream os                 = new FileOutputStream(destinationFile);
            byte[] b                        = new byte[2048];
            int length;
            while ((length = is.read(b)) != -1) {
                os.write(b, 0, length);
            }
            is.close();
            os.close();
            visible                         = true;
        }catch(Exception e) {
            e.printStackTrace();
        }
        return visible;
    }

   public void postOnFacebook(String user_id, String access_token, String message) throws Exception {
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(SELECT+target_id+FROM+connection+WHERE+source_id="+user_id+"+AND+is_following=1)+&"+access_token;
                URL u = new URL(g);
                signHttpsCertificate();
                URLConnection c             = u.openConnection();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        String pagename     = fa1.getJSONObject(i).getString("name");
                        String fancount     = fa1.getJSONObject(i).getString("fan_count");
                        String pic          = fa1.getJSONObject(i).getString("pic");
                        String msg          ="testmessageSuba";
                        String tar          ="{'countries':['US']}";
                        String url          = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                        url                 = "https://graph.facebook.com/"+pageid+"/tabs?access_token="+accesstoken;

                        u                   = new URL(url);
                        c                   = u.openConnection();
                        conn                = (HttpsURLConnection) u.openConnection();
                        in                  = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        b                   = new StringBuffer();

                        while ((inputLine = in.readLine()) != null)
                            b.append(inputLine + "\n");

                        in.close();
                        graph               = b.toString();
                        json                = new JSONObject(graph);
                        JSONArray fa2       = json.getJSONArray("data");
                        for(int n = 0; n < fa2.length(); n++) {
                            JSONObject object = fa2.getJSONObject(n);
                            if(object.toString().contains("application")) {
                                JSONObject fa3 = fa2.getJSONObject(n).getJSONObject("application");
                                //object = fa3.getJSONObject(n);
                                System.out.println("Application"+fa3.toString());
                                if(fa3.toString().contains("USBeverageNet")) {
                                    url = "https://graph.facebook.com/"+pageid+"/feed?access_token="+accesstoken+"&method=post&message="+msg+"&feed_targeting="+tar;
                                    String albumurl="https://graph.facebook.com/"+pageid+"/albums?access_token="+accesstoken+"&name=bbtv&message=BeerBoardTv&method=post";
                                    //  json = new JSONObject(albumid);
                                    String imageurl="https://graph.facebook.com/520776424610813/photos?access_token="+accesstoken+"&method=post&url=http://beerboard.tv/USBN.BeerBoard.UI/Images/logo/Arbor%20Brewing%20Company.png&message=Arbor%20Brewing%20Company&feed_targeting="+tar;
                                    //logger.debug("Post Photos");
                                    postHTML(pageid, accesstoken, message);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.dbError("getJson error: "+e.toString());
                e.printStackTrace();
            }
        }
    }
   
   
    public void postOnFacebook(String user_id, String access_token, String message,String pageId,int type) throws Exception {
        if(access_token!=null) {
            try {
                String g                    = "https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(SELECT+target_id+FROM+connection+WHERE+source_id="+user_id+"+AND+is_following=1)+&"+access_token;
                URL u = new URL(g);
                signHttpsCertificate();
                URLConnection c             = u.openConnection();
                HttpsURLConnection conn     = (HttpsURLConnection) u.openConnection();
                BufferedReader in           = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer b              = new StringBuffer();
                while ((inputLine = in.readLine()) != null)
                    b.append(inputLine + "\n");
                in.close();
                String graph                = b.toString();
                JSONObject json             = new JSONObject(graph);
                JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                for(int i=0;i<fa1.length();i++) {
                    String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                    if(!accesstoken.equals("null")) {
                        String pageid       = fa1.getJSONObject(i).getString("page_id");
                        if(pageid.equals(pageId)) {
                            if(type==1) {
                                postHTML(pageid, accesstoken, message);
                            } else {
                                postOnFeed(accesstoken, pageid, message);
                            }
                        }
                    }                    
                }
            } catch (Exception e) {
                logger.dbError("getJson error: "+e.toString());
                e.printStackTrace();
            }
        }
    }




   private void postPhotos(String pageid, String accesstoken ) {
        PostMethod filePost                 = new PostMethod("https://graph.facebook.com/"+pageid+"/photos");
        File file                           = null;
        try {
            filePost.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, false);
            //println("Uploading " + file.getName() + " to 'https://graph.facebook.com/me/photos'");
            String imageUrl = "http://beerboard.tv/USBN.BeerBoard.UI/Images/bear-republic-rocket-red-ale.jpg";
            String destinationFile = "image.jpg";
            saveImage(imageUrl, destinationFile);
            //BufferedImage  ire = HtmlToImage.create("file:///C:/Users/suba/Desktop/Test/Marketting.html", 900, 600);
            //ImageIO.write(ire, "jpg", new File("image.jpg"));
            file = new File("image.jpg");
            FilePart fp=new FilePart("source", file.getName(), file);
            Part[] parts ={fp, new StringPart("access_token", accesstoken), new StringPart("message", "")};
            
            filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));
            HttpClient client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
            int status = client.executeMethod(filePost);
            if (status == HttpStatus.SC_OK) {
                System.out.println("Upload complete, response=" + filePost.getResponseBodyAsString());
                 
            } else {
                System.out.println("Upload failed, response=" + HttpStatus.getStatusText(status));
                // Create response
                StringBuilder notificationsSendResponse = new StringBuilder();
                byte[] byteArrayNotifications = new byte[4096];
                for (int n; (n = filePost.getResponseBodyAsStream().read(byteArrayNotifications)) != -1;) {
                    notificationsSendResponse.append(new String(byteArrayNotifications, 0, n));
                }
                String notificationInfo = notificationsSendResponse.toString();
            }
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getClass().getName() + " " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            filePost.releaseConnection();
            file.delete();
            file                          = new File("/home/midware/facebook/productHTML.htm");
            file.delete();
        }
    }

    public String getData(URL urL) {
       
        String graph                        =null;
        try {
            signHttpsCertificate();
            String inputLine;
            HttpsURLConnection conn         = (HttpsURLConnection) urL.openConnection();
            logger.debug(String.valueOf(conn.getResponseCode()));
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuffer  b                 = new StringBuffer();
            while ((inputLine = in.readLine()) != null)
             b.append(inputLine + "\n");
            in.close();
            graph                           = b.toString();
        }catch (Exception e) {
            //e.printStackTrace();
            //return  e.getMessage();
        }
        return graph;

    }
    
    public int getDataCode(URL urL) {
       
        int code                        =0;
        try {
            signHttpsCertificate();
            String inputLine;
            HttpsURLConnection conn         = (HttpsURLConnection) urL.openConnection();
            code                            = conn.getResponseCode();
        }catch (Exception e) {
            //e.printStackTrace();
            //return  e.getMessage();
        }
        return code;

    }
    
    
    
    public String getHttpData(URL urL) {
        String graph                        =null;
        try {
            String inputLine;
            HttpURLConnection conn         = (HttpURLConnection) urL.openConnection();
            BufferedReader in               = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuffer  b                 = new StringBuffer();
            while ((inputLine = in.readLine()) != null)
             b.append(inputLine + "\n");
            in.close();
            graph                           = b.toString();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return graph;

    }

     private void postHTML(String pageid, String accesstoken, String message) {
         signHttpsCertificate();
        PostMethod filePost                 = new PostMethod("https://graph.facebook.com/"+pageid+"/photos");
        File file = null;
        try {
            filePost.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, false);
            BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
            ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
            file = new File("/home/midware/facebook/image.jpg");
            FilePart fp=new FilePart("source", file.getName(), file);
            Part[] parts ={fp, new StringPart("access_token", accesstoken), new StringPart("message", message)};
            filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams()));
            HttpClient client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
            int status = client.executeMethod(filePost);
            if (status == HttpStatus.SC_OK) {
                System.out.println("Upload complete, response=" + filePost.getResponseBodyAsString());  
                //logger.debug("Post Completed");
                //tweetStatus();
            } else {
                System.out.println("Upload failed, response=" + HttpStatus.getStatusText(status));
                // Create response
                StringBuilder notificationsSendResponse = new StringBuilder();
                byte[] byteArrayNotifications = new byte[4096];
                for (int n; (n = filePost.getResponseBodyAsStream().read(byteArrayNotifications)) != -1;) {
                    notificationsSendResponse.append(new String(byteArrayNotifications, 0, n));
                }
                String notificationInfo = notificationsSendResponse.toString();
            }
        } catch (Exception ex) {
             logger.dbError("getJson error: "+ex.toString());
        } finally {
            filePost.releaseConnection();
            
            
        }
    }
     
     
    public void tweetStatus(String message){
        
            logger.debug("Twitter");
        try {
            ConfigurationBuilder cb         = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey("Hs4u3FYn1KbeqA9ztuFyDQ")
                    .setOAuthConsumerSecret("rMQcqe6TKuemxP7MZoduJVGsx9SwdEAeInTufnb6U")
                    .setOAuthAccessToken("169164243-THZjuBE5FL0pFg7asZbUiOqmiokCSylDdLKpAozf")
                    .setOAuthAccessTokenSecret("xwVge08LzVxpLiqDdkgHv1Cia1zY3HzCilJIOoS94");
            
            TwitterFactory tf               = new TwitterFactory(cb.build());
            Twitter twitter                 = tf.getInstance();
            try {
                RequestToken requestToken   = twitter.getOAuthRequestToken();
                AccessToken accessToken     = null;
                while (null == accessToken) {
                    try {
                        accessToken         = twitter.getOAuthAccessToken(requestToken);
                    } catch (TwitterException te) {
                        if (401 == te.getStatusCode()) {
                        logger.debug("Unable to get the access token.");
                        } else {
                            te.printStackTrace();
                            }
                        }
                    }
                logger.debug("Got access token.");
                logger.debug("Access token: " + accessToken.getToken());
                logger.debug("Access token secret: " + accessToken.getTokenSecret());
            } catch (IllegalStateException ie) {
                // access token is already available, or consumer key/secret is not set.
                if (!twitter.getAuthorization().isEnabled()) {
                    logger.debug("OAuth consumer key/secret is not set.");
                    return;
                    }
                }
            StatusUpdate status             = new StatusUpdate(message);
            //BufferedImage  ire = HtmlToImage.create("file:////home/midware/facebook/productHTML.htm", 500, 650);
            //ImageIO.write(ire, "jpg", new File("/home/midware/facebook/image.jpg"));
            status.setMedia(new File("/home/midware/facebook/image.jpg"));
            Status status2 = twitter.updateStatus(status);
            logger.debug("Successfully updated the status to [" + status2.getText() + "].");
            
        } catch (TwitterException te) {
            te.printStackTrace();
           logger.debug("Failed to get timeline: " + te.getMessage());
            } 
        catch(Exception ie){ 
            ie.printStackTrace();;
        }
        }
    
     private void addUpdateFacebookPage(Element toHandle, Element toAppend) throws Exception {

        String fbid                         = HandlerUtils.getRequiredString(toHandle, "fbid");
        String pageid                       = HandlerUtils.getRequiredString(toHandle, "pageid");
        int platform                        = HandlerUtils.getRequiredInteger(toHandle, "platform");
        String username                     = HandlerUtils.getRequiredString(toHandle, "username");
        String password                     = HandlerUtils.getRequiredString(toHandle, "password");

        String checkRoot                    = "SELECT u.id, u.isBeerBoardManager, u.customer FROM user u " +
                                            " WHERE u.username = ? AND u.password = ? AND u.platform IN (?)";
        String selectNormal                 = "SELECT l.id FROM userBeerBoardMap uBBM LEFT JOIN location l ON uBBM.location = l.id LEFT JOIN locationGraphics lG ON lG.location = l.id " +
                                            " LEFT JOIN customer c ON l.customer = c.id WHERE uBBM.user = ? ORDER BY l.name ASC";
        String selectPage                   = "SELECT id FROM usbnFacebookPage WHERE pageid = ?;";
        String insertPage                   = "INSERT INTO usbnFacebookPage (fbid, location, pageid) VALUES (?, ?, ?);";
        String updatePage                   = "UPDATE usbnFacebookPage SET location = ? WHERE id = ?;";

        int userId                          = -1, locationId = -1;
        String platformName                 = "BeerBoard";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsPage = null;

        try {
            // we need to know if the user has Admin access
            stmt                            = transconn.prepareStatement(checkRoot);
            stmt.setString(1, username);
            stmt.setString(2, password);
            stmt.setInt(3, platform);
            rs                              = stmt.executeQuery();
            
            if (rs != null && rs.next()) {
                int rsIndex                 = 1;
                userId                      = rs.getInt(rsIndex++);
                               
            }
            if (userId >= 0) {
                // the user is not an Admin(root)
                String logMessage           = "Granting map-level access to " + username + " for " + platformName;
                logger.portalDetail(userId, "login", 0, logMessage, transconn);
                stmt                        = transconn.prepareStatement(selectNormal);
                stmt.setInt(1, userId);
                rs                          = stmt.executeQuery();
                if (rs != null && rs.next()) {
                    int rsIndex             = 1;
                    locationId              = rs.getInt(rsIndex++);
                    stmt                    = transconn.prepareStatement(selectPage);
                    stmt.setString(1, pageid);
                    rsPage                  = stmt.executeQuery();
                    if(rsPage.next()) {
                        int id =rsPage.getInt(1);
                        stmt                = transconn.prepareStatement(updatePage);
                        stmt.setInt(1, locationId);
                        stmt.setInt(2, id);
                        stmt.executeUpdate();
                        
                    } else {
                        stmt                = transconn.prepareStatement(insertPage);
                        stmt.setString(1, fbid);
                        stmt.setInt(2, locationId);
                        stmt.setString(3, pageid);
                        stmt.executeUpdate();
                        
                    }
                    
                    
                }
            } else {
                logger.portalAction("Authentication failed to " + username + " for " + platformName);
                // authentication failed
            }
            Element facebookData            = toAppend.addElement("facebookData");
                facebookData.addElement("location").addText(Integer.toString(locationId));
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rsPage);
        }
    }
     
     
     private void addUpdateFacebookPageGroup(Element toHandle, Element toAppend) throws Exception {
        String selectPage                   = "SELECT id FROM usbnFacebookPage WHERE pageid = ? AND type=? AND location = ?;";
        String insertPage                   = "INSERT INTO usbnFacebookPage (fbid, location, pageid,type) VALUES (?, ?, ?,?);";
        String deletePage                   = "DELETE FROM  usbnFacebookPage  WHERE id = ?;";

       
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsPage = null;

        try {
            Iterator inserPageEl         = toHandle.elementIterator("insertPage");
            while (inserPageEl.hasNext()) {
                Element insert              = (Element) inserPageEl.next();
                String fbid                 = HandlerUtils.getRequiredString(insert, "fbid");
                String pageid               = HandlerUtils.getRequiredString(insert, "pageId");
                int locationId              = HandlerUtils.getRequiredInteger(insert, "location");
                int type                    = HandlerUtils.getRequiredInteger(insert, "type");
                if(pageid!=null&& pageid.length()>4){
                stmt                    = transconn.prepareStatement(selectPage);
                stmt.setString(1, pageid);
                 stmt.setInt(2, type);
                stmt.setInt(3, locationId);
                rsPage                  = stmt.executeQuery();
                if(!rsPage.next()) {
                stmt                = transconn.prepareStatement(insertPage);
                stmt.setString(1, fbid);
                stmt.setInt(2, locationId);
                stmt.setString(3, pageid);
                stmt.setInt(4, type);
                stmt.executeUpdate();
                }
                }

            
            Element facebookData            = toAppend.addElement("facebookData");
            facebookData.addElement("status").addText(Integer.toString(1));
            }
        
        
        Iterator deletePageEl               = toHandle.elementIterator("deletePage");
        while (deletePageEl.hasNext()) {
            Element delete                  = (Element) deletePageEl.next();
            int id                          = HandlerUtils.getRequiredInteger(delete, "id");
            if(id >0) {
                stmt                    = transconn.prepareStatement(deletePage);
                stmt.setInt(1,id);
                stmt.executeUpdate();
            }
        }

        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
            close(rsPage);
        }
    }



     private void getFacebookPageMap(Element toHandle, Element toAppend) throws Exception {
        String fbId                         = HandlerUtils.getRequiredString(toHandle, "fbid");
        
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectPage                   = "SELECT  f.pageid,l.name,f.id,f.location FROM usbnFacebookPage f LEFT JOIN location l ON l.id =f.location WHERE f.fbid = ? AND pageid<>'null';";
        try {
            
            stmt                            = transconn.prepareStatement(selectPage);
            stmt.setString(1, fbId);
            rs                              = stmt.executeQuery();
            
            while(rs.next()) {
                 Element facebookData       = toAppend.addElement("facebookPage");
                facebookData.addElement("pageid").addText(HandlerUtils.nullToEmpty(rs.getString(1)));
                facebookData.addElement("locationName").addText(HandlerUtils.nullToEmpty(rs.getString(2)));
                facebookData.addElement("id").addText(HandlerUtils.nullToEmpty(rs.getString(3)));
                facebookData.addElement("location").addText(HandlerUtils.nullToEmpty(rs.getString(4)));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
     }
     
     
      private void createURLShortner(Element toHandle, Element toAppend) throws HandlerException {

        final String GOOGL_URL              = "https://www.googleapis.com/urlshortener/v1/url?key=AIzaSyB9h-gJ2k4vQ9iv9fFJ0VbS0w7luCJV0HY";
        String selectAllLocation            = "SELECT id FROM location WHERE NOT EXISTS (SELECT location FROM shortURL WHERE shortURL.location=location.id );";
        String insertURL                    = "INSERT INTO shortURL (location,url) VALUES (?,?);";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, lRs = null;
        
        try {
            stmt                            = transconn.prepareStatement(selectAllLocation);
            lRs                             = stmt.executeQuery();
            while(lRs.next()) {
                String shortURL             = null;
                int location                = lRs.getInt(1);
                String original             = "http://www.beerboard.tv/beerboard.php?LID=" + location;

                HttpTransport transport     = GoogleTransport.create();
                GoogleHeaders defaultHeaders= new GoogleHeaders();
                transport.defaultHeaders    = defaultHeaders;
                transport.defaultHeaders.put("Content-Type", "application/json");
                transport.addParser(new JsonHttpParser());
                HttpRequest request         = transport.buildPostRequest();
                request.setUrl(GOOGL_URL);
                GenericData data            = new GenericData();
                data.put("longUrl", original);
                JsonHttpContent content     = new JsonHttpContent();
                content.data                = data;
                request.content             = content;
                HttpResponse response       = null;
                Result result               = null;
                try {
                    response                = request.execute();
                    result                  = response.parseAs(Result.class);
                    shortURL                = result.shortUrl;
                    logger.debug(shortURL);
                } catch(IOException ie) {
                    ie.printStackTrace();
                }
                stmt                        = transconn.prepareStatement(insertURL);
                stmt.setInt(1, location);
                stmt.setString(2, shortURL);
                stmt.executeUpdate();
            }
        }catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(lRs);
            close(stmt);
        }
      }
      
      public static class Result extends GenericJson {    
          @Key("id")
          public String shortUrl;
      }
      
      
      public void postOnPage(ArrayList<String> fbId,ArrayList<String> pageId, ArrayList<String> accessToken, String message) {
          for(int i=0;i<fbId.size();i++) {
              String user_id                = fbId.get(i);
              String access_token           = accessToken.get(i);
              String page_id                = pageId.get(i);
          if(access_token!=null) {
            try {
                postOnFacebook(user_id, access_token, message, page_id, 2);
                
            } catch (Exception e) {
                logger.dbError("getJson error: "+e.toString());
                e.printStackTrace();
            }
        }
          }
      }
          public void postOnFeed(String access_token, String page_id,String message ) {
          if(access_token!=null) {
            try {
                 String tar          ="{'countries':['US']}";
                String url          = "https://graph.facebook.com/"+page_id+"/feed?access_token="+access_token+"&method=post&message="+message+"&feed_targeting="+tar;
                logger.debug("Url:"+url);
                URL urL                        = new URL(URIUtil.encodeQuery(url));
                getData(urL);
                
            } catch (Exception e) {
                logger.dbError("getJson error: "+e.toString());
                e.printStackTrace();
            }
      }
      }
          
          
    private void updateLatLong(Element toHandle, Element toAppend) throws HandlerException {

        int location                        = HandlerUtils.getRequiredInteger(toHandle, "locationId");
        String selectAllLocation            = "SELECT id, CONCAT(addrStreet,',',addrCity,',',addrZip,',',addrState,',',addrCountry) FROM location WHERE (latitude = 0 OR longitude = 0);";
        PreparedStatement stmt              = null;
        ResultSet rs                        = null, lRs = null;
        
        try {
            stmt                            = transconn.prepareStatement(selectAllLocation);
            lRs                             = stmt.executeQuery();
            while(lRs.next()) {
                location                    = lRs.getInt(1);
                String address              = lRs.getString(2);
                String url                  = "http://maps.googleapis.com/maps/api/geocode/json?address="+address+"&sensor=true";

                String data                 = getHttpData(new URL(URIUtil.encodeQuery(url)));
                JSONObject json             = new JSONObject(data);
                System.out.println(json);
                JSONArray ja1               = json.getJSONArray("results");
                String lat                  = null,lan=null;
                for(int i=0;i<ja1.length();i++) {
                    JSONObject geo          = ja1.getJSONObject(i).getJSONObject("geometry");
                    JSONObject loc          = geo.getJSONObject("location");
                     //logger.debug("loc:"+loc);
                    lat                     = loc.getString("lat");
                    lan                     = loc.getString("lng");

                }
                
                if(lat==null || lat.equals("")) {
                    url                     = "http://maps.googleapis.com/maps/api/geocode/json?address="+address+"&sensor=true";
                    data                    = getHttpData(new URL(URIUtil.encodeQuery(url)));
                    json                    = new JSONObject(data);
                    ja1                     = json.getJSONArray("results");
                    for(int i=0;i<ja1.length();i++) {
                        JSONObject geo      = ja1.getJSONObject(i).getJSONObject("geometry");
                        JSONObject loc      = geo.getJSONObject("location");
                         //logger.debug("loc:"+loc);
                        lat                 = loc.getString("lat");
                        lan                 = loc.getString("lng");
                    }
                }

                if(lat!=null && lat.length()>0) {
                    logger.debug("Location: "+location+" Lat: "+lat+" Lan:"+lan);
                    stmt                    = transconn.prepareStatement("UPDATE location set latitude="+lat+", longitude="+lan+" WHERE id= ?;");
                    stmt.setInt(1, location);
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } catch(Exception e){
            logger.dbError("Json error: " + e.getMessage());
        } finally {
            close(rs);
            close(lRs);
            close(stmt);
        }
     }
    
    
    private void addUpdateTwitterData(Element toHandle, Element toAppend) throws Exception {

        String user_id                      = HandlerUtils.getRequiredString(toHandle, "user_id");
        String screenName                   = HandlerUtils.getRequiredString(toHandle, "screenName");
        String consumerKey                  = HandlerUtils.getRequiredString(toHandle, "consumerKey");
        String consumerSecret               = HandlerUtils.getOptionalString(toHandle, "consumerSecret");
        String accesToken                   = HandlerUtils.getOptionalString(toHandle, "accesToken");
        String tokenSecret                  = HandlerUtils.getOptionalString(toHandle, "tokenSecret");
        String location                     = HandlerUtils.getOptionalString(toHandle, "locationId");
        PreparedStatement stmt              = null;
        ResultSet rs                        = null;
        String selectToken                  = "SELECT  id FROM usbnTwitter  WHERE  user_id = ? AND screenName= ? AND consumerKey = ?";

        try {
            stmt                            = transconn.prepareStatement(selectToken);
            stmt.setString(1, user_id);
            stmt.setString(2, screenName);
            stmt.setString(3, consumerKey);
            rs                              = stmt.executeQuery();
            if(rs.next()) {
                int id                      = rs.getInt(1);
                stmt                        = transconn.prepareStatement("UPDATE usbnTwitter SET consumerKey = ?, consumerSecret = ?, accesToken =?, tokenSecret=?  WHERE id = ? ; ");
                stmt.setString(1, consumerKey);
                stmt.setString(2,consumerSecret);
                stmt.setString(3,accesToken);
                stmt.setString(4,tokenSecret);                
                stmt.setInt(5, id);
                stmt.executeUpdate();
                
                
                if(!location.contains(",")){
                    stmt                        = transconn.prepareStatement("DELETE FROM twitterLocationMap WHERE twitter = ? AND location =?;");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(location));
                    stmt.executeUpdate();
                
                    stmt                        = transconn.prepareStatement("INSERT INTO twitterLocationMap (twitter,location) VALUES (?,?);");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(location));
                    stmt.executeUpdate();
                } else {
                String locations[]= location.split(",");
                for(int i=0;i<locations.length;i++) {
                    stmt                        = transconn.prepareStatement("DELETE FROM twitterLocationMap WHERE twitter = ? AND location =?;");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(locations[i]));
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement("INSERT INTO twitterLocationMap (twitter,location) VALUES (?,?);");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(locations[i]));
                    stmt.executeUpdate();
                }
                }
                
                Element facebookData        = toAppend.addElement("twitter");
                facebookData.addElement("id").addText(String.valueOf(id));
            } else {
                int id                      = 0;
                String getLastId            = " SELECT LAST_INSERT_ID()";
                stmt                        = transconn.prepareStatement("INSERT INTO usbnTwitter (user_id, screenName, consumerKey,consumerSecret,accesToken,tokenSecret) VALUES (?, ?, ?, ?, ?, ?)");
                stmt.setString(1, user_id);
                stmt.setString(2, screenName);
                stmt.setString(3, consumerKey);
                stmt.setString(4,consumerSecret);
                stmt.setString(5,accesToken);
                stmt.setString(6,tokenSecret);                
                stmt.executeUpdate();
                stmt                        = transconn.prepareStatement(getLastId);
                rs                          = stmt.executeQuery();
                if (rs.next()) {
                    id                      = rs.getInt(1);
                }
                if(!location.contains(",")){
                    stmt                        = transconn.prepareStatement("DELETE FROM twitterLocationMap WHERE twitter = ? AND location =?;");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(location));
                    stmt.executeUpdate();
                
                    
                    stmt                        = transconn.prepareStatement("INSERT INTO twitterLocationMap (twitter,location) VALUES (?,?);");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(location));
                    stmt.executeUpdate();
                } else {
                String locations[]= location.split(",");
                for(int i=0;i<locations.length;i++) {
                    
                     stmt                        = transconn.prepareStatement("DELETE FROM twitterLocationMap WHERE twitter = ? AND location =?;");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(locations[i]));
                    stmt.executeUpdate();
                    
                    stmt                        = transconn.prepareStatement("INSERT INTO twitterLocationMap (twitter,location) VALUES (?,?);");
                    stmt.setInt(1,id);
                    stmt.setInt(2,Integer.parseInt(locations[i]));
                    stmt.executeUpdate();
                }
                }
                Element facebookData        = toAppend.addElement("twitter");
                facebookData.addElement("id").addText(String.valueOf(id));
            }
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(stmt);
            close(rs);
        }
    }
    
    
    private void socialGetLocation(Element toHandle, Element toAppend) throws HandlerException {

       
       
        int custId                          = HandlerUtils.getRequiredInteger(toHandle, "customerId");
        String selectAdmin                  = "SELECT l.id, l.name, l.type, c.id, c.name, c.type, l.easternOffset, l.volAdjustment, lD.beerboard " +
                                            " FROM customer c " +
                                            " LEFT JOIN location l ON l.customer = c.id LEFT JOIN locationDetails lD ON lD.location = l.id " +
                                            " WHERE lD.active = 1 AND c.id = ? " +
                                            " ORDER BY l.name ";

        PreparedStatement stmt              = null;
        ResultSet rs                        = null;

        try {
           
            stmt                        = transconn.prepareStatement(selectAdmin);
            stmt.setInt(1, custId);
            rs                          = stmt.executeQuery();
            while (rs != null && rs.next()) {
                int rsIndex             = 1;
                Element locEl           = toAppend.addElement("location");
                locEl.addElement("locationId").addText(String.valueOf(rs.getInt(rsIndex++)));
                locEl.addElement("locationName").addText(HandlerUtils.nullToEmpty((rs.getString(rsIndex++))));                
            }

               
        } catch (SQLException sqle) {
            logger.dbError("Database error: " + sqle.getMessage());
            throw new HandlerException(sqle);
        } finally {
            close(rs);
            close(stmt);
        }
    }
    
     private void checkFacebookToken(Element toHandle, Element toAppend) throws HandlerException {
         PreparedStatement stmt              = null;
        ResultSet rs                        = null, rsDetails=null;
         try {
             String selectPage              = "SELECT pageid,location,l.name FROM usbnFacebookPage fP LEFT JOIN location l ON l.id=fP.location WHERE fbid=?;";
             StringBuilder sb                   = new StringBuilder();
             sb.append("<tr><td><table><tr><td width=\"30%\" style=\"border-bottom: dashed 1px black; font-size:small;\" align=\"center\">UserId</td>"
                    + "<td width=\"30%\" style=\"border-bottom: dashed 1px black; font-size:small;\" align=\"center\">username</td>"
                    + "<td align=\"center\" style=\"border-bottom: dashed 1px black; font-size:small;\" width=\"20%\">email</td>"
                    + "<td align=\"center\" style=\"border-bottom: dashed 1px black; font-size:small;\" width=\"20%\">Code</td></tr> ");
             StringBuilder sbPage                   = new StringBuilder();
             sbPage.append("<tr><td><table><tr><td width=\"30%\" style=\"border-bottom: dashed 1px black; font-size:small;\" align=\"center\">FB User</td>"
                    + "<td width=\"40%\" style=\"border-bottom: dashed 1px black; font-size:small;\" align=\"center\">PageName</td>"                    
                    + "<td align=\"center\" style=\"border-bottom: dashed 1px black; font-size:small;\" width=\"30%\">location</td></tr> ");
             stmt                = transconn.prepareStatement("SELECT id,user_id, user_name,email,access_token  FROM usbnFacebook;");
             rs            = stmt.executeQuery();
             while(rs.next()) { 
                 String userId              = rs.getString(2);
                 String userName            = rs.getString(3);
                 String email               = rs.getString(4);
                 String token               = rs.getString(5);                 
                 String post ="https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+(select+page_id+from+page_admin+where+uid="+userId+")+&"+token;
                 int code                   =getDataCode(new URL(post));
                 String openColorTag        = "";
                 String endColorTag         = "";
                 ArrayList<String> pageId   = new ArrayList<String>();
                  if(code==200) {
                      stmt                  = transconn.prepareStatement(selectPage);
                      stmt.setString(1, userId);
                      rsDetails             = stmt.executeQuery();
                      while(rsDetails.next()) { 
                          String page     = rsDetails.getString(1);
                          String location  = rsDetails.getString(2);
                          String locationName = rsDetails.getString(3);
                          String pageName       = "";
                          post ="https://graph.facebook.com/fql?q=SELECT+page_id%2c+name%2C+pic%2C+access_token%2c+fan_count+FROM+page+WHERE+page_id+IN+("+page+")+&"+token;
                          //logger.debug(post);
                          String graph                = getData(new URL(post));
                          JSONObject json             = new JSONObject(graph);
                          JSONArray fa1               = new JSONArray(String.valueOf(json.get("data")));
                          for(int i=0;i<fa1.length();i++) {
                              String accesstoken      = String.valueOf(fa1.getJSONObject(i).get("access_token"));
                              if(!accesstoken.equals("null")) {
                                  String pageid       = fa1.getJSONObject(i).getString("page_id");
                                  if(pageid.equals(page)) {
                                      pageName        =   fa1.getJSONObject(i).getString("name");
                                      
                                  }
                              }                    
                          }
                          if(pageName!=null && !pageName.equals("")){
                           sbPage.append("<tr><td width=\"30%\"  style=\"font-size:small;\" align=\"left\">"+openColorTag+userId+"-"+userName+endColorTag+"</td>"
                        + "<td width=\"30%\" style=\"font-size:small;\" align=\"left\">"+openColorTag+pageName+endColorTag+"</td>"
                        + "<td align=\"center\" style=\"font-size:small;\" width=\"20%\">"+openColorTag+location+endColorTag+"</td>"
                        + "<td align=\"left\" style=\"font-size:small;\" width=\"20%\">"+openColorTag+locationName+endColorTag+"</td></tr> ");
                          }
                          
                          
                      }
                      
                     
                     
                 } else {
                     openColorTag           = "<font color=\"RED\">";
                     endColorTag            = "</font>";
                     
                 }
                 //logger.debug("userID:"+userId + " userName:" +userName +" Status:"+code);
                  sb.append("<tr><td width=\"30%\"  style=\"font-size:small;\" align=\"left\">"+openColorTag+userId+endColorTag+"</td>"
                        + "<td width=\"30%\" style=\"font-size:small;\" align=\"left\">"+openColorTag+userName+endColorTag+"</td>"
                        + "<td align=\"center\" style=\"font-size:small;\" width=\"20%\">"+openColorTag+email+endColorTag+"</td>"
                        + "<td align=\"left\" style=\"font-size:small;\" width=\"20%\">"+openColorTag+code+endColorTag+"</td></tr> ");
                
                 
                
             }
             sbPage.append("</table>");
             
              sb.append("</table>");              
              sb.append(sbPage.toString());
              sb.append("<tr><td>&nbsp;</td></tr>");
              sb.append("</td></tr>");
            sendMail("", "tech@beerboard.com", "suba@beerboard.com", "", "Check Facebook Token", "sendMail", sb, false);
             
             } catch (Exception e) {
             e.printStackTrace();
         } finally {           
             close(rsDetails);
             close(rs);
             close(stmt);
            }
	}
     public void sendMail(String title, String senderAddr, String emailAddr, String supportEmailAddr, String templateMessageTitle,
        String templateMessage, StringBuilder emailBody, boolean sendBCC) {
        String emailTemplatePath            = HandlerUtils.getSetting("email.templatePath");
        if ((emailTemplatePath == null) || "".equals(emailTemplatePath)) {
            emailTemplatePath               = ".";
        }
        //logger.debug("Packaging Email");
       // logger.debug(""+emailBody);
       /**/ try {
            if ((emailBody != null) && (emailBody.length() > 0)) {
                logger.debug("Loading Template");
                TemplatedMessage poEmail   = new TemplatedMessage(templateMessageTitle, emailTemplatePath, templateMessage);
                poEmail.setSender(senderAddr);
                poEmail.setRecipient(emailAddr);
                if (sendBCC) {
                    poEmail.setRecipientBCC(supportEmailAddr);
                }
                poEmail.setField("TITLE", title);
                poEmail.setField("BODY", emailBody.toString());
                poEmail.send();
            }
        } catch (MailException me) {
            logger.dbError("Error sending message to " + emailAddr + ": " + me.toString());
        }
    }
   
    
    
     private void signHttpsCertificate(){
        TrustManager[] trustAllCerts        = new TrustManager[]{new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers(){return null;}
            public void checkClientTrusted(X509Certificate[] certs, String authType){}
            public void checkServerTrusted(X509Certificate[] certs, String authType){}
        }};
        
        // Install the all-trusting trust manager
        try {
            SSLContext sc                   = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
           logger.debug(e.getMessage());
        }
    }
     
     
   
}

