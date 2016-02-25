/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.terakeet.util;

/**
 *
 * @author suba
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.*;

public class BreadCrumb {
    private MidwareLogger logger;
    private static SimpleDateFormat dbDateFormat
                                            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public String getItemsData(String username, String password) {
        logger                              = new MidwareLogger(BreadCrumb.class.getName());
        Calendar currentDate                = Calendar.getInstance();
        currentDate.add(Calendar.DATE, -1);
        String date                         = dbDateFormat.format(currentDate.getTime());
        String res                          = "";
        //String stringUrl                    = "https://api.breadcrumb.com/ws/v2/items.json";
        String stringUrl                    = "https://api.breadcrumb.com/ws/v2/menuitems.json";
        try {
            //logger.debug("Request URL:" + stringUrl);
            URL url                         = new URL(stringUrl);
            URLConnection conn              = url.openConnection();
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
            conn.setRequestProperty("X-Breadcrumb-API-Key", "2637da95f922b81bb6635debea54a5d7");
            conn.setRequestProperty("X-Breadcrumb-Username", username);
            conn.setRequestProperty("X-Breadcrumb-Password", password);
            conn.connect();
            BufferedReader serverResponse   = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            res                             = serverResponse.readLine();
            serverResponse.close();
        } catch (IOException e) {
            logger.debug("BreadCrumb IO:"+e.getMessage());
        }
        return res;
    }
    
    public String getCategoryData(String username, String password) {
        logger                              = new MidwareLogger(BreadCrumb.class.getName());
        Calendar currentDate                = Calendar.getInstance();
        currentDate.add(Calendar.DATE, -1);
        String date                         = dbDateFormat.format(currentDate.getTime());
        String res                          = "";
        String stringUrl                    = "https://api.breadcrumb.com/ws/v2/categories.json";
        try {
            //logger.debug("Request URL:" + stringUrl);
            URL url                         = new URL(stringUrl);
            URLConnection conn              = url.openConnection();
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
            conn.setRequestProperty("X-Breadcrumb-API-Key", "2637da95f922b81bb6635debea54a5d7");
            conn.setRequestProperty("X-Breadcrumb-Username", username);
            conn.setRequestProperty("X-Breadcrumb-Password", password);
            conn.connect();
            BufferedReader serverResponse   = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            res                             = serverResponse.readLine();
            serverResponse.close();
        } catch (IOException e) {
            logger.debug("BreadCrumb IO:"+e.getMessage());
        }
        return res;
    }

    public String getTradingDayData(String username, String password) {
        logger                              = new MidwareLogger(BreadCrumb.class.getName());
        Calendar currentDate                = Calendar.getInstance();
        currentDate.add(Calendar.DATE, -1);
        String date                         = dbDateFormat.format(currentDate.getTime());
        String res                          = "";
        String stringUrl                    = "https://api.breadcrumb.com/ws/v2/trading_days.json?limit=10&offset=190";
        try {
            //logger.debug("Request URL:" + stringUrl);
            URL url                         = new URL(stringUrl);
            URLConnection conn              = url.openConnection();
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
            conn.setRequestProperty("X-Breadcrumb-API-Key", "2637da95f922b81bb6635debea54a5d7");
            conn.setRequestProperty("X-Breadcrumb-Username", username);
            conn.setRequestProperty("X-Breadcrumb-Password", password);
            conn.connect();
            BufferedReader serverResponse   = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            res                             = serverResponse.readLine();
            serverResponse.close();
        } catch (IOException e) {
            logger.debug("BreadCrumb IO:"+e.getMessage());
        }
        return res;
    }
    
    public String getCheckData(String username, String password, String date) {
        logger                              = new MidwareLogger(BreadCrumb.class.getName());
        //logger.debug("D: " + date);
        String res                          = "";
        String stringUrl                    = "https://api.breadcrumb.com/ws/v2/checks.json?start=" + date;
        try {
            //logger.debug("Request URL:" + stringUrl);
            URL url                         = new URL(stringUrl);
            URLConnection conn              = url.openConnection();
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
            conn.setRequestProperty("X-Breadcrumb-API-Key", "2637da95f922b81bb6635debea54a5d7");
            conn.setRequestProperty("X-Breadcrumb-Username", username);
            conn.setRequestProperty("X-Breadcrumb-Password", password);
            conn.connect();
            BufferedReader serverResponse   = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            res                             = serverResponse.readLine();
            serverResponse.close();            
        } catch (IOException e) {
            logger.debug("BreadCrumb IO:"+e.getMessage());
        }
        return res;
    }
}
