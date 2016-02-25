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
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;



public class BOSSClancy {
    
    private net.terakeet.util.MidwareLogger logger;

    public String getOAuthToken() {
        logger                              = new net.terakeet.util.MidwareLogger(net.terakeet.util.BreadCrumb.class.getName());
        URL url;
        HttpURLConnection connection        = null;  
        String data;
        try {
    //username=user@boss.ie&password=*Vision1&grant_type=password
            data = URLEncoder.encode("username", "UTF-8") + "=" + URLEncoder.encode("user@boss.ie", "UTF-8");
            data += "&" + URLEncoder.encode("password", "UTF-8") + "=" + URLEncoder.encode("*Vision1", "UTF-8");
            data += "&" + URLEncoder.encode("grant_type", "UTF-8") + "=" + URLEncoder.encode("password", "UTF-8");
            url = new URL("http://clancybosswebapi.azurewebsites.net/token");
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
            connection.setRequestProperty("Content-Length", "" + 
                    Integer.toString(data.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US"); 
            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
    //Send request
            DataOutputStream wr             = new DataOutputStream (connection.getOutputStream ());
            wr.writeBytes (data);
            wr.flush ();
            wr.close ();
            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer(); 
            while((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            return response.toString();
        } catch (Exception e) {
             return "";
        } finally {
            if(connection != null) {
                connection.disconnect(); 
            }
        }
    }
    
     public String getSalesMaxId(String token) {         
         try {
    	   URL url                          = new URL("http://clancybosswebapi.azurewebsites.net/api/DetailArticleSalesMaxId");
           URLConnection conn = url.openConnection();	        	
           conn.setAllowUserInteraction(false);       
           conn.setDoOutput(true); 
           conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
           conn.addRequestProperty("Authorization", "Bearer " + token);
           conn.connect();
           BufferedReader serverResponse = new BufferedReader(new InputStreamReader(conn.getInputStream()));           
           String line;
            StringBuffer response = new StringBuffer(); 
            while((line = serverResponse.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
           serverResponse.close();
           return response.toString();
         } catch (Exception e) { }
          return "";
   }
     
     
    public String getSales(int locationId,int startId, int endId, String token) {         
         try {
    	   URL url                          = new URL("http://clancybosswebapi.azurewebsites.net/api/DetailArticleSales?restguid=C08B5CC8-383D-4FC5-8D1C-0B5C4ADB5A45&locationid="+locationId+"&startid="+startId+"&endid="+endId);
           URLConnection conn = url.openConnection();	        	
           conn.setAllowUserInteraction(false);       
           conn.setDoOutput(true); 
           conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
           conn.addRequestProperty("Authorization","Bearer "+token);
           conn.connect();
           BufferedReader serverResponse = new BufferedReader(new InputStreamReader(conn.getInputStream()));           
           String line;
            StringBuffer response = new StringBuffer(); 
            while((line = serverResponse.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
           serverResponse.close();
           return response.toString();
         }catch (Exception e) {
        	 e.printStackTrace();
          return "";
       }
       

   }
    
}
