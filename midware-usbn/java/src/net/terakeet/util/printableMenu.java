/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.terakeet.util;


import java.io.File;
import java.net.URL;
import java.sql.*;
import java.text.BreakIterator;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import net.terakeet.soapware.DatabaseConnectionManager;
import net.terakeet.soapware.HandlerUtils;
import net.terakeet.soapware.RegisteredConnection;
import net.terakeet.soapware.handlers.BeerBoardHandler;
import net.terakeet.soapware.*;
import org.dom4j.Element;

/**
 *
 * @author suba
 */
public class printableMenu {    
    private static SimpleDateFormat dateFormat
                                            = new SimpleDateFormat("MM/dd/yyyy");
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
     
     public  ArrayList<String> getBottledTemplate3(int beerType,int location,int customer,int count, String color, String logo,Element toAppend, MidwareLogger logger, RegisteredConnection transconn  ) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";


         try {
             String selectDraftBeer         = "SELECT  sPS.name,p.name,IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),IF(pD.ibu = 0.0, '', pD.ibu) FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + "  LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + " LEFT JOIN productSetMap sPSM ON sPSM.product = l.product LEFT JOIN productSet sPS ON sPS.id = sPSM.productSet"
                                            + "  WHERE l.status = 'RUNNING' AND b.location = ?  AND sPS.productSetType = 9 AND p.id NOT IN(4311,10661)  GROUP BY l.product ORDER BY sPS.name, pD.boardName, pD.category ;";
             String selectBottleBeer        = "SELECT  pS.name,p.name,IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),IF(pD.ibu = 0.0, '', pD.ibu) FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id  LEFT JOIN productSetMap pSM ON pSM.product=p.id LEFT JOIN productSet pS ON pS.id=pSM.productSet "
                                            + " WHERE location=? AND pS.productSetType=9 AND p.id NOT IN(4311,10661) ORDER by pS.name;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";
             String style3                  = "style=\" color:#000000; font-family:'serif'; font-size:10px; font-weight:bold; \"";
             String style4                  = "";
             if(count == 20) {
                // logger.debug("Loading S1");
                  style1                    = "style=\" color: "+color+"; font-family:'arial'; font-size:20px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style2                    = "style=\" color: "+color+"; font-family:'serif'; font-size:24px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style4                    = "style=\" color: "+color+"; font-family: Calibri; font-size:18px; font-weight:bold;    padding:5px; text-align:center;\"";
             } else if(count >= 30){
                 //logger.debug("Loading S2");
                  style1                    = "style=\" color: "+color+"; font-family:'arial'; font-size:16px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style2                    = "style=\" color: "+color+"; font-family:'serif'; font-size:20px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style4                    = "style=\" color: "+color+"; font-family: Calibri; font-size:26px; font-weight:bold;    padding:5px; text-align:center;\"";
             } else if(count == 36){
                 //logger.debug("Loading S3");
                  style1                    = "style=\" color: "+color+"; font-family:'arial'; font-size:10px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style2                    = "style=\" color: "+color+"; font-family:'serif'; font-size:8px; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  style4                    = "style=\" color: "+color+"; font-family: Calibri; font-size:12px; font-weight:bold;    padding:5px; text-align:center;\"";
             }
             int prodCount                  = 0;

             stmt                           = transconn.prepareStatement(selectlogo);
             stmt.setInt(1,location);
             rs                             = stmt.executeQuery();
             if(rs.next()){
                 logo                       = HandlerUtils.nullToString(rs.getString(1), logo);
             }
             String pngFileName             = logo;
             logo                           = logo.replaceAll("\'", "%27").replaceAll(" ", "%20");
             String logoURL                 ="http://bevmanager.net/Images/Location_logo/"+logo;
            /* if(!logo.equals("USBNLogo.png") && logo.contains(".png")){
                 String fileName            = pngFileName.replaceAll(".png", ".jpg");
                 BeerBoardHandler bb        = new BeerBoardHandler();
                 //bb.saveImage("http://bevmanager.net/Images/Location_logo/"+logo, "/home/midware/"+pngFileName);
                 bb.convertPNGToJPG("http://bevmanager.net/Images/Location_logo/"+logo,"/home/midware/"+fileName,"#FFFFFF");
                 bb.fileUpload("/home/midware/"+fileName);
                 fileName                           = fileName.replaceAll("\'", "%27").replaceAll(" ", "%20");
                 logoURL                    = "http://social.usbeveragenet.com:8080/fileUploader/Images/"+fileName;
             }*/
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType ==1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String StyleName           = HandlerUtils.nullToEmpty(rs.getString(1));
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(3));
                 String ibu                 = HandlerUtils.nullToEmpty(rs.getString(4));


                // String style1              = "class=\"beer_list_header\"",style2="class=\"beer_list\"";
                 if(prodCount ==0){
                     prodDesc               = new StringBuilder();
                     prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"  width=\"700px\" border=\"0\">  <tr> "
                                            //+ " <td colspan=\"4\" "+style1+" >"+title+"</td> </tr>  <tr>"
                                           // + " <td colspan=\"4\" "+style1+" >Served Ice Cold!</td> </tr> <tr>"
                                            + " <td "+style1+"><u>Style</u></td>"
                                            + " <td "+style1+"><u>Brand</u></td>"
                                            + " <td "+style1+"><u>ABV%</u></td>"
                                            + " <td "+style1+"><u>IBU</u></td>"
                                            + " </tr>");
                      if(count ==20) {
                          prodDesc.append(" <tr><td  colspan=\"4\" height=\"5px\""+style2+"></td></tr>");
                      }
                 }
                  prodDesc.append(" <tr><td "+style2+">"+StyleName+"</td>"
                                            + " <td "+style2+">"+productName+"</td>"
                                            + " <td "+style2+">"+abv+"</td>"
                                            + " <td "+style2+">"+ibu+"</td></tr>");
                  if(count ==20) {
                      prodDesc.append(" <tr><td  colspan=\"4\" height=\"5px\""+style2+"></td></tr>");
                  }
                // logger.debug(productName+ ": "+prodCount);
                 prodCount++;

                 if (count ==prodCount){
                     prodDesc.append("</table>");
                     styleMenus.add(prodDesc.toString());
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                  prodDesc.append("</table>");
                  styleMenus.add(prodDesc.toString());
             }
             prodCount                      = 0;
             String closeTr                 = "";
             logger.debug("Menu Size:"+styleMenus.size());
             for(int i=0;i<styleMenus.size();i++) {
                 prodDesc                   = new StringBuilder();
                 prodDesc.append("<table width=\"1500px\" height=\"700px\" border=\"0\">" +
                                            "<tr><td width=\"700px\" valign=\"top\" align=\"left\">" +
                                            "<table width=\"700px\" border=\"0\"><tr><td width=\"75px\">" +
                                            "<img style=\"max-width:100px; max-height:75px; background-color:"+bgcolor+";\" src=\""+logoURL+"\"/></td>" +
                                            "<td width=\"50px\"></td><td align=\"center\" "+style4 +"> Ice Cold "+title+"</td></tr>" +
                                            "</table>" +
                                            "</td>" +
                                            "<td style=\" width=\"100px\";\" valign=\"top\"></td>" +
                                            "<td width=\"700px\" valign=\"top\" align=\"left\">" +
                                            "<table width=\"700px\" border=\"0\">" +
                                            "<tr><td width=\"75px\"><img style=\"max-width:100px; max-height:75px;  background-color:"+bgcolor+";\" src=\""+logoURL+"\"/></td>" +
                                            "<td width=\"50px\"></td><td align=\"center\" "+style4 +"> Ice Cold "+title+"</td></tr></table>" +
                                            "</td></tr>" +
                                            "<tr><td height=\"5px\" valign=\"top\" align=\"left\"></td>" +
                                            "<td height=\"5px\"valign=\"top\" align=\"left\"></td>" +
                                            "<td height=\"5px\" valign=\"top\" align=\"left\"></td></tr>" +
                                            "<tr><td width=\"700px\" height=\"700px\" valign=\"top\">"+styleMenus.get(i)+"</td>" +
                                            "<td style=\" width=\"100px\";\" height=\"700px\" valign=\"top\"></td>" +
                                            "<td width=\"700px\" height=\"700px\" valign=\"top\">"+styleMenus.get(i)+"</td></tr>" +
                                            "<tr><td valign='bottom' align='right' ><hr/>" +
                                            "<table width='700px'><tr><td align='right'>" +
                                            "<img height='50px' align='right' src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Beerboardqr80x80.png' /></td>" +
                                            "<td width=\"2px\"></td><td width='110px' align='right' "+style3+">Our Draft List is Mobile!<img width='100px' src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/beerboardlogo.png' /></td></tr>" +
                                            "</table>" +
                                            "</td>" +
                                            "<td style=\" width=\"100px\";\" valign=\"top\"></td>" +
                                            "<td valign='bottom' align='right'><hr/>" +
                                            "<table width='700px'>" +
                                            "<tr><td align='right'><img height='50px' align='right' src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Beerboardqr80x80.png' /></td>" +
                                            "<td width=\"2px\"></td><td width='110px' align='right' "+style3+">Our Draft List is Mobile!<img width='100px' src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/beerboardlogo.png' /></td></tr>" +
                                            "</table>" +
                                            "</td></tr></table>");
                 menus.add(prodDesc.toString());
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);
        }
        return menus;
     }



        public  ArrayList<String> getBottledTemplate6(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         HashMap<Integer, String> productCalories
                                            = new HashMap<Integer, String>();
         
         productCalories.put(1,"96");//MillerLight
         productCalories.put(3,"180");//Sam Boston Lager 180
         productCalories.put(21,"125");//Guinness 125
         productCalories.put(25,"110");//Bud Light 110
         productCalories.put(36,"164");//Blue Moon 164
         productCalories.put(40,"148");//Heineken 148
         productCalories.put(42,"99");//Yuengling 99 
         productCalories.put(52,"154");//Stella Artois 154
         productCalories.put(58,"138");//Newcastle 138
         productCalories.put(59,"100");//Coors Light 100
         productCalories.put(61,"145");//Budweiser 145
         productCalories.put(221,"130");//Dos Equis Lager 130
         productCalories.put(708,"167");//Shock Top 167 
         productCalories.put(2532,"190");//Angry Orchard 190
         productCalories.put(12,"153");//Labatt Blue 153 
         productCalories.put(2596,"147");//Founders All Day IPA
         productCalories.put(2992,"180");//Bell's Kalamazoo Stout 180 
         productCalories.put(1103,"210");//Bell two 210
         productCalories.put(8112,"144");// 'Cigar City End of Summer' 144
         productCalories.put(2596,"147");//Founders All Day IPA 147
         productCalories.put(3739,"165");//Redd's Apple Ale 165
         productCalories.put(3776,"138");//Redhook Gamechanger Ale 138
         productCalories.put(228,"175");//Sierra Nevada Pale Ale 175
         productCalories.put(770,"126");//Goose Island 312 Urban Wheat 126



         

         

         
         

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),  "
                                            + " IF(pD.abv = 0.0, '', CONCAT((pD.abv * 1.25 * 16), ' cal.')), "
                                            + " IF(pD.category=2 ,1,2 ) craft, l.product FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + "  LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661)  GROUP BY l.product ORDER BY craft, pD.category, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')) "
                                            + " IF(pD.abv = 0.0, '', CONCAT('Approx. ', (pD.abv * 1.25 * 16), ' cal. per pint')) "
                                            + "  FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id  "
                                            + " WHERE location=?  AND p.id NOT IN(4311,10661) ORDER by pD.category, p.name;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             
             if(rowCount > 30) {
                 count                      = 18;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             } else if(rowCount > 24){
                 count                      = 15;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:9pt;  word-wrap:break-word; text-align:left;\"";
             } else if( rowCount > 16){
                 count                      = 12;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:14pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";
             } else {
                 count                      = 8;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:16pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:11pt;  word-wrap:break-word; text-align:left;\"";
             }

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 String calories            = rs.getString(4);
                 int productId              = rs.getInt(6);
                 if(productCalories.get(productId)!=null && !productCalories.get(productId).equals("")) {
                     calories               = productCalories.get(productId) + " cal.";
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 = rs.getString(3) ;
                 if(calories!=null && !calories.equals("")) {
                     abv                    +=  " | " + calories;
                 }
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "Domestic";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "Craft";
                     } else if(category == 3) {
                         categoryName       = "Import";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"
                                 + bgcolor + "\" height=\"400px\"  width=\"90%\" border=\"0\"><tr> "
                                 + " <td "+style1+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+style1+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                 }
                 prodDesc.append(" <tr><td "+style2+">"+productName+"  "+abv+"</td> </tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
             String backgroundImage         = "Menu800.jpg";
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 prodDesc.append("<table width=\"100%\"><tr><td height=\"150px\">&nbsp;</td></tr><tr><td align=\"center\">");
                 prodDesc.append("<table background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/Menu800.jpg\" width=\"384px\" height=\"576px\">"
                         + "<tr><td height=\"40px\">&nbsp;</td><td>&nbsp;</td></tr>"
                         + "<tr><td height=\"500px\" width=\"50%\" valign=\"top\" align=\"center\">"+col1+"</td><td valign=\"top\" align=\"center\" width=\"50%\">"+col2+"</td></tr>");
                 prodDesc.append("</table></td></tr></table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt); 
        }
        return menus;
     }
       
       public ArrayList<String> getBottledTemplate5 (int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";


         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,IF(pD.abv = 0.0, '', CONCAT('(',pD.abv,'%)')),if (pD.category=2 ,1,2 ) craft  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + "  LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661)  GROUP BY l.product ORDER BY craft, pD.category, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')) FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id  "
                                            + " WHERE location=?  AND p.id NOT IN(4311,10661) ORDER by pD.category, p.name;";
             String selectlogo              = "SELECT logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             
             if(rowCount > 30) {
                 count                      = 18;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:23pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:15pt;  word-wrap:break-word; text-align:left;\"";
             } else if(rowCount > 24){
                 count                      = 15;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:25pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:17pt;  word-wrap:break-word; text-align:left;\"";
             } else if( rowCount > 16){
                 count                      = 12;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:27pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:20pt;  word-wrap:break-word; text-align:left;\"";
             } else {
                 count                      = 8;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:30pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:25pt;  word-wrap:break-word; text-align:left;\"";
             }

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(3));
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "Domestic";
                     } else if(category == 2) {
                         categoryName       = "Craft";
                     } else if(category == 3) {
                         categoryName       = "Import";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\" height=\"850px\"  width=\"90%\" border=\"0\"><tr> "
                                 + " <td "+style1+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+style1+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                     oldCategory            = category;
                 }
                 prodDesc.append(" <tr><td "+style2+">"+productName+"  "+abv+"</td> </tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"10px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
             String backgroundImage         = "Menu.jpg";
             if (customer == 66) {
                 backgroundImage            = "Menu800.jpg";
             }
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 prodDesc.append("<table background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/"+backgroundImage+"\" width=\"800px\" height=\"1200px\">"
                         + "<tr><td height=\"100px\">&nbsp;</td><td>&nbsp;</td></tr><tr>"
                         + "<td height=\"900px\" width=\"50%\" valign=\"top\" align=\"center\">"+col1+"</td><td valign=\"top\" align=\"center\" width=\"50%\">"+col2+"</td></tr>");
                 if (customer != 66) {
                     prodDesc.append("<tr><td align=\"left\" valign=\"bottom\">" +
                             "<img style=\" max-height:200px; max-width:300px;\" src=\""+logo+"\"/></td><td align=\"right\" valign=\"bottom\">" +
                             "<img style=\" max-height:200px; max-width:300px;\" src=\"http://bevmanager.net/Images/Assets/Generic/beerboardlogo.png\"/></td></tr></table>");
                 } else {
                     prodDesc.append("<tr><td height=\"200px\">&nbsp;</td><td>&nbsp;</td></tr>");
                 }
                 prodDesc.append("</table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);  
        }
        return menus;
     }




        public  ArrayList<String> getBottledTemplate4(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("Template 400x600dpi loading");

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,IF(pD.abv = 0.0, '', CONCAT('(',pD.abv,'%)')),if (pD.category=2 ,1,2 ) craft  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + "  LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY craft, pD.category, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')),if (pD.category=2 ,1,2 ) craft FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id  "
                                            + " WHERE location=?  AND p.id NOT IN(4311,10661) ORDER BY craft, pD.category, p.name ;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             
             if(rowCount > 30) {
                 count                      = 18;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             } else if(rowCount > 24){
                 count                      = 15;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:9pt;  word-wrap:break-word; text-align:left;\"";
             } else if( rowCount > 16){
                 count                      = 12;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:14pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";
             } else {
                 count                      = 8;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:16pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:11pt;  word-wrap:break-word; text-align:left;\"";
             }

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(3));
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "Domestic";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "Craft";
                     } else if(category == 3) {
                         categoryName       = "Import";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\" height=\"400px\"  width=\"90%\" border=\"0\"><tr> "
                                 + " <td "+style1+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+style1+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                 }
                 prodDesc.append(" <tr><td "+style2+">"+productName+"  "+abv+"</td> </tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
             String backgroundImage         = "Menu800.jpg"; 
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 prodDesc.append("<table width=\"100%\"><tr><td height=\"150px\">&nbsp;</td></tr><tr><td align=\"center\">");
                 prodDesc.append("<table background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/Menu800.jpg\" width=\"384px\" height=\"576px\">"
                         + "<tr><td height=\"40px\">&nbsp;</td><td>&nbsp;</td></tr>"
                         + "<tr><td height=\"500px\" width=\"50%\" valign=\"top\" align=\"center\">"+col1+"</td><td valign=\"top\" align=\"center\" width=\"50%\">"+col2+"</td></tr>");
                  
                 prodDesc.append("</table></td></tr></table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
        
        
         public  ArrayList<String> getBWWTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("Template 550x800dpi loading");

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,pD.abv,if (pD.category=2 ,1,2 ) craft, pD.ibu, pSD.fgMax,fgMin  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + "  LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN brewStyleMap bSM ON bSM.product=p.id  "
                                            + " LEFT JOIN productSet pS ON pS.id=bSM.style LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id  WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY craft, pD.category, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,pD.abv,if (pD.category=2 ,1,2 ) craft, pD.ibu, pSD.fgMax,fgMin  FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN brewStyleMap bSM ON bSM.product=p.id  "
                                            + " LEFT JOIN productSet pS ON pS.id=bSM.style LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id WHERE location=?  AND p.id NOT IN(4311,10661)  GROUP BY b.product ORDER BY craft, pD.category, p.name ;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             String style3                  = "";
             if(rowCount > 30) {
                 count                      = 18;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:12pt;  word-wrap:break-word; text-align:left;\"";
                 style3                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:9pt;  word-wrap:break-word; text-align:left;\"";
             } else if(rowCount > 24){
                 count                      = 15;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:16pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:12pt;  word-wrap:break-word; text-align:left;\"";
                 style3                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:9pt;  word-wrap:break-word; text-align:left;\"";
             } else if( rowCount > 16){
                 count                      = 12;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:18pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:14pt;  word-wrap:break-word; text-align:left;\"";
                 style3                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";
             } else {
                 count                      = 8;
                 style1                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:18pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:14pt;  word-wrap:break-word; text-align:left;\"";
                 style3                     = "style=\" color: "+color+"; font-family:'TradeGothic'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";
             }

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 float abv                  = rs.getFloat(3);
                 double fgMax               = rs.getDouble(6);
                 double fgMin               = rs.getDouble(7);
                  if(fgMin == 0) {
                     fgMin                  = 1.01;
                 }
                  double OG                 = (abv / 131.25) + fgMax;
                 double calories            = 3621 * fgMax * (((0.8114 * fgMax) + (0.1886 * OG) - 1) + (0.53 * ((OG - fgMax) / (1.775 - OG))));
                 String abvStr              ="";
                 if(abv>0) {
                     abvStr                 =String.valueOf(abv)+"%";
                 }
                 String calStr              = "";
                 if(calories > 0) {
                     //String result          = String.format("%.2f", calories);
                      Long L = Math.round(calories);
                       int c = Integer.valueOf(L.intValue());
                       String result      = String.valueOf(c);
                     //logger.debug("Calories:"+ result);
                     if(abv > 0) {
                        calStr              = " | " ;
                     }
                     calStr                 += result+" <span "+style3+">CALS</span>";
                 }
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "Domestic";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "Craft";
                     } else if(category == 3) {
                         categoryName       = "Import";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" height=\"600px\"   width=\"90%\" border=\"0\"><tr> "
                                 + " <td "+style1+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+style1+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                 }
                 prodDesc.append(" <tr><td "+style2+">"+productName+"  "+abv+" "+calStr+"</td> </tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 for(int i=prodCount; i<count;i++) {
                      prodDesc.append(" <tr><td "+style1+" height=\"30px\"></td> </tr>");
                 }
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
             String backgroundImage         = "Menu800.jpg"; 
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 prodDesc.append("<table width=\"100%\"><tr><td align=\"center\">");
                 prodDesc.append("<table  background=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/Menu550x800in.png\" style=\"width: 5.5in; height:8in\">"
                         + "<tr><td height=\"50px\">&nbsp;</td><td>&nbsp;</td></tr>"
                         + "<tr><td height=\"700px\" width=\"50%\" valign=\"top\" align=\"center\">"+col1+"</td><td valign=\"top\" align=\"center\" width=\"50%\">"+col2+"</td></tr>");
                  
                 prodDesc.append("</table></td></tr></table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
        
        
        
        public  ArrayList<String> getDescriptionTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                              = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("Description Template");
         try {
             
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY pS.name, p.name;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";
             String selectBottleBeer        = "SELECT  DISTINCT  bS.majorStyle, pS.name,IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), "
                                            + " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description),   p.id,price FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "  LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style "
                                            + "  LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location  WHERE b.location= ? AND p.id NOT IN(4311,10661) ORDER BY  pS.name, p.name;";
             
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";
             String style3                  = "";
             String style4                  = "";

             stmt                           = transconn.prepareStatement(selectlogo);
             stmt.setInt(1,location);
             rs                             = stmt.executeQuery();
             if(rs.next()){
                 logo                       = HandlerUtils.nullToString(rs.getString(1), logo);
             }
             logo                           = "http://bevmanager.net/Images/Location_logo/"+logo.replaceAll(" ", "%20");
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             String nowontap                = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/NowOnTap.jpg";
             String bottles                 = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/bottles.png";
             String blackline               = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BlackLine.jpg";
             int prodCount                  = 0;
            
             String styleHeader             = "style=\" color:#000000; font-family:'Avantgarde'; font-size:20pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style1                         = "style=\" color: #000000; font-family:'Avantgarde'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                         = "style=\" color: #000000; font-family:'Avantgarde'; font-size:11pt;  word-wrap:break-word; text-align:left;\"";
             style4                         = "style=\" color: #000000; font-family:'Avantgarde'; font-size:11pt;  word-wrap:break-word; text-align:right;\"";
             style3                         = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";

             StringBuilder menuDesc         = new StringBuilder();
             String title                   = "";                          
             if(beerType ==1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = bottles;
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = nowontap;
             }

             
             menuDesc                       = new StringBuilder();
             rs                             = stmt.executeQuery();
             String preStyle                = "";
             int tableRow                   = -1, pageCount = 1;
             while (rs.next()) {
                 if(tableRow == -1){
                     menuDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" width=\"100%\" border=\"0\"> ");
                     tableRow++;
                 }
                 
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 int product                = rs.getInt(6);                 
                 if (abv.length() > 1) {
                     abv                    = abv + " ABV";
                 }
                 StringBuilder prodDesc     = new StringBuilder();
                 
                BreakIterator bi            = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }

               
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               ="";
                 if(beerType ==1){  
                     double bPrice          = rs.getDouble(7);
                     if(bPrice > 0){
                         price              =" $"+bPrice;
                     }
                 } else {
                     price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                 prodDesc.append("<table style='width:350px; height: 130px;' border=\"0\"> <tr><td><div class='ptextfill' style='width:350px; height: 20px;'>"
                                             + "<span style=\" color: #000000; font-family:'Arial';  text-align: left; font-weight:bold; word-wrap:break-word; \">"+name+"</span></div></td></tr>"
                                             + "<tr><td><table width=\"100%\" border=\"0\"><tr><td " + style2 + ">" + style + " " + abv + "</td>" +
                                             "<td "+style4+">"+price+"</td></tr></table></td></tr>"
                                             + "<tr><td><div class='dtextfill' style='width:350px; height:70px;'>"
                                             + "<span style=\"  color: "+color+"; font-family:'Avantgarde'; text-align: justify; text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td></tr></table>");
                 /*if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                          menuDesc.append(" <td width=\"50%\"> </td></tr>");
                          prodCount          =0;
                          tableRow++;
                     }
                     menuDesc.append(" <tr><td colspan=\"2\"><div "+styleHeader+" align=\"center\">"+majorStyle+"</div></td></tr>");
                     menuDesc.append(" <tr><td colspan=\"2\"><div "+styleHeader+" align=\"center\"></div></td></tr>");
                     if(tableRow== 0) {
                         tableRow++;
                     }
                 }*/
                 if(prodCount == 0) {
                     menuDesc.append(" <tr><td valign=\"top\" width=\"50%\">"+prodDesc.toString()+"</td>");
                     prodCount++;

                 } else {
                     menuDesc.append(" <td valign=\"top\" width=\"50%\">"+prodDesc.toString()+"</td></tr>");
                     prodCount          =0;
                     tableRow++;

                 }

                 preStyle                   = majorStyle;

                 if(tableRow >= (pageCount == 1 ? 7 : 8)) {
                     if(prodCount > 0) {
                         menuDesc.append(" <td width=\"50%\"> </td></tr>");
                         prodCount          = 0;
                         tableRow++;
                     }
                     if (pageCount == 1) { menuDesc.append(" <tr><td width=\"50%\" style=\"height: 40px\">&nbsp;</td><td width=\"50%\"></td></tr>"); }
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     tableRow               = -1;
                     pageCount++;
                 }

             }

             if(tableRow > 0) {
                 if(prodCount > 0) {
                     menuDesc.append(" <td width=\"50%\">&nbsp;</td></tr>");
                     prodCount              = 0;
                     tableRow++;
                 }
                 menuDesc.append("</table>");
                 styleMenus.add(menuDesc.toString());
                 menuDesc                   = new StringBuilder();
                 tableRow                   = 0;
             }
             //logger.debug("Size:" + styleMenus.size());

             StringBuilder prodDesc          = new StringBuilder();
             for(int i = 0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {
                     prodDesc.append("<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js\"></script>"
                                             + "<script src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/js/jquery.textfill.min.js\"></script>"
                                             + "<script type=\"text/javascript\">$(document).ready(function() {"
                                             + "$('.dtextfill').textfill({ maxFontPixels: 12 });});</script>  <script type=\"text/javascript\">"
                                             + "$(document).ready(function(){$('.ptextfill').textfill({ maxFontPixels: 18 });});</script>");
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') format(\"truetype\"); }  "
                         + " </style>");
                 }
                 prodDesc.append("<table width=\"100%\">" +
                                            (i == 0 ?"<tr><td height='70px'><table width=\"100%\"><tr><td align=\"left\"><img src=\"" + logo + "\" style=\"max-height:70px; max-width:200px\"></td>" +
                                            "<td align=\"right\"><img src=\"" + title + "\" style=\"max-height:70px;\"></td></tr></table></td></tr>" : "") +
                                            "<tr><td height='20px'></td></tr>" +
                                            "<tr><td>"+styleMenus.get(i)+"</td></tr></table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        } finally {
             close(rs);
            close(stmt);
        }
        return menus;
    }

    public  ArrayList<String> getDescriptionFeatureTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                            = "#000000",
         String  bgcolor                    = "#FFFFFF";

         logger.debug("Description Kiabacca Feature Template");
         try {
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
             String SelectFeatureBeer       = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 1 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
             String selectBottleBeer        = "SELECT  DISTINCT  bS.majorStyle, pS.name,IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), "
                                            + " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description),   p.id,price FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "  LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style "
                                            + "  LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location  WHERE b.location= ? AND p.id NOT IN(4311,10661) ORDER BY bS.majorStyle;";
            
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "   GROUP BY i.product ORDER BY i.product, iP.value;";
             
            
             ArrayList<Integer> featuredBeers
                                            = new ArrayList<Integer>();
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
            
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";


             double prodCount                  = -1;
             
             count                      = 8;
             String styleHeader         = "style=\" color: "+color+"; font-family:'AvantgardeBold'; font-size:14pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName           = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style1                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             String style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
             String style4                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:right;\"";

             StringBuilder menuDesc         = new StringBuilder();
             String preStyle                = "";
             int tableRow                   = -1;
             menuDesc                       = new StringBuilder();
             boolean createTable            = true;
             double countLimit              = 12;
             if(beerType ==2){
             stmt                        = transconn.prepareStatement(SelectFeatureBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 int product                = rs.getInt(6);                 
                 StringBuilder prodDesc     = new StringBuilder();                                  
                 featuredBeers.add(product);
                BreakIterator bi = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index = 0;
		while (bi.next() != BreakIterator.DONE) {
		String sentence             =    desc.substring(index, bi.current());
		if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
		index = bi.current();
		//logger.debug("Sentence: "+ index+":"+ sentence.length());
		}
                desc                       = descClean;
                 if(desc.length() <4){
                     desc                   = "";
                 }
                 
                 

                
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+" >"+name+"</span><span "+ styleName+"> - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                         + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                         + " <td  "+style4+" width=\"150px\" align=\"right\"> "+price+"</td> </tr></table>");
                 if(createTable){
                     menuDesc.append("<table border=\"0\"><tr>"
                             + "<td width=\"35%\" ><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                     + "<td "+styleHeader+">Featured</td><td width=\"35%\"><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     createTable= false;
                     prodCount = prodCount+0.5;
                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                        //  menuDesc.append(" <td colspan=\"3\"> </td></tr>");

                     }
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     prodCount = prodCount+0.5;
                 }

                 menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"</td></tr>"
                         + "<tr><td colspan=\"3\"> <hr style=\"border-top: dashed 2px;\" /></td></tr>");
                 prodCount++;
                 preStyle                   = majorStyle;
                 if(prodCount >= countLimit) {

                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                 }
             }
            }
             
             String title                   = "";                          
             if(beerType ==1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottles & Cans";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Now On Tap";
             }

             int draftCount                 = -1;
            // stmt                           = transconn.prepareStatement(selectDraftBeer);
             //stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next() && !featuredBeers.contains(rs.getInt(6))) {
                String majorStyle           = HandlerUtils.nullToEmpty(rs.getString(1));
                String style                = HandlerUtils.nullToEmpty(rs.getString(2));
                String name                 = HandlerUtils.nullToEmpty(rs.getString(3));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(4));
                String desc                 = HandlerUtils.nullToEmpty(rs.getString(5));
                int product                 = rs.getInt(6);
                
                StringBuilder prodDesc      = new StringBuilder();
                
                BreakIterator bi            = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }
                
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               ="";
                 if(beerType ==1){  
                     double bPrice          = rs.getDouble(7);
                     if(bPrice > 0){
                         price              =" $"+bPrice;
                     }
                 } else {
                     price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+">"+name+" - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                         + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                         + " <td  width=\"150px\" "+style4+" align=\"right\"> "+price+"</td> </tr></table>");
                 if(prodCount == -1 || draftCount==-1){
                     if(createTable) {
                         menuDesc.append("<table border=\"0\">");
                         createTable = false;
                         prodCount++;
                     } else {
                          prodCount++;
                     }
                     menuDesc.append("<tr><td width=\"35%\"><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                     + "<td  "+styleHeader+">"+title+"</td><td width=\"35%\" ><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     //prodCount++;
                     tableRow++;
                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                          //menuDesc.append(" <td colspan=\"3\"> </td></tr>");
                           
                     }
                      prodCount = prodCount+0.5;
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     if(tableRow== 0) {
                         tableRow++;
                     }
                 }
                
                     menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"<hr style=\"border-top: dashed 2px;\" /></td></tr>");
                     prodCount++;
                     draftCount++;

                 

                 preStyle                   = majorStyle;
                 
                if(styleMenus.size() > 0) {
                     countLimit             = 13.5;
                 }

                 if(prodCount>=countLimit) {
                      
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                     preStyle               = "";
                 }



             }

             if(prodCount > -1) {
                  menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = 0;

             }
             logger.debug("Size:" + styleMenus.size());
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             StringBuilder prodDesc                   = new StringBuilder();
             int size                       = styleMenus.size();
             for(int i=0; i < styleMenus.size(); i++) {
                  prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {                     
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); } "
                              + "@font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); } "
                         + " </style>");
                 }
                
                 prodDesc.append("<div><div style=\"vertical-align: top;\"><table   border=\"0\" width=\"100%\">");
                  if(i ==0  ) {
                      prodDesc.append(" <tr><td align=\"left\"><img src=\""+logo+"\" style=\"max-height:70px; max-width:200px\"></td></tr>");
                  } else {
                      
                  }
                  prodDesc.append("<tr> <td>"+styleMenus.get(i)+"</td></tr>"); 
                       
                         if(i ==0) { 
                             //prodDesc.append("<tr><td align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr></table>");
                             //prodDesc.append("<div  style=\"position:absolute; float:right; right:20px; bottom:10px; z-index:2;\" align=\"right\" "+style3+">Our draft list is mobile<img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div>");                            
                               prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         } else {
                             // prodDesc.append(" <tr><td height=\"150px\" ></td></tr>");
                              prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         }


                 menus.add(prodDesc.toString());
                // logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);
        }
        return menus;
     }

    public  ArrayList<String> getPrintableMenuTemplate9(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                            = "#000000",
         String  bgcolor                    = "#FFFFFF";

         logger.debug("Kiabacca Feature Template");
         try {
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
              String SelectFeatureBeer       = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 1 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "   GROUP BY i.product ORDER BY i.product, iP.value;";
              String selectBottleBeer        = "SELECT  DISTINCT  bS.majorStyle, pS.name,p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pDE.description, p.id, pSD.description,price FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN productDesc pDE ON pDE.product = p.id LEFT JOIN brewStyleMap bM ON bM.product = p.id   LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style"
                                            + " LEFT JOIN beerStyles bS ON bS.id =  bSM.style LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id WHERE location=? AND p.id NOT IN(4311,10661) ORDER BY pS.name, p.name;";


             ArrayList<Integer> featuredBeers
                                            = new ArrayList<Integer>();
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";


             int prodCount                  = -1;
             String title                   = "";
             count                      = 8;
             String styleHeader         = "style=\" color: "+color+"; font-family:'AvantgardeBold'; font-size:14pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName           = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String style1                  = "";
             String style2                  = "";
             String style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";


             StringBuilder menuDesc         = new StringBuilder();
             String preStyle                = "";
             int tableRow                   = -1;
             menuDesc                       = new StringBuilder();
             boolean createTable            = true;
             int countLimit                 = 10;
             stmt                        = transconn.prepareStatement(SelectFeatureBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 int product                = rs.getInt(6);                 
                 StringBuilder prodDesc     = new StringBuilder();
                 featuredBeers.add(product);
                BreakIterator bi = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }

                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 color                      = (price.contains("9oz") ? "#CC0000;" : "#000000;");
                 style1                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
                 styleName                  = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                 //logger.debug("c: " + color);
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+" >"+name+"</span><span "+ styleName+"> - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                                            + "<span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                                            + " <td  width=\"150px\" align=\"right\"><span style=\" color: "+color+"; \">" + price + "</span></td></tr></table>");
                 if(createTable){
                     menuDesc.append("<table border=\"0\"><tr>"
                                            + "<td width=\"35%\" ><hr style=\"height:10px;   border:none;color:#333; background-color:#333;\" /></td>"
                                            + "<td "+styleHeader+">Featured</td><td width=\"35%\"><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     createTable= false;
                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                        //  menuDesc.append(" <td colspan=\"3\"> </td></tr>");

                     }
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     //prodCount++;
                 }

                 menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"</td></tr>"
                         + "<tr><td colspan=\"3\"> <hr style=\"border-top: dashed 2px;\" /></td></tr>");
                 prodCount++;
                 preStyle                   = majorStyle;
                 if(prodCount >= 10) {

                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                 }
             }

             int draftCount                 = -1;
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next() && !featuredBeers.contains(rs.getInt(6))) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 int product                = rs.getInt(6);                 
                 StringBuilder prodDesc     = new StringBuilder();

                BreakIterator bi = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index = 0;
		while (bi.next() != BreakIterator.DONE) {
		String sentence             =    desc.substring(index, bi.current());
		if(bi.current() <250) {
                    descClean               +=sentence;
                }
		index = bi.current();
		//logger.debug("Sentence: "+ index+":"+ sentence.length());
		}
                desc                        = descClean;
                if(desc.length() <4){
                     desc                   = "";
                 }
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 color                      = (price.contains("9oz") ? "#CC0000" : "#000000");
                 style1                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
                 style2                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
                 styleName                  = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+">"+name+" - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                         + "<span style=\" color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                         + " <td  width=\"150px\" align=\"right\"> <span style=\" color: "+color+"; \">" + price + "</span></td> </tr></table>");
                 if(prodCount == -1 || draftCount==-1){
                     if(createTable) {
                         menuDesc.append("<table border=\"0\">");
                         createTable = false;
                         prodCount++;
                     } else {
                          prodCount++;
                     }
                     menuDesc.append("<tr><td width=\"35%\"><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                     + "<td  "+styleHeader+">Now ON TAP</td><td width=\"35%\" ><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     tableRow++;

                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                          //menuDesc.append(" <td colspan=\"3\"> </td></tr>");

                     }
                     // prodCount++;
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     if(tableRow== 0) {
                         tableRow++;
                     }
                 }

                     menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"<hr style=\"border-top: dashed 2px;\" /></td></tr>");
                     prodCount++;
                     draftCount++;



                 preStyle                   = majorStyle;

                 if(styleMenus.size() > 0) {
                     countLimit             = 12;
                 }

                 if(prodCount>=countLimit) {

                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                     preStyle               = "";
                 }



             }

             if(prodCount > -1) {
                  menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = 0;

             }
             logger.debug("Size:"+styleMenus.size());
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             StringBuilder prodDesc                   = new StringBuilder();
             int size                       = styleMenus.size();
             for(int i=0; i < styleMenus.size(); i++) {
                  prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); } "
                              + "@font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); } "
                         + " </style>");
                 }

                 prodDesc.append("<div><div style=\"vertical-align: top;\"><table   border=\"0\" width=\"100%\">");
                  if(i ==0  ) {
                      prodDesc.append(" <tr><td align=\"left\"><img src=\"" + logo + "\" style=\"max-height:70px; max-width:200px\"></td></tr>");
                  } else {

                  }
                  prodDesc.append("<tr> <td>"+styleMenus.get(i)+"</td></tr>");

                         if(i ==0) {
                             //prodDesc.append("<tr><td align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr></table>");
                             //prodDesc.append("<div  style=\"position:absolute; float:right; right:20px; bottom:10px; z-index:2;\" align=\"right\" "+style3+">Our draft list is mobile<img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div>");
                               prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         } else {
                             // prodDesc.append(" <tr><td height=\"150px\" ></td></tr>");
                              prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         }


                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
              close(rs);
            close(stmt); 
        }
        return menus;
     }
        
        
         public  ArrayList<String> getCWOCTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //String bgcolor                     = "#336699";
         color                              = "#000000";
         logger.debug("Template CW-OC loading");

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            +" LEFT JOIN brewStyleMap bM ON bM.product = l.product  LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY craft, pD.category DESC, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft   FROM bottleBeer b LEFT JOIN product p ON p.id=b.product "
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN brewStyleMap bM ON bM.product = b.product  LEFT JOIN productSet pS ON pS.id = bM.style WHERE location=?  AND p.id NOT IN(4311,10661) GROUP BY b.product ORDER BY craft, pD.category DESC, p.name;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             count                      = 30;
             style1                     = "style=\" color: "+color+"; font-family:'Arial'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleLeft           = "style=\" color: "+color+"; font-family:'Arial'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             String styleRight          = "style=\" color: "+color+"; font-family:'Arial'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:right;\"";
             String styleRight2         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:13pt;  word-wrap:break-word; text-align:right;\"";
             String style3              = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:13pt;  word-wrap:break-word; text-align:left;\"";
             String headStyle            = "style=\" color: "+color+"; font-family:'Arial'; font-size:22pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 ="";
                  float abvF                 =rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"%";
                 }
                 
                 String style               = HandlerUtils.nullToEmpty(rs.getString(4));
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(5));
                 if(style.contains("Do Not Know")){
                     style                  = "";
                 }
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "DOMESTIC";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "CRAFT";
                     } else if(category == 3) {
                         categoryName       = "IMPORT";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table valign=\"top\" width=\"700px\" cellspacing=\"0\" cellpadding=\"0\"  border=\"0\"><tr> "
                                 + " <td "+headStyle+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+headStyle+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                     prodDesc.append(" <tr><td width=\"40%\" align=\"left\" "+styleLeft+">Brand</td><td width=\"5%\">&nbsp;</td><td width=\"30%\" "+style1+">Style</td>"
                         + " <td width=\"5%\">&nbsp;</td><td width=\"10%\" "+style1+">Origin</td><td width=\"5%\">&nbsp;</td><td width=\"10%\" align=\"right\" "+styleRight+">ABV</td></tr>"
                         + "<tr>");
                     prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");
                 }
                 prodDesc.append(" <tr><td width=\"40%\"><div class='ptextfill' style='width:100%;height:20px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+productName+" </span></div></td><td width=\"5%\">&nbsp;</td>"
                         + "<td width=\"30%\" align=\"center\"><div class='ptextfill' style='width:100%;height:20px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+style+" </span></div></td>"
                         + " <td width=\"5%\">&nbsp;</td><td align=\"center\" width=\"10%\"><div class='ptextfill' style='width:100%;height:20px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+origin+"</span></div></td><td width=\"5%\">&nbsp;</td><td width=\"10%\" "+styleRight2+">"+abv+"</td></tr>"
                         + "<tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 for(int i=prodCount;i<count;i++){
                    //  prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                 }
                 
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
              style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
              String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 if(menus.size()==0) {
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); } "
                              + "@font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); } "
                         + " </style>");
                 }
                 prodDesc.append("<table  border=\"0\" height=\"1150px\" width=\"100%\"><tr><td align=\"center\" height=\"120px\"><img style=\" max-height:120px; max-width:300px;\" src=\""+logo+"\"/></td></tr>");
                 prodDesc.append("<tr><td height=\"1000px\"  valign=\"top\" align=\"center\">"+col1+"</td></tr>");                 
              
                 prodDesc.append("<tr><td height=\"20px\"  valign=\"top\" align=\"right\"><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></td></tr>");
                    prodDesc.append("</table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);
        }
        return menus;
     }
         
     public  ArrayList<String> getCWOCTemplate2(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //String bgcolor                     = "#336699";
         color                              = "#000000";
         logger.debug("Template CW-OC loading");

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft, p.id  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            +" LEFT JOIN brewStyleMap bM ON bM.product = l.product  LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY craft, pD.category DESC, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft ,p.id  FROM bottleBeer b LEFT JOIN product p ON p.id=b.product "
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN brewStyleMap bM ON bM.product = b.product  LEFT JOIN productSet pS ON pS.id = bM.style WHERE location=?  AND p.id NOT IN(4311,10661) GROUP BY b.product ORDER BY craft, pD.category DESC, p.name;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "   GROUP BY i.product ORDER BY i.product, iP.value;";

             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             count                      = 36;
             style1                     = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleLeft           = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             String styleRight          = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:right;\"";
             String styleRight2         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
             String style3              = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";
             String headStyle            = "style=\" color: "+color+"; font-family:'Arial'; font-size:18pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 int productId              = rs.getInt(7);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 ="";
                  float abvF                 =rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"%";
                 }
                 
                 String style               = HandlerUtils.nullToEmpty(rs.getString(4));
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(5));
                 if(style.contains("Do Not Know")){
                     style                  = "";
                 }
                 String price           = HandlerUtils.nullToEmpty(productPrices.get(productId));
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "DOMESTIC";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "CRAFT";
                     } else if(category == 3) {
                         categoryName       = "IMPORT";
                     }
                     
                     
                     if(prodCount == 0){
                         prodDesc.append("<table valign=\"top\" width=\"700px\" cellspacing=\"0\" cellpadding=\"0\"  border=\"0\"><tr> "
                                 + " <td "+headStyle+">"+categoryName+"</td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+headStyle+"><b>"+categoryName+"</b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                     prodDesc.append(" <tr><td width=\"30%\" align=\"left\" "+styleLeft+">Brand</td><td width=\"3%\">&nbsp;</td><td width=\"23%\" "+style1+">Style</td>"
                         + " <td width=\"3%\">&nbsp;</td><td width=\"10%\" "+style1+">Origin</td><td width=\"3%\">&nbsp;</td>"
                             + "<td width=\"10%\" "+style1+">ABV</td><td width=\"3%\">&nbsp;</td> <td width=\"10%\" align=\"center\" "+style1+">Price</td></tr>"
                         + "<tr>");
                     prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");
                 }
                 prodDesc.append(" <tr><td width=\"30%\"><div class='ptextfill' style='width:100%;height:15px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+productName+" </span></div></td><td width=\"3%\">&nbsp;</td>"
                         + "<td width=\"23%\" align=\"center\"><div class='ptextfill' style='width:100%;height:15px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+style+" </span></div></td>"
                         + " <td width=\"3%\">&nbsp;</td><td align=\"center\" width=\"10%\"><div class='ptextfill' style='width:100%;height:15px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+origin+"</span></div></td>"                         
                         + "<td width=\"3%\">&nbsp;</td><td width=\"10%\" "+styleRight2+">"+abv+"</td>"
                         + " <td width=\"3%\">&nbsp;</td><td align=\"center\" width=\"20%\"><div class='ptextfill' style='width:100%;height:15px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: right;    text-justify: inter-word; word-wrap:break-word; \">"+price+"</span></div></td></tr>"
                         + "<tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 for(int i=prodCount;i<count;i++){
                    //  prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                 }
                 
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
              style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
              String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 if(menus.size()==0) {
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url(http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf) format(\"truetype\"); } "
                              + "@font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); } "
                         + " </style>");
                 }
                 prodDesc.append("<table  border=\"0\" height=\"1150px\" width=\"100%\"><tr><td align=\"center\" height=\"120px\"><img style=\" max-height:120px; max-width:300px;\" src=\""+logo+"\"/></td></tr>");
                 prodDesc.append("<tr><td height=\"1000px\"  valign=\"top\" align=\"center\">"+col1+"</td></tr>");                 
              
                 prodDesc.append("<tr><td height=\"20px\"  valign=\"top\" align=\"right\"><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></td></tr>");
                    prodDesc.append("</table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);
        }
        return menus;
     }    
         
         
    public  ArrayList<String> getTemplate12(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("Template With Beer Map Style");

         try {
             String selectDraftBeer         = "SELECT bS.style, p.name,pD.abv, pD.origin, p.id, pS.name  FROM line l LEFT JOIN bar b ON b.id = l.bar "
                                            + "LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            + "LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style"
                                            + " LEFT JOIN beerStyles bS ON bS.id =  bSM.style"
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY bS.style, p.name ;";
              String selectBottleBeer       = "SELECT  bS.style, p.name,pD.abv, pD.origin, p.id, pS.name, price   FROM bottleBeer b LEFT JOIN product p ON p.id=b.product "
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style "
                                            + " LEFT JOIN beerStyles bS ON bS.id =  bSM.style WHERE location=?  AND p.id NOT IN(4311,10661) GROUP BY b.product ORDER BY bS.style, p.name;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";

             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             String oldStyle                = null;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
              count                      = 24;
              style1                     = "style=\" color: "+color+"; font-family:'AvantgardeBold'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
              style2                     = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:12pt;  word-wrap:break-word; text-align:left;\"";
              String style3              = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:left;\"";

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             int productHead                = 0;
             while (rs.next()) {
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 int product                = rs.getInt(5);
                 String price               = "";
                 if(beerType==1){
                     double bPrice          = rs.getDouble(7);
                     if(bPrice>0){
                         price              = " $"+bPrice;
                     }
                 } else {
                     price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                 
                 String styleAbvOrigin      = "";
                 
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                  String abv                 ="";
                  float abvF                 =rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"%";
                 }
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(4));
                 String styleName           = HandlerUtils.nullToEmpty(rs.getString(6));
                 
                 if(styleName!= null && !styleName.equals("")){
                     styleAbvOrigin         = styleName;
                 }
                 
                 if(!abv.equals("")){
                     if(!styleAbvOrigin.equals("")){
                         styleAbvOrigin         +=" - "+abv;
                     } else {
                         styleAbvOrigin         = abv;
                     }
                 }
                 
                 if(!origin.equals("")){
                     if(!styleAbvOrigin.equals("")){
                         styleAbvOrigin         +=" - "+origin;
                     } else {
                         styleAbvOrigin         = origin;
                     }
                 }
                 StringBuilder prodTable    = new StringBuilder();
                 prodTable.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"  border=\"0\">");
                 prodTable.append(" <tr><td><div class='ptextfill' style='width:100%;height:20px;'><span style=\" color: "+color+" font-family:'Avantgarde';  text-align: left; \">"+productName+"</span></div></td> </tr>");
                 prodTable.append(" <tr><td "+style3+">"+styleName+" - "+abv+" - "+origin+"</td> </tr>");
                 if(price!=null &&!price.equals("")){
                     //prodTable.append(" <tr><td "+style3+">"+price+"</td> </tr>");
                 }
                  prodTable.append("</table>");
                 
                 if(!brewStyle.equals(oldStyle) || prodCount == 0) {
                     String categoryName    = "";
                     
                     
                     if(prodCount == 0){
                         prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\" height=\"400px\"  width=\"90%\" border=\"0\"><tr> "
                                 + " <td "+style1+">"+brewStyle+" <hr style=\"border-top: dashed 2px;\" /></td> </tr>");
                         productHead++;
                        
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+style1+"><b> "+brewStyle+"</b> <hr style=\"border-top: dashed 2px;\" /></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldStyle           = brewStyle;
                      prodCount          =prodCount+2;
                     
                     
                 }
                 prodDesc.append(" <tr><td>"+prodTable.toString()+"</td> </tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"8px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                     productHead            = 0;
                 }
             }

             if(prodCount > 0){
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
                style3              = "style=\" color: #000000; font-family:'Avantgarde'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
              String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 if(menus.size()==0) {
                     /* prodDesc.append(" <style>"
                         + " @font-face {font-family: Avantgarde;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Avantgarde.ttf') format(\"truetype\"); } "
                              + "@font-face {font-family: AvantgardeBold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/avantgarde-bold.ttf') format(\"truetype\"); } "
                         + " </style>");*/
                 }
                 prodDesc.append("<table  border=\"0\" height=\"1100px\" width=\"100%\"><tr><td colspan=\"2\" valign=\"top\" align=\"center\" height=\"120px\"><img style=\" max-height:120px; max-width:300px;\" src=\""+logo+"\"/></td></tr>");
                 prodDesc.append("<tr><td height=\"980px\" width=\"50%\" valign=\"top\" align=\"center\">"+col1+"</td><td valign=\"top\" align=\"center\" width=\"50%\">"+col2+"</td></tr>");                  
                 //prodDesc.append("<tr><td colspan=\"2\" height=\"20px\"  valign=\"top\" align=\"right\"><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></td></tr>");
                 prodDesc.append("</table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
    
    
    public  ArrayList<String> getSupplyHouseTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("Supply House");

         try {
             
             String selectDraftBeer         = "SELECT  pS.name, IF(cBN.name IS NULL, p.name, cBN.name), pD.abv, " +
                                            " pD.origin, p.id, IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY  p.name;";
             String selectBottleBeer        = "SELECT p.name, pD.abv, b.type, CONCAT('$', b.price) FROM bottleBeer b " +
                                            " LEFT JOIN product p ON p.id = b.product LEFT JOIN productDescription pD ON pD.product=p.id  "
                                            + " WHERE location=?  AND p.id NOT IN(4311,10661) ORDER by b.type, p.name;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', round(iP.value)), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";
             
             
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             ArrayList<String> html         = getHtmlTemplate(1);
             
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);
             title                          = "Draft Beer";
             
             String oldStyle                = null;
             rs                             = stmt.executeQuery();
             count                          = 20;
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             int productHead                = 0;
             int index                      = 0;
             int totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 int product                = rs.getInt(5);
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 String styleAbvOrigin      = "";
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2)).toUpperCase();
                 
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV";
                 }                 
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(6));
                 
                 String[] descriptionArray  = desc.split("\\.");
                 String descClean           = "";
                 if (descriptionArray.length > 0) {
                     for (int i = 0; i < descriptionArray.length; i++) {
                        String sentence     = descriptionArray[i] + ".";
                        if ((descClean.length() + sentence.length()) < 200) {
                            descClean       += sentence;
                        }
                     }
                     //logger.debug("Desc Sentence: " + descClean);
                 }
                 desc                   = descClean;
                
                 
               
                 for( int i=index; i<html.size(); i++) {
                     String text            = html.get(i);
                    // logger.debug("Text:"+text);
                     if(text.contains("@@BEERNAME@@")){
                         text   = text.replace("@@NO@@", String.valueOf(totalProduct)+".");
                         text   = text.replace("@@BEERNAME@@", productName+" ");
                         text   = text.replace("@@ABV@@"," "+ abv);
                         text   = text.replace("@@BEERPRICE@@",price);
                         text   = text.replace("@@DESC@@", desc);
                         //logger.debug("text: " + text);
                         html.remove(i);
                         html.add(i, text);                         
                         break;
                     }
                 }

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     StringBuilder sb = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                         s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@DESC@@", "");
                         //s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     prodCount              = 0;
                     index                  = 0;
                 }
             }

             if(prodCount > 0){
                 StringBuilder sb = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                          s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@DESC@@", "");
                        // s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     prodCount              = 0;
                     index                  = 0;
                 
             }

             stmt                           = transconn.prepareStatement(selectBottleBeer);
             stmt.setInt(1, location);
             title                          = "Draft Beer";

             oldStyle                       = null;
             rs                             = stmt.executeQuery();
             count                          = 23;
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             productHead                    = 0;
             index                          = 0;
             totalProduct                   = 0;
             while (rs.next()) {
                 totalProduct++;
                 String price               = rs.getString(4);
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(1)).toUpperCase();

                 for(int i = index; i < html.size(); i++) {
                     String text            = html.get(i);
                     //logger.debug("Text:" + text);
                     if (rs.getInt(3) == 1) {
                        if(text.contains("@@BOTTLEBEERNAME@@")){
                             text           = text.replace("@@BOTTLEBEERNAME@@", productName+" ");
                             text           = text.replace("@@BTLBEERPRICE@@",   price);
                             html.remove(i);
                             html.add(i, text);
                             break;
                         }
                     } else {
                        if(text.contains("@@CANBEERNAME@@")){
                             text           = text.replace("@@CANBEERNAME@@", productName+" ");
                             text           = text.replace("@@CANBEERPRICE@@",price);
                             html.remove(i);
                             html.add(i, text);
                             break;
                         }
                     }
                 }
                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);
                 if (prodCount >= count){
                     StringBuilder sb       = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@CANBEERNAME@@", "");
                         s                  = s.replaceAll("...@@CANBEERPRICE@@", "");
                         s                  = s.replaceAll("@@BOTTLEBEERNAME@@", "");
                         s                  = s.replaceAll("...@@BTLBEERPRICE@@", "");
                         sb.append(s);

                     }
                     styleMenus.add(sb.toString());
                     prodCount              = 0;
                     index                  = 0;
                     html.clear();
                 }
             }

             if(prodCount > 0){
                 StringBuilder sb = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@CANBEERNAME@@", "");
                         s                  = s.replaceAll("...@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@BOTTLEBEERNAME@@", "");
                         s                  = s.replaceAll("...@@BEERPRICE@@", "");
                         sb.append(s);
                     }
                     styleMenus.add(sb.toString());
                     prodCount              = 0;
                     index                  = 0;
             }
             
             for( int i=0; i < styleMenus.size(); i++) {                
                 String col1                = styleMenus.get(i);                 
                 menus.add(col1);
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
    
    ArrayList<String> getHtmlTemplate(int type) {
         ArrayList<String> html = new ArrayList<String>();
        try {
            Scanner scanner = new Scanner(new File("/home/midware/pdf/template/supply.html"));
            if(type==1){
                scanner = new Scanner(new File("/home/midware/pdf/template/supply.html"));
            } else if(type==2){
                scanner = new Scanner(new File("/home/midware/pdf/template/BWW.html"));
            } else if(type==3){
                scanner = new Scanner(new File("/home/midware/pdf/template/BWWbottlecan.html"));
            }
            while(scanner.hasNextLine()) {
                html.add(scanner.nextLine());
            }
            scanner.close();
            for(int i=0;i<html.size();i++) {
			 System.out.println(html.get(i));
		 }
		 //String[] namesArr = (String[]) names.toArray();
		 } catch(Exception e ) {
			 e.printStackTrace();
		 }
        return  html;
    }
    
    ArrayList<String> getHtmlRemoteTemplate(String file) {
         ArrayList<String> html = new ArrayList<String>();
        try {
            URL url = new URL(file);
            Scanner scanner = new Scanner(url.openStream());
            while(scanner.hasNextLine()) {
                html.add(scanner.nextLine());
            }
            scanner.close();
            for(int i=0;i<html.size();i++) {
			 System.out.println(html.get(i));
		 }
		 //String[] namesArr = (String[]) names.toArray();
		 } catch(Exception e ) {
			 e.printStackTrace();
		 }
        return  html;
    }
    
    
    public  ArrayList<String> getBWWMenuTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#FFFFFF";
         logger.debug("BWW Menu HQ Test");

         try {
             
             
             String selectDraftBeer         = "SELECT pS.name, p.name, pD.abv, p.id, (SELECT name FROM customBeerName WHERE location = b.location AND product = p.id " +
                                            " LIMIT 1), pD.ibu, pD.calorie FROM line l LEFT JOIN bar b ON b.id = l.bar "
                                            + "LEFT JOIN product p ON p.id = l.product  LEFT JOIN productDescription pD ON pD.product = p.id  LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style"
                                            + " LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY  p.name ;";
             String selectBottleBeer        = "SELECT pS.name, p.name, pD.abv, b.type, CONCAT('$', ROUND(b.price, 2)), pD.ibu, pD.calorie, (SELECT name FROM customBeerName WHERE location = b.location AND product = p.id " +
                                            " LIMIT 1) FROM bottleBeer b  LEFT JOIN product p ON p.id = b.product LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + " LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style "
                                            + "  LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id   WHERE location=?  AND p.id NOT IN(4311,10661) ORDER by b.type, p.name;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', ROUND(iP.value, 2)), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            " GROUP BY i.product ORDER BY i.product, iP.value;";
             
             
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             ArrayList<String> html         = getHtmlRemoteTemplate("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/BWW/BWW.html");
             ArrayList<String> bottle       = getHtmlRemoteTemplate("http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/BWW/BWWbottlecan.html"); 
             String line                    = "<img width=\"400px\" src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/supplyline.jpg\" alt=\"\">";
             
             //ArrayList<String> html         = getHtmlTemplate(2);
             //ArrayList<String> bottle       = getHtmlTemplate(3);
             
             
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);         
             rs                             = stmt.executeQuery();             
             int index                      = 0;
             int totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 int product                = rs.getInt(4);
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 String styleIbuCal         = "";
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2)).toUpperCase();
                 String customName          = HandlerUtils.nullToEmpty(rs.getString(5)).toUpperCase();         
                 if(customName!=null && !customName.equals("")){
                     productName            = customName;
                 }                 
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV";
                 }       
                 int ibu                    = rs.getInt(6);
                 int calories               = rs.getInt(7);                 
                 String abvStr              ="";
                 if(abvF>0) {
                     abvStr                 =String.valueOf(abv)+"%";
                 }                
                 String result      = String.valueOf(calories);
                 String calStr              = "";
                 if(calories > 0) {                     
                     calStr                 += result+" <span class='cals2'>CALS</span>";
                 }
                 styleIbuCal                ="<span class='brewstyle'>"+brewStyle+" </span>";
                 if(ibu>0){
                     styleIbuCal            +="<span class='ibu'> "+ibu+" </span> <span class='cals2'>IBU</span>" ;
                     
                 }
                 styleIbuCal                +="<span class='cals1'> "+calStr+"</span>";        

                 for( int i=index; i<html.size(); i++) {
                     String text            = html.get(i);
                    // logger.debug("Text:"+text);
                     if(text.contains("@@BEERNAME@@")){
                         text   = text.replace("@@NO@@", String.valueOf(totalProduct)+".");
                         text   = text.replace("@@BEERNAME@@", productName+" ");
                         text   = text.replace("@@ABV@@"," "+ abv);
                         text   = text.replace("@@BEERPRICE@@",price);
                         text   = text.replace("@@StyleIBUCALS@@", styleIbuCal);
                         text   = text.replace("@@line@@", line);
                         //logger.debug("text: " + text);
                         html.remove(i);
                         html.add(i, text);                         
                         break;
                     }
                 }

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     StringBuilder sb = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                         s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@StyleIBUCALS@@", "");
                         s                  = s.replaceAll("@@line@@", "");
                         //s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     prodCount              = 0;
                     index                  = 0;
                 }
             }

             if(prodCount > 0){
                 StringBuilder sb = new StringBuilder();
                     for (String s : html)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                          s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@StyleIBUCALS@@", "");
                         s                  = s.replaceAll("@@line@@", "");
                        // s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     styleMenus.add(sb.toString());
                     prodCount              = 0;
                     index                  = 0;
                 
             }
             
             

            stmt                           = transconn.prepareStatement(selectBottleBeer);
             stmt.setInt(1, location);         
             rs                             = stmt.executeQuery();             
              index                      = 0;
              totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String price               = HandlerUtils.nullToEmpty(rs.getString(5));
                 String styleIbuCal         = "";
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2)).toUpperCase();                 
                 int type                   = rs.getInt(4);
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV";
                 }       
                 int ibu                    = rs.getInt(6);
                 int calories               = rs.getInt(7);
                 String customName          = HandlerUtils.nullToEmpty(rs.getString(8)).toUpperCase();
                 if(customName != null && !customName.equals("")){
                     productName            = customName;
                 }    
                 
                 String abvStr              ="";
                 if(abvF>0) {
                     abvStr                 =String.valueOf(abv)+"%";
                 }
                
                 String result      = String.valueOf(calories);
                 String calStr              = "";
                 if(calories > 0) {                     
                     calStr                 += result+" <span class='cals2'>CALS</span>";
                 }
                 styleIbuCal                ="<span class='brewstyle'>"+brewStyle+" </span>";
                 if(ibu>0){
                     styleIbuCal            +="<span class='ibu'> "+ibu+" </span> <span class='cals2'>IBU</span>" ;
                     
                 }
                 styleIbuCal                +="<span class='cals1'> "+calStr+"</span>";        

                 for( int i=index; i<bottle.size(); i++) {
                     String text            = bottle.get(i);
                    // logger.debug("Text:"+text);
                     if(text.contains("@@BEERNAME@@")){
                         text   = text.replace("@@NO@@", String.valueOf(totalProduct)+".");
                         text   = text.replace("@@BEERNAME@@", productName+" ");
                         text   = text.replace("@@ABV@@"," "+ abv);
                         text   = text.replace("@@BEERPRICE@@",price);
                         text   = text.replace("@@StyleIBUCALS@@", styleIbuCal);
                         text   = text.replace("@@line@@", line);
                         //logger.debug("text: " + text);
                         bottle.remove(i);
                         bottle.add(i, text);                         
                         break;
                     }
                 }

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     StringBuilder sb = new StringBuilder();
                     for (String s : bottle)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                         s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@StyleIBUCALS@@", "");
                         s                  = s.replaceAll("@@line@@", "");
                         //s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     prodCount              = 0;
                     index                  = 0;
                 }
             }

             if(prodCount > 0){
                 StringBuilder sb = new StringBuilder();
                     for (String s : bottle)
                     {
                         s                  = s.replaceAll("@@NO@@", "");
                         s                  = s.replaceAll("@@BEERNAME@@", "");
                          s                  = s.replaceAll("@@ABV@@", "");
                         s                  = s.replaceAll("@@BEERPRICE@@", "");
                         s                  = s.replaceAll("@@StyleIBUCALS@@", "");
                         s                  = s.replaceAll("@@line@@", "");
                        // s                  = s.replaceAll("@@CANBEERNAME@@...@@BEERPRICE@@", "");
                         //s                  = s.replaceAll("@@BOTTLEBEERNAME@@...@@BEERPRICE@@", "");
                         sb.append(s);
                     
                     }
                     styleMenus.add(sb.toString());
                     prodCount              = 0;
                     index                  = 0;
                 
             }
             
             for( int i=0; i < styleMenus.size(); i++) {                
                 String col1                = styleMenus.get(i);                 
                 menus.add(col1);
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
    
    public  ArrayList<String> getExtendedFeaturedTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                            = "#000000",
         String  bgcolor                    = "#FFFFFF";

         logger.debug("Description Extended Feature Template");
         try {
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), "
                                            + "  pS1.name, p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS1 ON pS1.id = bM.brewery  LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
             String SelectFeatureBeer       = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), "
                                            + " pS1.name,p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS1 ON pS1.id = bM.brewery  LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 1 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
             String SelectCaskBeer          = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), "
                                            + " pS1.name, p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS1 ON pS1.id = bM.brewery  LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 1 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyle;";
            String selectPrices             = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";

             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();

             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String selectlogo              = "SELECT logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = -1;
             String title                   = "";
             count                          = 16;
             String styleHeader             = "style=\" color: "+color+"; font-family:'AvantgardeBold'; font-size:14pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName               = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style1                         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";

             StringBuilder menuDesc         = new StringBuilder();
             String preStyle                = "";
             int tableRow                   = -1;
             menuDesc                       = new StringBuilder();
             boolean createTable            = true;
             int countLimit                 = 15;
             stmt                           = transconn.prepareStatement(SelectFeatureBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 String brewery             = HandlerUtils.nullToEmpty(rs.getString(6));
                 int product                = rs.getInt(7);
                 
                 StringBuilder prodDesc     = new StringBuilder();
                 BreakIterator bi           = BreakIterator.getSentenceInstance();
		 bi.setText(desc);
                 String descClean           = "";
		 int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
                 }
                 desc                       = descClean;
                 if(desc.length() <4){
                     desc                   = "";
                 }
                
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+" >"+name+"</span><span "+ styleName+"> - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                                             + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                                             + " <td  width=\"150px\" align=\"right\"> <span "+ style2+" >"+brewery+"</span><br/>"+price+"</td> </tr></table>");
                 if(createTable){
                     menuDesc.append("<table border=\"0\"><tr>"
                                            + "<td width=\"35%\" ><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                            + "<td "+styleHeader+">Featured</td><td width=\"35%\"><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     createTable            = false;
                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                 }

                 menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"</td></tr>"
                                            + "<tr><td colspan=\"3\"> <hr style=\"border-top: dashed 2px;\" /></td></tr>");
                 prodCount++;
                 preStyle                   = majorStyle;
                 if(prodCount >= 9) {
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount              = -1;
                     createTable            = true;
                 }
             }
             
             createTable                    = true;
             stmt                           = transconn.prepareStatement(SelectCaskBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 String brewery             = HandlerUtils.nullToEmpty(rs.getString(6));
                 int product                = rs.getInt(7);

                 StringBuilder prodDesc     = new StringBuilder();
                 BreakIterator bi           = BreakIterator.getSentenceInstance();
		 bi.setText(desc);
                 String descClean           = "";
		 int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() <4){
                     desc                   = "";
                 }
                 
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+" >"+name+"</span><span "+ styleName+"> - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                                             + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                                             + " <td  width=\"150px\" align=\"right\"> <span "+ style2+" >"+brewery+"</span><br/>"+price+"</td> </tr></table>");
                 if(createTable){
                     menuDesc.append("<table border=\"0\"><tr>"
                                            + "<td width=\"35%\" ><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                            + "<td "+styleHeader+">Cask Conditioned</td><td width=\"35%\"><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     createTable            = false;
                 }
                 
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                 }

                 menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"</td></tr>"
                                            + "<tr><td colspan=\"3\"> <hr style=\"border-top: dashed 2px;\" /></td></tr>");
                 prodCount++;
                 preStyle                   = majorStyle;
                 if(prodCount >= 9) {
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                 }
             }

             int draftCount                 = -1;
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 String brewery             = HandlerUtils.nullToEmpty(rs.getString(6));
                 int product                = rs.getInt(7);
                 StringBuilder prodDesc     = new StringBuilder();
                 
                 BreakIterator bi           = BreakIterator.getSentenceInstance();
		 bi.setText(desc);
                 String descClean           = "";
		 int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }
                 
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 prodDesc.append("<table border=\"0\"> <tr><td "+style1+" width=\"700px\"> <span "+ styleName+">"+name+" - </span> "+abv+" ABV<span "+ styleName+"> - </span>"+style+" <br/> <div class='dtextfill' style='width:100%;height:30px;'>"
                                             + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify;    text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                                             + " <td  width=\"150px\" align=\"right\"> <span "+ style2+" >"+brewery+"</span><br/>"+price+"</td> </tr></table>");
                 if(prodCount == -1 || draftCount==-1){
                     if(createTable) {
                         menuDesc.append("<table border=\"0\">");
                         createTable = false;
                         prodCount++;
                     } else {
                          prodCount++;
                     }
                     menuDesc.append("<tr><td width=\"35%\"><hr style=\"height:10px;   border:none;color:#333;background-color:#333;\" /></td>"
                                     + "<td  "+styleHeader+">Now ON TAP</td><td width=\"35%\" ><hr style=\"height:10px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     tableRow++;

                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                          //menuDesc.append(" <td colspan=\"3\"> </td></tr>");
                           
                     }
                     // prodCount++;
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     if(tableRow== 0) {
                         tableRow++;
                     }
                 }
                
                     menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"<hr style=\"border-top: dashed 2px;\" /></td></tr>");
                     prodCount++;
                     draftCount++;

                 

                 preStyle                   = majorStyle;
                 
                 if(styleMenus.size() > 0) {
                     countLimit             = 18;
                 }

                 if(prodCount>=countLimit) {
                      
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                     preStyle               = "";
                 }
             }

             if(prodCount > -1) {
                  menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = 0;
             }
             logger.debug("Size:"+styleMenus.size());
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             StringBuilder prodDesc                   = new StringBuilder();
             int size                       = styleMenus.size();
             for(int i=0; i < styleMenus.size(); i++) {
                  prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {                     
                 }
                
                 prodDesc.append("<div><div style=\"vertical-align: top;\"><table   border=\"0\" width=\"100%\">");
                  if(i ==0  ) {
                      prodDesc.append(" <tr><td align=\"center\"><img src=\""+logo+"\" style=\"max-height:70px; max-width:200px\"></td></tr>");
                  } else {
                  }
                  prodDesc.append("<tr> <td>"+styleMenus.get(i)+"</td></tr>"); 
                       
                         if(i ==0) { 
                             //prodDesc.append("<tr><td align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr></table>");
                             //prodDesc.append("<div  style=\"position:absolute; float:right; right:20px; bottom:10px; z-index:2;\" align=\"right\" "+style3+">Our draft list is mobile<img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div>");                            
                               prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         } else {
                             // prodDesc.append(" <tr><td height=\"150px\" ></td></tr>");
                              prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         }


                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);
        }
        return menus;
     }
    
    
    public  ArrayList<String> getFatHeadTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         String styleTitle          = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:16pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleHeader         = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:11pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName           = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:8pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
         //color                            = "#000000",
       //  String dot                         = "<td><img height=\"25px\" src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/dot.png\"></td>";
         String dot                         = "<span "+ styleHeader+" >.</span>";
         String  bgcolor                    = "#FFFFFF";

         logger.debug("Fat Head Template");
         try {
             String selectDraftBeer         = "SELECT DISTINCT IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " pD.origin,pD.ibu, IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            "  AND l.local = ? AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY  p.name;";
             String SelectFeatureBeer       ="SELECT DISTINCT  bS.majorStyle, pS.name,p.name, IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), pDE.description, p.id, pSD.description FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id =l.product"
                                            + " LEFT JOIN productDescription pD ON pD.product = p.id   LEFT JOIN productDesc pDE ON pDE.product = p.id  LEFT JOIN brewStyleMap bM ON bM.product = l.product"
                                            + " LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id "
                                            + " WHERE  p.id NOT IN(4311,10661) AND b.location = ? AND l.advertise= 1 AND l.status = 'RUNNING'  GROUP BY l.product ORDER BY   bS.majorStyle;";
             String selectPrices            = "SELECT i.product, CONCAT(s.ounces,' oz'), CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            " GROUP BY i.product ORDER BY i.product, iP.value;";
             
             
              String selectBottleBeer        = "SELECT  DISTINCT IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), "
                                            + "  pD.origin,pD.ibu, IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description),   p.id,price FROM bottleBeer b LEFT JOIN product p ON p.id=b.product"
                                            + " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "  LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style "
                                            + "  LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location  WHERE b.location= ? AND p.id NOT IN(4311,10661) ORDER BY  bS.majorStyle;";
             
             String getMarketPeriod         = "SELECT DISTINCT IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " pD.origin,pD.ibu, IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), " +
                                            " p.id FROM lineUpdates lU LEFT JOIN line l ON l.id = lU.line LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = lU.location WHERE lU.location = ?  " +
                                            "  ORDER BY lU.date DESC, lU.id DESC LIMIT 1;";

             String samplerProdut[]         ={"STARLIGHT LAGER #1", "BUMBLEBERRY HONEY BLUEBERRY ALE #2","TRAIL HEAD PALE ALE #3 ","HEAD HUNTER IPA #4 ","ROCKETMAN RED #5"};
             String samplerAbv[]            ={"4.9% ABV "+dot+" 29 IBU "+dot+" 16 oz "+dot+" $4.50","5.3% ABV "+dot+" 13 IBU . 16oz "+dot+" $4.50 "," 6.3% "+dot+" 55 IBU "+dot+" 16oz "+dot+" $5 ","7.5% ABV "+dot+" 87 IBU "+dot+" 16oz "+dot+" $5 ","6.4% ABV "+dot+" 50 IBU "+dot+" 16 oz "+dot+" $4.75 "};
             String samplerDesc[]           ={"This German-style lager has crackery aromas of fresh milled grain and grassy, floral hops, with malt flavors, fresh cut hay and bitter noble hops. Clean, crisp and refreshing. ",
                                              "Fresh harvested spring honey, infused with fresh blueberries. Light & refreshing. Crackery malt flavors, hint of sweetness & light blueberry finish. ",
                                              "Brewed with whole flower Simcoe and Citra hops. ", "West Coast-Style IPA with a huge hop display of pine, grapefruit, citrus and pineapple. A punch-you-in-the-mouth brew for those who truly love their hops! Uncivilized and Aggressive! ",
                                                "A rich red/copper colored ale with a floral hoppy twist. Medium sweet malt body balanced with Mosaic and Centennial hops to satisfy both hop and malt lovers alike! "};
             ArrayList<Integer> localBeers
                                            = new ArrayList<Integer>();
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";


             double prodCount               = -1;             
             count                          = 8;             
             style1                         = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Cambria'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             String style3                  = "style=\" color: #000000; font-family:'Cambria'; font-size:10pt;  word-wrap:break-word; text-align:right;\"";
             StringBuilder menuDesc         = new StringBuilder();
             String preStyle                = "";
             int tableRow                   = -1;
             menuDesc                       = new StringBuilder();
             boolean createTable            = true;
             double countLimit              = 16;
             int pixelCount                 =0;
             Calendar currentDate                = Calendar.getInstance();
             if(beerType ==2){            
             for(int i=0;i<samplerProdut.length;i++) {
                 String name                = samplerProdut[i];
                 String desc                = samplerDesc[i];
                 String abv                 = samplerAbv[i];
                 StringBuilder prodDesc     = new StringBuilder();
                 if(i==0){
                     menuDesc.append("<div "+style2+">"+dateFormat.format(currentDate.getTime())+"</div><table border=\"0\"><tr>"
                             + "<td colspan=\"2\" "+styleTitle+" width=\"350px\">FAT HEAD\'S BEERS <P "+ styleHeader+" >BREWER\'S CHOICE SAMPLER TRAY #1-5 </p> <hr style=\"height:3px;    border:none;color:#333;background-color:#333;\" /> </td></tr>");
                     prodCount++;
                     pixelCount             =pixelCount+80;
                 }
                 prodDesc.append("<table border=\"0\"> <tr><td align=\"center\" width=\"700px\" "+ styleHeader+" >"+name+"</td></tr><tr><td align=\"center\" width=\"700px\" "+ styleName+" >"+desc+" </td></tr><tr><td align=\"center\" width=\"700px\" "+ styleName+" > "+abv+"  </td></tr>"
                         + "</table>");
              
                 prodCount++;
                 menuDesc.append("<tr><td colspan=\"2\">"+prodDesc.toString()+"</td></tr>");
                 pixelCount                 =pixelCount+80;
                
                 
             }
            }
             menuDesc.append("<tr><td width=\"350px\" colspan=\"2\"><hr style=\"height:3px;    border:none;color:#333;background-color:#333;\" /></td></tr>");
             stmt                        = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1,1);
             stmt.setInt(2, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {                                
                String name                 = HandlerUtils.nullToEmpty(rs.getString(1));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(2));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(3));
                int ibu                     =  rs.getInt(4);
                int product                 = rs.getInt(6);                
                String desc                 = HandlerUtils.nullToEmpty(rs.getString(5));  
                
                localBeers.add(product);
                String price                ="";
                name                        = name.replace("Fat Head\'s", "");
                name                        = name.replace("Fat Heads", "");
                name                        = name.toUpperCase();
                if(beerType ==1){  
                    double bPrice          = rs.getDouble(7);
                    if(bPrice > 0){
                        price               =" $"+bPrice;
                    }
                } else {
                    price                   = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                StringBuilder prodDesc      = new StringBuilder();                 
                BreakIterator bi            = BreakIterator.getSentenceInstance();
                bi.setText(desc);
                String descClean            = "";
                int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }
                 if(abv!=null && !abv.equals("")){
                     abv                    +=" ABV" ;
                     if(ibu > 0){
                         abv                += " "+dot+" "+ibu+" IBU";
                     }
                 } else {
                     if(ibu >0){
                         abv                = ibu+" IBU";
                     }
                 }
                 
                 if(abv!=null && !abv.equals("")){
                     if(price!=null && !price.equals("")){
                         abv                += " "+dot+" "+price;
                     }
                 } else {
                     if(price!=null && !price.equals("")){
                         abv                = price;
                     }
                 }
                 String abvStr              = "";
                 if(abv!=null && !abv.equals("")){
                        abvStr              ="<tr><td  width=\"350px\" "+ style2+">"+abv+"</td></tr>";
                    }
                  prodDesc.append("<table border=\"0\"> <tr><td align=\"center\" width=\"700px\" "+ styleHeader+" >"+name+"</td></tr><tr><td align=\"center\" width=\"700px\" "+ styleName+" >"+desc+" </td></tr><tr><td align=\"center\" width=\"700px\" "+ styleName+" > "+abv+"  </td></tr>"
                         + "</table>");
              
                 prodCount++;
                 menuDesc.append("<tr><td colspan=\"2\">"+prodDesc.toString()+"</td></tr>");
                 pixelCount                 =pixelCount+80;
                  if(prodCount>=countLimit) {                      
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                     preStyle               = "";
                     menuDesc.append("<table border=\"0\">");                     
                 }                
             }
             
             menuDesc.append("</table>");
             countLimit                     = 35;             
             String title                   = "";                          
             if(beerType ==1){
                 stmt                       = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottles & Cans";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, 0);
                 stmt.setInt(2, location);
                 title                      = "Now On Tap";
             }

             int draftCount                 = -1;
            // stmt                           = transconn.prepareStatement(selectDraftBeer);
             //stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {                                
                String name                 = HandlerUtils.nullToEmpty(rs.getString(1));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(2));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(3));
                int ibu                     =  rs.getInt(4);
                int product                 = rs.getInt(6);
                if(!localBeers.contains(product)){
                
                StringBuilder prodDesc      = new StringBuilder();
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 String price               ="";
                 if(beerType ==1){  
                     double bPrice          = rs.getDouble(7);
                     if(bPrice > 0){
                         price              =" $"+bPrice;
                     }
                 } else {
                     price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                 if(createTable){
                     menuDesc.append("<table border=\"0\" cellspacing=\"0\" cellpadding=\"0\" ><tr>"
                             + "<td colspan=\"2\" "+styleTitle+">GUEST BEERS</td></tr>");
                      createTable = false;
                      prodCount++;
                      pixelCount                 =pixelCount+80;
                 }
                 if(origin!=null && !origin.equals("")){
                     name                   +=", "+origin;
                 }
                 
                 if(abv!=null && !abv.equals("")){
                     abv                    +=" ABV" ;
                     if(ibu > 0){
                         abv                += " "+dot+" "+ibu+" IBU";
                     }
                 } else {
                     if(ibu >0){
                         abv                = ibu+" IBU";
                     }
                 }
                 
                 if(abv!=null && !abv.equals("")){
                     if(price!=null && !price.equals("")){
                         abv                += " "+dot+" "+price;
                     }
                 } else {
                     if(price!=null && !price.equals("")){
                         abv                = price;
                     }
                 }
                 String abvStr              = "";
                 if(abv!=null && !abv.equals("")){
                        abvStr              ="<tr><td  width=\"350px\" "+ style2+">"+abv+"</td></tr>";
                    }
                 
                
                 prodDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" border=\"0\"> <tr><td  width=\"350px\" "+ style1+">"+name+"</td></tr>" +abvStr
                         + " </table>");
                 
                  
                     menuDesc.append("<tr><td><img height=\"20px\" src=\"http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/dot.png\"></td><td>"+prodDesc.toString()+"</td></tr>");
                      prodCount                  =prodCount+1;
                     draftCount++;
                     pixelCount                 =pixelCount+40;
 
                 
                

                 if(prodCount>=countLimit) {
                      
                     menuDesc.append("</table>");
                     logger.debug("Pixel"+pixelCount);
                     pixelCount             =0;
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                   //  createTable            = true;
                     preStyle               = "";
                      menuDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" >");
                 }
                }



             }

             if(prodCount > -1) {
                 menuDesc.append("<tr><td width=\"350px\" colspan=\"2\"><hr style=\"height:3px;    border:none;color:#333;background-color:#333;\" /></td></tr>");
                  menuDesc.append("</table>");
                  menuDesc.append("<table border=\"0\"> <tr><td  width=\"20px\"></td><td  width=\"350px\" "+ style1+">*ANY 4.75oz SAMPLE IS $2.50-$3</td></tr>"
                          + "<tr><td  width=\"20px\"></td><td  width=\"350px\" "+ style1+">*ANY taster (approximately 1oz) is free</td></tr>"
                          + "<tr><td  width=\"20px\"></td><td  width=\"350px\" "+ style1+">*Brewer's Choice Sampler: $8.50 </td></tr>"
                          + "<tr><td  width=\"20px\"></td><td  width=\"350px\" "+ style1+">*Build-Your-Own Sampler: $10</td></tr>"
                           + "<tr><td  width=\"20px\"></td><td  width=\"350px\" "+ style1+"></td></tr>"
                         + " </table>");
             stmt                           = transconn.prepareStatement(getMarketPeriod);             
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             if (rs.next()) { 
                 String name                = HandlerUtils.nullToEmpty(rs.getString(1));
                String abv                  = HandlerUtils.nullToEmpty(rs.getString(2));
                String origin               = HandlerUtils.nullToEmpty(rs.getString(3));
                int ibu                     =  rs.getInt(4);
                int product                 = rs.getInt(6);                
                String desc                 = HandlerUtils.nullToEmpty(rs.getString(5));                 
                
                 localBeers.add(product);
                  String price              ="";
                 if(beerType ==1){  
                     double bPrice          = rs.getDouble(8);
                     if(bPrice > 0){
                         price              =" $"+bPrice;
                     }
                 } else {
                     price                  = HandlerUtils.nullToEmpty(productPrices.get(product));
                 }
                 StringBuilder prodDesc     = new StringBuilder();                 
                BreakIterator bi            = BreakIterator.getSentenceInstance();
		bi.setText(desc);
                String descClean            = "";
		int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }
               
                 if(abv!=null && !abv.equals("")){
                     abv                    +=" ABV" ;
                     if(ibu > 0){
                         abv                += " "+dot+" "+ibu+" IBU";
                     }
                 } else {
                     if(ibu >0){
                         abv                = ibu+" IBU";
                     }
                 }
                 
                 if(abv!=null && !abv.equals("")){
                     if(price!=null && !price.equals("")){
                         abv                += " "+dot+" "+price;
                     }
                 } else {
                     if(price!=null && !price.equals("")){
                         abv                = price;
                     }
                 }
                 String abvStr              = "";
                 if(abv!=null && !abv.equals("")){
                        abvStr              ="<tr><td  width=\"350px\" "+ style2+">"+abv+"</td></tr>";
                    }
                 color                      ="#FF0000";
                 styleTitle                 = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:16pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                 styleHeader                = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:11pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                 styleName                  = "style=\" color: "+color+"; font-family:'Cambria_Bold'; font-size:8pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
                  menuDesc.append("<table border=\"0\"> <tr><td "+styleTitle+">NEW ON TAP:</td></tr><tr><td "+styleTitle+"></td></tr><tr><td align=\"center\" width=\"700px\" "+ styleHeader+" >"+name+"</td></tr><tr><td align=\"center\" width=\"350px\" "+ styleName+" >"+abv+"</td></tr></table>");
             }
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = 0;

             }
             
             logger.debug("Size:" + styleMenus.size());
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             StringBuilder prodDesc                   = new StringBuilder();
             int size                       = styleMenus.size();
             for(int i=0; i < styleMenus.size(); i++) {
                  prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {                     
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: Cambria;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Cambria.ttf') format(\"truetype\"); } "
                              + "@font-face {font-family: Cambria_Bold;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/Cambria_Bold.ttf') format(\"truetype\"); } "
                         + " </style>");
                 }
                
                 prodDesc.append("<div><div style=\"vertical-align: top; align:center;\"><table   border=\"0\" width=\"100%\">");
                  if(i ==0  ) {
                     // prodDesc.append(" <tr><td align=\"left\"><img src=\""+logo+"\" style=\"max-height:70px; max-width:200px\"></td></tr>");
                  } else {
                      
                  }
                  prodDesc.append("<tr> <td width=\"350px\" valign=\"top\" align=\"center\">"+styleMenus.get(i)+"</td>"); 
                  i++;
                  if(i<styleMenus.size()){
                      prodDesc.append("<td width=\"50px\" align=\"center\" valign=\"top\"></td><td  width=\"350px\" align=\"center\">"+styleMenus.get(i)+"</td></tr>"); 
                  } else{
                      prodDesc.append("<td width=\"350px\" align=\"center\"></td></tr>"); 
                  }
                  
                       
                         if(i ==0) { 
                             //prodDesc.append("<tr><td align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr></table>");
                             //prodDesc.append("<div  style=\"position:absolute; float:right; right:20px; bottom:10px; z-index:2;\" align=\"right\" "+style3+">Our draft list is mobile<img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div>");                            
                               prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         } else {
                             // prodDesc.append(" <tr><td height=\"150px\" ></td></tr>");
                              prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         }


                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);
        }
        return menus;
     }
    
    public  ArrayList<String> getBWW60MenuTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         ArrayList<String> products         = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#ffd200";
         bgcolor                        = "transparent";
         logger.debug("BWW 4 column");

         try {
             
             
             String selectDraftBeer         = "SELECT pS.name, p.name, pD.abv, p.id, pD.ibu, (SELECT name FROM customBeerName WHERE location = b.location AND product = p.id " +
                                            " LIMIT 1)  FROM line l LEFT JOIN bar b ON b.id = l.bar "
                                            + "LEFT JOIN product p ON p.id = l.product  LEFT JOIN productDescription pD ON pD.product = p.id  LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style"
                                            + " LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY  p.name LIMIT 60;";
             String selectBottleBeer        = "SELECT pS.name, p.name, pD.abv,b.price, (SELECT name FROM customBeerName WHERE location = b.location AND product = p.id " +
                                            " LIMIT 1), pD.category, pD.ibu FROM bottleBeer b  LEFT JOIN product p ON p.id = b.product LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + " LEFT JOIN brewStyleMap bM ON bM.product = b.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style "
                                            + "  LEFT JOIN productSetDescription pSD ON pSD.productSet=pS.id   WHERE location=?  AND p.id NOT IN(4311,10661) GROUP BY p.id ORDER by pD.category, p.name ;";
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', ROUND(iP.value, 2)), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";
             
             
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 price                  = HandlerUtils.nullToEmpty(rs.getString(3));                 
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";
             style1                         = "style=\" color: "+color+"; font-family:'AachenBold'; font-size:40pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:25pt;  word-wrap:break-word; text-align:left;\"";
             String style3                  = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:25pt;  word-wrap:break-word; text-align:right;\"";
             String styleT                  = "style=\" color: "+bgcolor+"; font-family:'Avantgarde'; font-size:25pt;  word-wrap:break-word; text-align:right;\"";

             int prodCount                  = 0;
             int colCount                   =0, rowCount=0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             StringBuilder draftDesc        = new StringBuilder();
             
             
             //ArrayList<String> html         = getHtmlTemplate(2);
             //ArrayList<String> bottle       = getHtmlTemplate(3);
             
             
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);         
             rs                             = stmt.executeQuery();             
             int index                      = 0;
             int totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 int product                = rs.getInt(4);                 
                 int IBU                    = rs.getInt(5);   
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String customName          = HandlerUtils.nullToEmpty(rs.getString(6));     
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 if(customName!=null && !customName.equals("")){
                     productName            = customName;
                 }                 
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV ";
                 } 
                 if(IBU >0){
                     if(!abv.equals("")){
                         abv                    += " | "+String.valueOf(IBU)+ " IBU ";
                     }  else {
                         abv                    += String.valueOf(IBU)+ " IBU ";
                     }
                 }
                 String PriceTag            ="<td " + style2 + ">" +  price + "</td>";
                 if(price.equals("")){
                    PriceTag                ="<td " + styleT + ">@@@Price@@@</td>";
                 } 
                 prodDesc                   = new StringBuilder();
                 prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:95%;'  border=\"0\"> <tr><td colspan='3'><div class='ptextfill' style='width:650px;height:89px;'>"
                                             + "<span style=\" color: #000000; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">"+productName+"</span></div></td></tr>"
                                             + "<tr><td ><div class='ptextfill' style='width:100%;height:50px;'>"
                                             + "<span style=\" color: #000000; font-family:'Avantgarde';  text-align: left; \">"+brewStyle+"</span></div></td><td " + style3 + "  align='center'>" +  abv + "</td><td width='2%' align='right' "+style3+"><p "+styleT+">_</p></td></tr>"
                                                + "<tr>" +  PriceTag + "<td align='right' "+style3+">"+""+"</td><td width='5%' align='right' "+style3+"></td></tr>" +
                                             "</table>");
                 if(prodCount==0){         
                     draftDesc.append(" <style>"
                         + " @font-face {font-family: AachenBold;  src: url('http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/AachenBold.ttf') format(\"truetype\"); } " 
                         + " @font-face {font-family: Avantgarde;  src: url('http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Avantgarde.ttf') format(\"truetype\"); } " 
                             + "</style>");
                     draftDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"   width=\"100%\" height=\"15in\" border=\"0\"> "
                            + " <tr><td colspan=\"5\"  height='20px'></td> </tr>"
                            + "<tr> <td colspan=\"5\"  "+style1+">DRAFT BEER</td></tr>"
                            + " <tr><td colspan=\"5\"  height='30px'></td> </tr>");
                     
                 }
                 if(colCount==0){
                     draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                 } else if(colCount==2){
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr> <tr> <td colspan='5' height='30px'></td> </tr>");
                     rowCount++;
                 } else {
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                 }
                 
                 colCount++;
                 prodCount++;
                 if(colCount>=3){
                     colCount               =0;
                 }
                 
             }
             if(prodCount < 60){
                 for(int i=prodCount; i<60;i++){
                     prodDesc                   = new StringBuilder();
                     prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:95%;'  border=\"0\"> <tr><td colspan='3'><div class='ptextfill' style='width:650px;height:90px;'> "
                                               + "<span style=\" color: transparent; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">@@@Product NAme@@@</span></div> </td></tr>"
                                               + "<tr><td ><div class='ptextfill' style='width:100%;height:50px;'> <span style=\" color: transparent; font-family:'Avantgarde';  text-align: left; \">@@@Style@@@</span> </div></td><td " + styleT + "  align='center'>@@@ABV@@@</td><td width='2%' align='right' "+styleT+"><p "+styleT+">_</p></td></tr>"
                                                  + "<tr><td " + styleT + ">@@@Price@@@</td><td align='right' "+styleT+">"+""+"</td><td width='5%' align='right' "+styleT+"></td></tr>" +
                                               "</table>");
                     if(colCount==0){
                         draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                     } else if(colCount==2){
                         draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr><tr> <td colspan='5' height='30px'></td> </tr>");
                         rowCount++;
                     } else {
                         draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                     }
                     colCount++;
                     
                   if(colCount>=3){
                       colCount               =0;
                   }
                     
                 }
             }
             
             draftDesc.append("</table>");
             String style4                  = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:18pt;  word-wrap:break-word; text-align:center;\"";
             draftDesc.append("<div "+style4+"></div>");
             draftDesc.append("<div "+style4+">* Ask about our Seasonal and Limited Allocation Beers! *</div>");
             draftDesc.append("<div "+style4+">* Join us for Double Happy Hour, Monday through Friday, 2 PM - 6 PM & 10 PM - Close *</div>");
             draftDesc.append("<div "+style4+">* $3 Select Sharables, $3 Tall Domestic Draft Beers, $4 & $5 Tall Craft & Import Draft Beers (Limited Allocation Kegs are excluded)*</div>");
             styleMenus.add(draftDesc.toString());
             
             

               

           
             draftDesc                      = new StringBuilder();
             
             
            colCount =0; prodCount=0; rowCount=0;            
            int prevType                    = 0;
            stmt                           = transconn.prepareStatement(selectBottleBeer);
             stmt.setInt(1, location);         
             rs                             = stmt.executeQuery();             
              index                      = 0;
              totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));                 
                 double priceVal            = rs.getDouble(4);
                 
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV";
                 }   
                 int IBU                    = rs.getInt(7);
                 if(IBU >0){
                     if(!abv.equals("")){
                         abv                    += " | "+String.valueOf(IBU)+ " IBU";
                     }  else {
                         abv                    += String.valueOf(IBU)+ " IBU";
                     }
                     
                 }
                 String price               = "";
                 if(priceVal >0){
                     price                  = "$"+String.valueOf(priceVal);
                 } 
                 String PriceTag            ="<td " + style2 + ">" +  price + "</td>";
                 if(price.equals("")){
                    PriceTag                ="<td " + styleT + ">@@@Price@@@</td>";
                 } 
                 String customName          = HandlerUtils.nullToEmpty(rs.getString(5));
                 int bType                  = rs.getInt(6);
                 if(customName != null && !customName.equals("")){
                     productName            = customName;
                 }   
                 String categoryName        = "";
                 if(bType == 1) {
                     categoryName           = "DOMESTIC BOTTLED BEER";
                         
                     } else if(bType== 2) {
                         categoryName       = "CRAFT BOTTLED BEER";
                     } else if(bType == 3) {
                         categoryName       = "IMPORTED BOTTLED BEER";
                     }
                 if(prevType!=bType){
                     if(colCount>0){
                     for(int i=colCount;i<3;i++){
                          draftDesc.append("<td width=\"33%\"></td>");
                          totalProduct++;
                     }
                      draftDesc.append("</tr></table>");
                     }
                    
                     prodCount              =0;
                     colCount               = 0;
                     rowCount               =0;
                     
                 }
                 
             
                 prodDesc                   = new StringBuilder();
                 prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:95%;' border=\"0\"> <tr><td colspan='2'><div class='ptextfill' style='width:650px;height:70px;'>"
                                             + "<span style=\" color: #000000; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">"+productName+"</span></div></td><td width='2%' align='right' "+style3+"><p "+styleT+">_</p></td></tr>"
                                             + "<tr><td  ><div class='ptextfill' style='width:100%;height:40px;'>"
                                             + "<span style=\" color: #000000; font-family:'Avantgarde';  text-align: left; \">"+brewStyle+"</span></div></td><td " + style3 + "  align='center'>" +  abv + "</td><td width='2%' align='right' "+style3+"><p "+styleT+">_</p></td></tr>"
                                                + "<tr>" +  PriceTag + "<td align='right' "+style3+">"+""+"</td><td width='5%' align='right' "+style3+"></td></tr>" +
                                             "</table>");
                 if(prodCount==0){                           
                     
                     
                     draftDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"   width=\"100%\" height=\"15in\" border=\"0\">"
                             + " <tr><td colspan=\"5\"  height='20px'></td> </tr>"
                                 + "<tr> <td colspan=\"5\"  "+style1+">"+categoryName+"</td> </tr>"
                             + " <tr><td colspan=\"5\"  height='30px'></td> </tr>");
                     
                 }
                 if(colCount==0){
                     draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                 } else if(colCount==2){
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr> <tr> <td colspan='5' height='20px'></td><tr>");
                     rowCount++;
                 } else {
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                 }
                 prevType                   = bType;
                 
                 colCount++;
                 prodCount++;
                 if(colCount>=3){
                     colCount               =0;
                 }
                
                
             }
             
             if(totalProduct < 48){
                 for(int i=totalProduct; i<48;i++){
                     prodDesc                   = new StringBuilder();
                     prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:95%;'  border=\"0\"> <tr><td colspan='3'><div class='ptextfill' style='width:650px;height:90px;'> "
                                               + "<span style=\" color: transparent; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">@@@Product NAme@@@</span></div> </td></tr>"
                                               + "<tr><td ><div class='ptextfill' style='width:100%;height:50px;'> <span style=\" color: transparent; font-family:'Avantgarde';  text-align: left; \">@@@Style@@@</span> </div></td><td " + styleT + "  align='center'>@@@ABV@@@</td><td width='2%' align='right' "+styleT+"><p "+styleT+">_</p></td></tr>"
                                                  + "<tr><td " + styleT + ">@@@Price@@@</td><td align='right' "+styleT+">"+""+"</td><td width='5%' align='right' "+styleT+"></td></tr>" +
                                               "</table>");
                     if(colCount==0){
                         draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                     } else if(colCount==2){
                         draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr><tr> <td colspan='5' height='30px'></td> </tr>");
                         rowCount++;
                     } else {
                         draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                     }
                     colCount++;
                     
                   if(colCount>=3){
                       colCount               =0;
                   }
                     
                 }
             }
             if(draftDesc.toString().length()<1){
                 draftDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"   width=\"100%\" height=\"15in\" border=\"0\">"
                             + " <tr><td colspan=\"5\"  height='20px'></td> </tr>"
                                 + "<tr> <td colspan=\"5\"  "+style1+"></td> </tr>"
                             + " <tr><td colspan=\"5\"  height='30px'></td> </tr>");
                 
             }
             if(colCount>0){
                 for(int i=colCount;i<3;i++){
                     draftDesc.append("<td width=\"33%\"></td>");
                 }
                 draftDesc.append("</tr>");
             }
             
             //draftDesc.append("</table>");
             style1                         = "style=\" color: "+color+"; font-family:'AachenBold'; font-size:40pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             draftDesc.append("<tr><td valign=\"bottom\" colspan=\"5\"><table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+"\"   width=\"100%\" border=\"0\"> "
                                + " <tr><td  colspan='3' height='10px'></td> </tr>"
                                 + " <tr><td width='60%' align='left' valign='bottom' "+style1+">Beer & Sauce Pairing</td> <td width='30%' valign='bottom' align='right'><img height='90%' border=\"0\" src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/BWWblacklogo.png' ></td><td width='10%'></td></tr>"
                              + "<tr><td colspan='3' style=\"background-color:"+bgcolor+"\"   ><img border=\"0\" src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/sauces2.png' ></td></tr></table></td> </tr></table>");
            // draftDesc.append("<img border=\"0\" src='http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/sauces.png' >");
             styleMenus.add(draftDesc.toString());
             

            
             
             for( int i=0; i < styleMenus.size(); i++) {       
                 
                 String col1                = styleMenus.get(i);                 
                 menus.add(col1);
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
    
    
    public  ArrayList<String> getBWW4ColTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend , MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         ArrayList<String> products         = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                       = "#000000",
         String  bgcolor                    = "#ffd200";
         //bgcolor                        = "transparent";
         logger.debug("BWW 4 column");

         try {
             
             
             String selectDraftBeer         = "SELECT pS.name, p.name, pD.abv, p.id, pD.ibu, (SELECT name FROM customBeerName WHERE location = b.location AND product = p.id " +
                                            " LIMIT 1)  FROM line l LEFT JOIN bar b ON b.id = l.bar "
                                            + "LEFT JOIN product p ON p.id = l.product  LEFT JOIN productDescription pD ON pD.product = p.id  LEFT JOIN productDesc pDE ON pDE.product = p.id "
                                            + "LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS ON pS.id = bM.style LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style"
                                            + " LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY  p.name;";
             
             String selectPrices            = "SELECT i.product, s.name, CONCAT('$', ROUND(iP.value, 2)), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "  GROUP BY i.product ORDER BY i.product, iP.value;";
             
             
             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);
                 int showOnTv               = rs.getInt(4);
                 String price               = "";
                 if(showOnTv > 0){
                     price                  = HandlerUtils.nullToEmpty(rs.getString(2)) + " - " + HandlerUtils.nullToEmpty(rs.getString(3));
                 } else {
                     price                  = HandlerUtils.nullToEmpty(rs.getString(3));
                 }
                 productPrices.put(productId, price);
             }
             String style1                  = "";
             String style2                  = "";
             style1                         = "style=\" color: "+color+"; font-family:'AachenBold'; font-size:40pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:25pt;  word-wrap:break-word; text-align:left;\"";
             String style3                  = "style=\" color: "+color+"; font-family:'Avantgarde'; font-size:25pt;  word-wrap:break-word; text-align:right;\"";

             int prodCount                  = 0;
             int colCount                   =0, rowCount=0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             StringBuilder draftDesc        = new StringBuilder();
             
             
             //ArrayList<String> html         = getHtmlTemplate(2);
             //ArrayList<String> bottle       = getHtmlTemplate(3);
             
             
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);         
             rs                             = stmt.executeQuery();             
             int index                      = 0;
             int totalProduct               =0;
             while (rs.next()) {
                 totalProduct++;
                 String  brewStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 int product                = rs.getInt(4);                 
                 int IBU                    = rs.getInt(5);   
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String customName          = HandlerUtils.nullToEmpty(rs.getString(6));     
                 String price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 if(customName!=null && !customName.equals("")){
                     productName            = customName;
                 }                 
                 String abv                 = "";
                 float abvF                 = rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"% ABV ";
                 } 
                 if(IBU >0){
                     if(!abv.equals("")){
                         abv                    += " | "+String.valueOf(IBU)+ " IBU";
                     }  else {
                         abv                    += String.valueOf(IBU)+ " IBU";
                     }
                 }
                 price                      = "";
                 prodDesc                   = new StringBuilder();
                 prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:100%;'  border=\"0\"> <tr><td colspan='3'><div class='ptextfill' style='width:650px;height:89px;'>"
                                             + "<span style=\" color: #000000; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">"+productName+"</span></div></td></tr>"
                                             + "<tr><td colspan='3'><div class='ptextfill' style='width:100%;height:50px;'>"
                                             + "<span style=\" color: #000000; font-family:'Avantgarde';  text-align: left; \">"+brewStyle+"</span></div></td></tr>"
                                                + "<tr><td " + style2 + ">" +  abv + "</td><td align='right' "+style3+">"+price+"</td><td width='5%' align='right' "+style3+"></td></tr>" +
                                             "</table>");
                 if(prodCount==0){         
                    
                     draftDesc.append("<table cellspacing=\"0\" cellpadding=\"0\" style=\"background-color:"+bgcolor+";\"   width=\"100%\" height=\"15in\" border=\"0\"> "
                            + " <tr><td colspan=\"5\"  height='20px'></td> </tr>"
                            + "<tr> <td colspan=\"5\"  "+style1+">DRAFT BEER</td> </tr>"
                            + " <tr><td colspan=\"5\"  height='30px'></td> </tr>");
                     
                 }
                 if(colCount==0){
                     draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                 } else if(colCount==2){
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr> <tr> <td colspan='5' height='30px'></td> <tr>");
                     rowCount++;
                 } else {
                     draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                 }
                 
                 colCount++;
                 prodCount++;
                 if(colCount>=3){
                     colCount               =0;
                 }
                 if(prodCount>=60){
                     prodCount              =0;
                     draftDesc.append("</table>");
                     styleMenus.add(draftDesc.toString());
                     draftDesc        = new StringBuilder();
                 }
                 
             }
             if(prodCount>0){
                 prodDesc                   = new StringBuilder();
                 prodDesc.append("<table  cellspacing=\"0\" cellpadding=\"0\" style='width:100%;'  border=\"0\"> <tr><td colspan='3'><div class='ptextfill' style='width:650px;height:89px;'>"
                                             + "<span style=\" color: #000000; font-family:'AachenBold';  text-align: left; font-weight:bold; word-wrap:break-word; \">"+""+"</span></div></td></tr>"
                                             + "<tr><td colspan='3'><div class='ptextfill' style='width:100%;height:50px;'>"
                                             + "<span style=\" color: #000000; font-family:'Avantgarde';  text-align: left; \">"+""+"</span></div></td></tr>"
                                                + "<tr><td " + style2 + ">" + ""  + "</td><td align='right' "+style3+">"+""+"</td><td width='5%' align='right' "+style3+"></td></tr>" +
                                             "</table>");
                    for(int i=prodCount;i<=60;i++){
                        if(colCount==0){
                            draftDesc.append("<tr><td width=\"2%\"></td><td width=\"32%\">"+prodDesc.toString()+"</td>");
                        } else if(colCount==2){
                            draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td><td width=\"2%\"></td></tr> <tr> <td colspan='5' height='30px'></td> <tr>");
                            rowCount++;
                        } else {
                            draftDesc.append("<td width=\"32%\">"+prodDesc.toString()+"</td>");
                        }
                        colCount++;
                        if(colCount>=3){
                            colCount               =0;
                        }
                    }
                     prodCount              =0;
                     draftDesc.append("</table>");
                     styleMenus.add(draftDesc.toString());
                     draftDesc        = new StringBuilder();
                 }
             
             
             
             for( int i=0; i < styleMenus.size(); i++) {       
                 
                 String col1                = styleMenus.get(i);     
                 prodDesc                   = new StringBuilder();
                 if(i==0){
                     prodDesc.append(" <style>"
                         + " @font-face {font-family: AachenBold;  src: url('http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/AachenBold.ttf') format(\"truetype\"); } " 
                         + " @font-face {font-family: Avantgarde;  src: url('http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/Avantgarde.ttf') format(\"truetype\"); } " 
                         + "</style>");
                 }
                 prodDesc.append(col1);
                 menus.add(prodDesc.toString());
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);   
        }
        return menus;
     }
    
    public  ArrayList<String> getHootersCTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();
         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //String bgcolor                     = "#336699";
         color                              = "#f6783b";
         logger.debug("Template Hooters loading");

         try {
             String selectDraftBeer         = "SELECT pD.category, p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft  FROM line l LEFT JOIN bar b ON b.id = l.bar"
                                            + " LEFT JOIN product p ON p.id = l.product LEFT JOIN productDescription pD ON pD.product = p.id "
                                            +" LEFT JOIN brewStyleMap bM ON bM.product = l.product  LEFT JOIN productSet pS ON pS.id = bM.style "
                                            + " WHERE l.status = 'RUNNING'  AND b.location = ?  AND p.id NOT IN(4311,10661) GROUP BY l.product ORDER BY craft, pD.category DESC, p.name ;";
             String selectBottleBeer        = "SELECT  pD.category,p.name,pD.abv,pS.name,pD.origin,if (pD.category=2 ,1,2 ) craft   FROM bottleBeer b LEFT JOIN product p ON p.id=b.product "
                                            + " LEFT JOIN productDescription pD ON pD.product=p.id LEFT JOIN brewStyleMap bM ON bM.product = b.product  LEFT JOIN productSet pS ON pS.id = bM.style WHERE location=?  AND p.id NOT IN(4311,10661) GROUP BY b.product ORDER BY craft, pD.category DESC, p.name;";
             String selectlogo              = "Select logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = 0;
             String title                   = "";
             StringBuilder prodDesc         = new StringBuilder();
             if(beerType == 1){
                 stmt                        = transconn.prepareStatement(selectBottleBeer);
                 stmt.setInt(1, location);
                 title                      = "Bottled Beer";
             } else {
                 stmt                        = transconn.prepareStatement(selectDraftBeer);
                 stmt.setInt(1, location);
                 title                      = "Draft Beer";
             }
             int oldCategory                = 0;
             rs                             = stmt.executeQuery();
             rs.last();
             int rowCount                   = rs.getRow();
             logger.debug("Row Count:" + rowCount);
             count                      = 15;
             style1                     = "style=\" color: "+color+"; font-family:'GrilledCheese'; font-size:9pt; font-weight:small; word-wrap:break-word; text-align:center;\"";
             style2                     = "style=\" color: "+color+"; font-family:'Arial'; font-size:8pt; font-weight:small; word-wrap:break-word; text-align:center;\"";
             String styleLeft           = "style=\" color: "+color+"; font-family:'GrilledCheese'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             String styleRight          = "style=\" color: "+color+"; font-family:'GrilledCheese'; font-size:15pt; font-weight:bold; word-wrap:break-word; text-align:right;\"";
             String styleRight2         = "style=\" color: "+color+"; font-family:'GrilledCheese'; font-size:13pt;  word-wrap:break-word; text-align:right;\"";
             String style3              = "style=\" color: "+color+"; font-family:'GrilledCheese'; font-size:13pt;  word-wrap:break-word; text-align:left;\"";
             String headStyle            = "style=\" color: "+color+"; font-family:'Arial'; font-size:13pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             

             //logger.debug("Count:" + count);
             prodDesc                       = new StringBuilder();
             rs.beforeFirst();
             while (rs.next()) {
                 int category               = rs.getInt(1);
                 if (category > 3) {
                     category               = 2;
                 }
                 
                 String productName         = HandlerUtils.nullToEmpty(rs.getString(2));
                 String abv                 ="";
                  float abvF                 =rs.getFloat(3);
                 if(abvF > 0) {
                     abv                    = String.valueOf(abvF)+"%";
                 }
                 
                 String style               = HandlerUtils.nullToEmpty(rs.getString(4));
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(5));
                 if(style.contains("Do Not Know")){
                     style                  = "";
                 }
                 String styleAbvOrigin      ="";
                 if(!origin.equals("")){
                     styleAbvOrigin         +=origin;
                 }
                 if(!styleAbvOrigin.equals("") && !style.equals("")){
                     styleAbvOrigin         +=" * "+style;
                     
                 } else if( !style.equals("")){
                     styleAbvOrigin         +=style;
                 }                 
                 if(!styleAbvOrigin.equals("") && !abv.equals("")){
                     styleAbvOrigin         +=" * "+abv;
                     
                 } else if( !abv.equals("")){
                     styleAbvOrigin         +=abv;
                 }
                 
                 if(category != oldCategory || prodCount == 0) {
                     String categoryName    = "";
                     if(category == 1) {
                         categoryName       = "DOMESTIC";
                         if (styleMenus.isEmpty()) {
                            prodDesc.append("</table>");
                            //logger.debug("product Count:" + prodCount);
                            styleMenus.add(prodDesc.toString());
                            prodDesc        = new StringBuilder();
                            prodCount       = 0;                             
                         }
                     } else if(category == 2) {
                         categoryName       = "CRAFT";
                     } else if(category == 3) {
                         categoryName       = "IMPORT";
                     }
                     
                     if(prodCount == 0){
                         prodDesc.append("<table valign=\"top\" width=\"100%\" height=\"6.8in\"  cellspacing=\"0\" cellpadding=\"0\"  border=\"0\"><tr> "
                                 + " <td "+headStyle+"><u>"+categoryName+"</u></td> </tr>");
                     } else {
                         prodDesc.append(" <tr><td "+style1+" height=\"20px\"></td> </tr>");
                         prodDesc.append(" <tr><td "+headStyle+"><b><u>"+categoryName+"</u></b></td> </tr>");
                     }
                     prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                     oldCategory            = category;
                     prodCount++;
                     
                 }
                 String prodTable           = "";
                prodTable                   ="<table  cellspacing=\"0\" cellpadding=\"0\" style='width:100%;'  border=\"0\"> <tr><td align=\"center\" "+style1+">"+productName+"</td></tr>"
                                             + "<tr><td align=\"center\" "+style2+">"+styleAbvOrigin+"</td></tr></table>";
                 /*prodDesc.append(" <tr><td width=\"100%\" align=\"center\"><div class='ptextfill' style='width:160px;height:30px;'><span style=\" color: #ed7d31; font-family:'GrilledCheese'; text-align: center;  \">"+productName+" </span></div></td</tr>"
                         + "<tr><td width=\"100%\" align=\"center\"><div class='ptextfill' style='width:160px;height:15px;'><span style=\" color: #ed7d31; font-family:'GrilledCheese'; text-align: center;    \">"+styleAbvOrigin+" </span></div></td></tr>");*/
                prodDesc.append("<tr><td>"+prodTable+"</td></tr>");
                 prodDesc.append(" <tr><td "+style1+" height=\"3px\"></td> </tr>");

                 prodCount++;
                 //logger.debug(productName+ ": "+prodCount);

                 if (prodCount >= count){
                     prodDesc.append("</table>");
                     //logger.debug("product Count:" + prodCount);
                     styleMenus.add(prodDesc.toString());
                     prodDesc               = new StringBuilder();
                     prodCount              = 0;
                 }
             }

             if(prodCount > 0){
                 for(int i=prodCount;i<count;i++){
                    //  prodDesc.append(" <tr><td "+style1+" height=\"5px\"></td> </tr>");
                 }
                 
                 prodDesc.append("</table>");
                 styleMenus.add(prodDesc.toString());
             }
             
             prodCount                      = 0;
              style3              = "style=\" color: "+color+"; font-family:'Arial'; font-size:25pt;  word-wrap:break-word; text-align:left;\"";
              String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             //logger.debug("Menu Size:" + styleMenus.size());
             for(int i=0; i < styleMenus.size(); i++) {
                 prodDesc                   = new StringBuilder();
                 String col1                = styleMenus.get(i);
                 String col2                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col2                   = styleMenus.get(i);
                 }
                 String col3                = "";
                 if(i + 1 < styleMenus.size()) {
                     i++;
                     col3                   = styleMenus.get(i);
                 }
                 if(menus.size()==0) {
                      prodDesc.append(" <style>"
                         + " @font-face {font-family: GrilledCheese;  src: url('http://social.usbeveragenet.com:8080/fileUploader/Images/GrilledCheese.ttf') format(\"truetype\"); } "                            
                         + " </style>");
                 }
                 String prodTable                   ="<table  cellspacing=\"0\" cellpadding=\"0\" style='width:100%;' border=\"0\"> <tr><td width=\"20%\" align=\"left\" ><img style=\" max-height:50px; max-width:200px;\" src=\""+logo+"\"/></td><td "+style3+"  width='50%' align='center' valign='bottom'>Draft Menu</td><td></td></tr><td "+style3+"  width='30%'></td></table>";
                 prodDesc.append("<table border=\"0\" height=\"7in\" width=\"100%\"><tr><td  height=\"20%\" colspan=\"3\" align=\"center\" >"+prodTable+"</td></tr>");
                 prodDesc.append("<tr><td height=\"80%\"  width='33%' valign=\"top\" align=\"left\">"+col1+"</td><td   width='33%' valign=\"top\" align=\"left\">"+col2+"</td><td  width='33%' valign=\"top\" align=\"left\">"+col3+"</td></tr>");                 
              
                 //prodDesc.append("<tr><td height=\"20px\"  valign=\"top\" align=\"right\"><table align=\"right\"> <tr><td  align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr> </table></td></tr>");
                    prodDesc.append("</table>");
                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
            close(rs);
            close(stmt);
        }
        return menus;
     }
    
     public  ArrayList<String> getMultiSizeTemplate(int beerType, int location, int customer, int count, String color, String logo, Element toAppend, MidwareLogger logger, RegisteredConnection transconn) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         PreparedStatement stmt             = null;
         ResultSet rs                       = null;
         //color                            = "#000000",
         String  bgcolor                    = "#FFFFFF";

         logger.debug("Multi BeverageSize Template");
         try {
             String selectDraftBeer         = "SELECT bS.majorStyle, pS.name, IF(cBN.name IS NULL, p.name, cBN.name), IF(pD.abv = 0.0, '', CONCAT(pD.abv,'%')), " +
                                            " IFNULL(IF(LENGTH(cBN.description) < 4 OR cBN.description IS NULL, pDE.description, cBN.description), pSD.description), "
                                            + "  pS1.name, p.id, pD.origin FROM line l LEFT JOIN bar b ON b.id = l.bar LEFT JOIN product p ON p.id = l.product " +
                                            " LEFT JOIN productDescription pD ON pD.product = p.id LEFT JOIN productDesc pDE ON pDE.product = p.id " +
                                            " LEFT JOIN brewStyleMap bM ON bM.product = l.product LEFT JOIN productSet pS1 ON pS1.id = bM.brewery  LEFT JOIN productSet pS ON pS.id = bM.style " +
                                            " LEFT JOIN beerStylesMap bSM ON bSM.productSet = bM.style LEFT JOIN beerStyles bS ON bS.id =  bSM.style " +
                                            " LEFT JOIN styleLogo sL ON sL.style = pS.name LEFT JOIN productSetDescription pSD ON pSD.productSet = pS.id " +
                                            " LEFT JOIN customBeerName cBN ON cBN.product = p.id AND cBN.location = b.location WHERE l.status = 'RUNNING' " +
                                            " AND l.advertise = 0 AND l.cask = 0 AND p.id NOT IN(4311,10661) AND b.location = ? GROUP BY l.product " +
                                            " ORDER BY bS.majorStyleId,pD.abv ;";            
             
            String selectPrices             = "SELECT i.product, s.name, CONCAT('$', iP.value), s.showOnTV FROM inventoryPrices iP LEFT JOIN inventory i " +
                                            " ON i.id = iP.inventory LEFT JOIN beverageSize s ON s.id = iP.size WHERE i.location = ? AND s.id <> 'null' " +
                                            "   ORDER BY i.product, iP.value;";

             HashMap<Integer, String> productPrices
                                            = new HashMap<Integer, String>();
             
             int prevProdId                 =0;
             int priceCount                 =0;
             String price               = "";
             stmt                           = transconn.prepareStatement(selectPrices);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 int productId              = rs.getInt(1);                 
                 int showOnTv               = rs.getInt(4);
                 if(prevProdId!=productId){
                     price                  ="";
                     priceCount             =0;
                 }                 
                 if(priceCount<4){
                     price                      += rs.getString(2)+"-"+rs.getString(3)+" ";
                 }
                 priceCount++;
                 prevProdId                 = productId;
                 productPrices.put(productId, price);
             }
             String selectlogo              = "SELECT logo FROM locationGraphics WHERE location = ?";
             String style1                  = "";
             String style2                  = "";

             int prodCount                  = -1;
             String title                   = "";
             count                          = 22;
             String styleHeader             = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName               = "style=\" color: "+color+"; font-family:'Arial'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style1                         = "style=\" color: "+color+"; font-family:'Arial'; font-size:9pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Arial'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             String style1Rught              = "style=\" color: "+color+"; font-family:'Arial'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:right;\"";
             String styleT                  = "style=\" color: "+bgcolor+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:right;\"";

             StringBuilder menuDesc         = new StringBuilder();
             String preStyle                = "";
             int tableRow                   = -1;
             menuDesc                       = new StringBuilder();
             boolean createTable            = true;
             int countLimit                 = 21;             
             int draftCount                 = -1;
             stmt                           = transconn.prepareStatement(selectDraftBeer);
             stmt.setInt(1, location);
             rs                             = stmt.executeQuery();
             while (rs.next()) {
                 String majorStyle          = HandlerUtils.nullToEmpty(rs.getString(1));
                 String style               = HandlerUtils.nullToEmpty(rs.getString(2));
                 String name                = HandlerUtils.nullToEmpty(rs.getString(3));
                 String abv                 = HandlerUtils.nullToEmpty(rs.getString(4));
                 String desc                = HandlerUtils.nullToEmpty(rs.getString(5));
                 String brewery             = HandlerUtils.nullToEmpty(rs.getString(6));
                 int product                = rs.getInt(7);
                 String origin              = HandlerUtils.nullToEmpty(rs.getString(8));
                 StringBuilder prodDesc     = new StringBuilder();
                 
                 BreakIterator bi           = BreakIterator.getSentenceInstance();
		 bi.setText(desc);
                 String descClean           = "";
		 int index                  = 0;
		 while (bi.next() != BreakIterator.DONE) {
                     String sentence        = desc.substring(index, bi.current());
                     //logger.debug("Sentence: "+ sentence);
                     if (index == 0) {
                        descClean           = sentence;
                     } else if(bi.current() < 250) {
                        descClean           += sentence;
                        //logger.debug("descClean: "+ descClean);
                     }
                     index                  = bi.current();
                     //logger.debug("Sentence: "+ index+":"+ sentence.length());
		 }

                 desc                       = descClean;
                 if(desc.length() < 4){
                     desc                    = "";
                 }
                 
                 if(!abv.trim().equals("")){
                     abv                    =" - "+abv+" ABV";
                 }
                 if(origin!=null &&!origin.equals("")){
                     origin = "<span "+style1+">Origin:"+origin+"</span><span "+styleT+">______</span>";
                 }
                 
                //logger.debug("Name:"+ name+" Desc Length:"+desc.length());
                 price               = HandlerUtils.nullToEmpty(productPrices.get(product));
                 prodDesc.append("<table   border=\"0\" width=\"100%\"> <tr><td "+style1+" > <span "+ styleName+">"+name+"  </span> "+abv+" <span "+ styleName+"> - </span>"+style+" </td><td "+style1Rught+"> <span>"+price+"</span></td></tr>"
                                             + "<tr><td colspan=\"2\"><div class='dtextfill' style='width:100%; height:28px;'>"
                                             + "<span style=\"  color: "+color+"; font-family:'Avantgarde';  text-align: justify; text-justify: inter-word; word-wrap:break-word; \">"+desc+"</span></div></td>"
                                             + " <td></td> </tr></table>");
                 if(prodCount == -1 || draftCount==-1){
                     if(createTable) {
                         menuDesc.append("<table width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\">");
                         createTable = false;
                         prodCount++;
                     } else {
                          prodCount++;
                     }
                     
                     tableRow++;

                 }
                 if(!preStyle.equals(majorStyle) || tableRow==0) {
                      if(prodCount > 0) {
                          //menuDesc.append(" <td colspan=\"3\"> </td></tr>");
                           
                     }
                     // prodCount++;
                     menuDesc.append(" <tr><td "+styleHeader+" valign=\"midde\" align\"center\" colspan=\"3\">"+majorStyle+"</td></tr>");
                     if(tableRow== 0) {
                         tableRow++;
                     }
                 }
                
                     menuDesc.append("<tr><td  colspan=\"3\">"+prodDesc.toString()+"</td></tr>");
                     prodCount++;
                     draftCount++;

                 

                 preStyle                   = majorStyle;
                 
                 if(styleMenus.size() > 0) {
                     countLimit             = 23;
                 }

                 if(prodCount>=countLimit) {
                      
                     menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = -1;
                     createTable            = true;
                     preStyle               = "";
                 }
             }

             if(prodCount > -1) {
                  menuDesc.append("</table>");
                     styleMenus.add(menuDesc.toString());
                     menuDesc               = new StringBuilder();
                     prodCount               = 0;
             }
             logger.debug("Size:"+styleMenus.size());
             String bblogo                  = "http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/BeerBoardLogo70.jpg";
             StringBuilder prodDesc                   = new StringBuilder();
             int size                       = styleMenus.size();
             for(int i=0; i < styleMenus.size(); i++) {
                  prodDesc                   = new StringBuilder();
                 if(menus.size()==0) {                     
                 }
                
                 prodDesc.append("<div><div style=\"vertical-align: top;\"><table   border=\"0\" width=\"100%\">");
                  if(i ==0  ) {
                      prodDesc.append(" <tr><td align=\"center\"><img src=\""+logo+"\" style=\"max-width:800px; max-height:70px;\"></td></tr>");
                      prodDesc.append(" <tr><td  "+styleHeader+">Now ON TAP</td></tr>");
                  } else {
                  }
                  prodDesc.append("<tr> <td>"+styleMenus.get(i)+"</td></tr>"); 
                       
                         if(i ==0) { 
                             //prodDesc.append("<tr><td align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></td></tr></table>");
                             //prodDesc.append("<div  style=\"position:absolute; float:right; right:20px; bottom:10px; z-index:2;\" align=\"right\" "+style3+">Our draft list is mobile<img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div>");                            
                               prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         } else {
                             // prodDesc.append(" <tr><td height=\"150px\" ></td></tr>");
                              prodDesc.append("</table></div> </div>");
                            // prodDesc.append("<div style=\"vertical-align: top; align:right;\"><div align=\"right\" "+style3+">Our draft list is mobile <img style=\"max-height:20px;\" src=\"" + bblogo + "\"></div></div></div>");
                         }


                 menus.add(prodDesc.toString());
                 //logger.debug(""+ prodDesc.toString() );
             }
        } catch (SQLException sqle) {
            logger.dbError("Database error: "+sqle.toString());
            throw new HandlerException(sqle);
        } catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             close(rs);
            close(stmt);
        }
        return menus;
     }
     
     public  ArrayList<String> getBeerBoardIndexTemplate(String customer, String location,String period,double keg,double average,int tapCount,int dayCount,Map<Integer,String> lineMap,Map<Integer,String> handleMap,Map<Integer,Double> handleValueMap, Element toAppend, MidwareLogger logger) throws HandlerException {

         ArrayList<String> menus            = new ArrayList<String>();
         ArrayList<String> styleMenus       = new ArrayList<String>();

         
         String color                       = "#000000";
         String  bgcolor                    = "#FFFFFF";

         logger.debug("BeerBoard Index Template");
         StringBuilder prodDesc             = new StringBuilder();
         try {
            
             
             String style1                  = "";
             String style2                  = "";
             
             String styleHeader             = "style=\" color: "+color+"; font-family:'Arial'; font-size:12pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             String styleName               = "style=\" color: "+color+"; font-family:'Arial'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:center;\"";
             style1                         = "style=\" color: "+color+"; font-family:'Arial'; font-size:9pt; font-weight:bold; word-wrap:break-word; text-align:left;\"";
             style2                         = "style=\" color: "+color+"; font-family:'Arial'; font-size:8pt;  word-wrap:break-word; text-align:left;\"";
             String style1Rught             = "style=\" color: "+color+"; font-family:'Arial'; font-size:10pt; font-weight:bold; word-wrap:break-word; text-align:right;\"";
             String styleT                  = "style=\" color: "+bgcolor+"; font-family:'Avantgarde'; font-size:8pt;  word-wrap:break-word; text-align:right;\"";

             StringBuilder menuDesc         = new StringBuilder();
             
            /*     5 stars: 	7 +
             4 stars: 	3  -  6.99
             3 stars: 	1  -  2.99
             2 stars: 	.4  -  .99
             1 stars: 	0   -  .399*/
             DecimalFormat df = new DecimalFormat("#.##");                
             prodDesc.append("<P style=\" font: italic 19px 'Arial';text-align: right;padding-right: 0px;margin-top: 0px;margin-bottom: 0px;\"><b>BeerBoard Index</b></P>"
                            + "<p style=\"  color: "+bgcolor+"; font: italic 1px 'Arial';\">-</p><div><P style=\" font: italic 10px 'Arial';text-align: right;padding-right: 0px;margin-top: 0px;margin-bottom: 0px;\">Period:<span style=\" font: italic 14px 'Arial';\"> "+period+"</span></P></div>"
                            + "<p style=\"  color: "+bgcolor+"; font: italic 1px 'Arial';\">-</p><div><P style=\" font: italic 10px 'Arial';text-align: right;padding-right: 0px;margin-top: 0px;margin-bottom: 0px;\">Avg. 30 day:  <span style=\" font: italic 14px 'Arial';\"> "+Double.valueOf(df.format(keg))+" kegs</span>  |    Avg. per line: <span style=\" font: italic 14px 'Arial';\"> "+Double.valueOf(df.format(average))+" kegs/tap</span></P></div>"
                            + "<P style=\"font: bold 16px 'Arial';text-align: left;padding-left: 0px;margin-bottom: 0px;\">"+location+"</P>");
             prodDesc.append("<table cellpadding=0 cellspacing=0><tr><td width='10%'>Line Index</td><td width='1%'></td<td width='40%'>Brand(s)</td>"
                + "<td  width='1%'>&nbsp;</td><td colspan=2 width='25%'style=\"background: #d9d9d9;\">Star Rating</td><td  width='1%'></td><td>Avg. Kegs/Month</td></tr>");
          toAppend.addElement("avgKeg").addText(String.valueOf(Double.valueOf(df.format(keg))));
          toAppend.addElement("avgTapKeg").addText(String.valueOf(Double.valueOf(df.format(average))));
          for (int i=1; i<=tapCount;i++) {
            int key                         =i;
            String handle                   = lineMap.get(key);
            String productName             = handleMap.get(key);
            double kegTurnOver              = handleValueMap.get(key);
            double kegAvg                   = (kegTurnOver/average)*100;  
            String star                        ="";
            String starLogo                     ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/";
            if(kegTurnOver >=7 ){
                star                        = "5";
                starLogo                    ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/5star.png";
            } else if(kegTurnOver >=3) {
                star                        = "4";
                starLogo                    ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/4star.png";
            } else if(kegTurnOver >=1) {
                star                        = "3";
                starLogo                    ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/3star.png";
            } else if(kegTurnOver >=.4) {
                star                        = "2";
                starLogo                    ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/2star.png";
            } else {
                star                        = "1";
                starLogo                    ="http://beerboard.tv/USBN.BeerBoard.UI/Images/Assets/pdf/1star.png";
            } 
            //logger.debug(key+" : "+productName+" : "+Double.valueOf(df.format(kegTurnOver)) +" : "+Double.valueOf(df.format(kegAvg))+" : "+ star);
            prodDesc.append("<tr><td></td><td></td><td>&nbsp;</td><td colspan=2></td><td></td></tr>");
            prodDesc.append("<tr><td>"+handle+"</td><td></td><td style=\"font: 14px 'Calibri';line-height: 18px; word-wrap:break-word;\">"+productName+"</td>"
                + "<td>&nbsp;</td><td  style=\" text-align: center;  width: 30px; background: #d9d9d9;\">"+star+"</td><td><img style=\" max-height:20px; max-width:200px;\" src=\""+starLogo+"\"/></td><td>&nbsp;</td><td>"+Double.valueOf(df.format(kegTurnOver))+"</td></tr>");
             Element indexEl         = toAppend.addElement("beerBoardIndex");
             indexEl.addElement("handle").addText(handle);
             indexEl.addElement("productName").addText(productName);
             indexEl.addElement("star").addText(star);
             indexEl.addElement("starLogo").addText(starLogo);
             indexEl.addElement("avg").addText(String.valueOf(Double.valueOf(df.format(kegTurnOver))));
          }
          menus.add(prodDesc.toString());

          
            
        }  catch (Exception e){
             logger.debug(e.getMessage());
        }finally {
             
        }
        return menus;
     }
    


        
    
}
