/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.terakeet.soapware.handlers.report;

import net.terakeet.soapware.*;

/**
 *
 * @author nsharma
 */
public class AfterHoursSummaryStructure {

    private int _locationId                 = 0;
    private int _barId                      = 0;
    private int _productId                  = 0;
    private double _value;
    private String _locationName;
    private String _barName;
    private String _productName;

    public AfterHoursSummaryStructure() {

    }

    public AfterHoursSummaryStructure(int locationId, String locationName, int barId, String barName, int productId, String productName, double value){
        _locationId                         = locationId;
        _locationName                       = locationName;
        _barId                              = barId;
        _barName                            = barName;
        _productId                          = productId;
        _productName                        = productName;
        _value                              = value;
    }
    
    public int LocationId(){return _locationId;}
    
    public int BarId(){return _barId;}
    
    public int ProductId(){return _productId;}
    
    public String LocaionName(){return _locationName;}
    
    public String BarName(){return _barName;}
    
    public String ProductName(){return _productName;}
    
    public double Value(){return _value;}

}
