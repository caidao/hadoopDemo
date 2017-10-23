package com.paner.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.dom.DOMDocument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @User: paner
 * @Email: panyiwen2009@gmail.com
 * @Date: 17/10/15 下午9:42
 */
public class CommonUtil {

    public static Map<String,String> transformXmlToMap(String value){
       // System.out.println("value = [" + value + "]");
        Map<String,String> map = new HashMap<String, String>();
        try {
            Document document = DocumentHelper.parseText(value);
            if (!"row".equals(document.getRootElement().getName())){
                return null;
            }
            map.put("Id",document.getRootElement().attributeValue("Id"));
            map.put("PostId",document.getRootElement().attributeValue("PostId"));
            map.put("Score",document.getRootElement().attributeValue("Score"));
            map.put("CreationDate",document.getRootElement().attributeValue("CreationDate"));
            map.put("UserId",document.getRootElement().attributeValue("UserId"));
            return map;
        } catch (DocumentException e) {
        }

        return new HashMap<String, String>();
    }


    private static  Map<String,String> initMap(){
       Map<String,String> map = new HashMap<String, String>();
       map.put("Id",null);
       map.put("PostId",null);

       return map;
    }

}
