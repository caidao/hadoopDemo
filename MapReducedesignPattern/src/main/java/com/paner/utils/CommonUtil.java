package com.paner.utils;

import org.dom4j.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
            Iterator<Attribute> item = document.getRootElement().attributeIterator();
            while (item.hasNext()){
                Attribute attribute = item.next();
                map.put(attribute.getName(),attribute.getValue());
            }
//            map.put("Id",document.getRootElement().attributeValue("Id"));
//            map.put("PostId",document.getRootElement().attributeValue("PostId"));
//            map.put("Score",document.getRootElement().attributeValue("Score"));
//            map.put("CreationDate",document.getRootElement().attributeValue("CreationDate"));
//            map.put("UserId",document.getRootElement().attributeValue("UserId"));
            return map;
        } catch (DocumentException e) {
        }

        return new HashMap<String, String>();
    }

    public static Element getXmlElemetFromString(String value){
        try {
            Document document = DocumentHelper.parseText(value);
            return document.getRootElement();
        } catch (DocumentException e) {

        }
        return null;
    }

    public static void copyAttrubutesToElement(List attrList,Element element){
         element.setAttributes(attrList);

    }

    public static String transformDocToString(Document document){
        return document.asXML();
    }

    private static  Map<String,String> initMap(){
       Map<String,String> map = new HashMap<String, String>();
       map.put("Id",null);
       map.put("PostId",null);

       return map;
    }

}
