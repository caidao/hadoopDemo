package com.paner.common;


import java.sql.*;

public class HiveClient {




    public static Connection getConnection(){
        Connection connection = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://10.104.111.33:10000/default;","master","");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void main(String[] args) throws SQLException {
        Connection cnn = HiveClient.getConnection();
        Statement statement = cnn.createStatement();
        ResultSet rs = statement.executeQuery("show databases");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        cnn.close();
    }

}
