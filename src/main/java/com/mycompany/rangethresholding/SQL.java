package com.mycompany.rangethresholding;

/**
 * @author aidanmartin
 */

import java.sql.*;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class SQL{
    @SuppressWarnings("unused")
    private Driver SQLdriver;
    public String err = null;
    private Connection con;
    private Statement stmt;
    public ResultSet rs = null;   
    public boolean open = false;
//Connect to SQL server
    public SQL(String host, String port, String user, String pass, String db){
        try {
            con = DriverManager.getConnection("jdbc:sqlserver://"+host+":"+port+";databaseName=master;user="+user+";password="+pass);
            stmt = con.createStatement();
            con.setAutoCommit(true);
            tryUpdate("DROP DATABASE IF EXISTS ["+db+"]; CREATE DATABASE ["+db+"]");
            tryUpdate("USE ["+db+"];");
            open = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//Close SQL connnection 
    public boolean close(){
        if (!open) 
            return true;
        try {
            getConnection().close();
            open = false;
            return true;
        } catch (SQLException e) {
            err = e.toString();
            return false;
        }
    }
//getters  
    public Connection getConnection(){
        return con;         //getter for SQL connection
    }
    public Statement getStmt(){
        return stmt;         //getter for SQL statement
    }
//work with database      
    public boolean tryUpdate(String str){
        try {       //try to modify/update DB with query in string
            getStmt().executeUpdate(str);
            return true;
        } catch (Exception e) {
            err = e.toString();
            return false;
        }
    }
    public boolean trySelect(String str){
        try {       //try to select data using query in string
            rs = getStmt().executeQuery(str);   //put into result set
            return true;
        } catch (Exception e) {
            rs = null;
            err = e.toString();
            return false;
        }
    }
//work with result set
    public boolean next(){
        try {   //check for next tuple
            return rs.next();
        } catch (SQLException e) {
            err = e.toString();
            return false;
        }
    }
//Data Definition-Create/delete tables 
    boolean createTable(String table, String fields){
        dropTable(table);
        return tryUpdate("CREATE TABLE "+table+" ("+fields+");");
    }
     boolean dropTable(String table){
        return tryUpdate("DROP TABLE "+table+";");
    }
     
    public int count(String table){ //return the number of tuples in a table
        trySelect("SELECT Count(*) FROM "+table+";");
        try {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            err = e.toString();
            return -1;
        }
    }

    public float[] selectEndpoints(){   //from queries to build endpoint tree
        int i;
        trySelect("SELECT COUNT(DISTINCT RangeX) FROM (SELECT DISTINCT RangeX FROM Query UNION SELECT DISTINCT RangeY FROM Query) AS c;");
        try {
            rs.next();
            i=rs.getInt(1);
        } catch (SQLException e) {
            err = e.toString();
            i=0;
        }
        float[] endpoints=new float[i];
        if(i!=0){
            i=0;
            trySelect("SELECT DISTINCT RangeX FROM Query UNION SELECT DISTINCT RangeY FROM Query;");
            while(next()){
                try {   //return array of endpoints
                    endpoints[i]=rs.getFloat(1);
                    i+=1;
                } catch (SQLException e) {
                    Logger.getLogger(SQL.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return endpoints;   //return as array of floats
    }
//SELECTION STATEMENTS
    public int selectAll(String table) {
        trySelect("SELECT * FROM "+table+";");
        try {
            rs.first();
            return rs.getInt(1);
        } catch (SQLException e) {
            err = e.toString();
            return -1;
        }
    }
    public boolean selectAllWhere(String table, String column, String value) {
        return trySelect("SELECT * FROM "+table+" WHERE "+column+"="+value+";");
    }
    public float selectFieldWhere(String field,String table, String column, String value) {
        trySelect("SELECT "+field+" FROM "+table+" WHERE "+column+"="+value+";");
        try {
            rs.next();
            return rs.getFloat(1);
        } catch (SQLException e) {
            err = e.toString();
            return -1;
        }
    }
    public int[] selectFieldsWhere(String field,String table, String column, String value) {
        int i;
        trySelect("SELECT COUNT("+field+") FROM "+table+" WHERE "+column+"="+value+";");
        try {
            rs.next();
            i=rs.getInt(1);
        } catch (SQLException e) {
            err = e.toString();
            i=0;
        }
        int[] ret=new int[i];
        if(i!=0){
            i=0;
            trySelect("SELECT "+field+" FROM "+table+" WHERE "+column+"="+value+";");
            while(next()){
                try {   //return array of endpoints
                    ret[i]=rs.getInt(1);
                    i+=1;
                } catch (SQLException e) {
                    Logger.getLogger(SQL.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return ret;   //return as array of floats
    }
//update tuples in database  
    public boolean updateWhere(String table, String column, String value, String values) {
        return tryUpdate("UPDATE "+table+" SET "+values+" WHERE "+column+"="+value+";");
    }
    public boolean updateAll(String table, String column, String value) {
        return tryUpdate("UPDATE "+table+" SET "+column+"="+value+";");
    }
 //insert into database   
    public boolean insert(String table, String values) {
        return tryUpdate("INSERT INTO "+table+" VALUES ("+values+");");
    }
}