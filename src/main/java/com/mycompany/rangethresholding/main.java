package com.mycompany.rangethresholding;

import java.io.*;
import java.util.*;
import java.sql.SQLException;


/**
 * @author aidanmartin  am16br@my.fsu.edu
 * COP5725 Project: Range Thresholding on Streams
 */
public class main {
    static SQL s;
    public static void baseline() throws SQLException{  //baseline algorithm
        float v;
        int w;
        int i=0;
        while(s.count("Query")>0&&s.count("Element")>0){    //loop while queries and elements exist
            s.selectAll("Element");
            while(s.next()){    //loop elements
                i+=1;
                v=s.rs.getFloat(2);    //V(ei)
                w=s.rs.getInt(3);      //W(ei)
                s.tryUpdate("UPDATE Query SET Weight=Weight+"+String.valueOf(w)+" WHERE RangeX<="+String.valueOf(v)+" and RangeY>"+String.valueOf(v)+";");
                s.selectAllWhere("Query", "Weight>", "Threshold");
                while(s.next()){    //print alert where necessary
                    System.out.println("ALERT: "+s.rs.getInt(4)+" shares sold in the range ("+s.rs.getFloat(2)+","+s.rs.getFloat(3)+"].");
                }
                //update tables if where required
                s.tryUpdate("DELETE FROM Query WHERE Weight >= Threshold;");
                s.tryUpdate("DELETE FROM Element WHERE Id <="+i+";");
                break;
            }
            
        }  
    }
    public static void distributed_tracking(EndpointTree t) throws SQLException {
        //q coordinator, h participants
        int threshold, h, slack, signals, m, m_new;
        int w_e=0;
        int i=0;
        int index=1;
        float v_e=0;
        m=s.count("Query");
        while(s.count("Query")>0&&s.count("Element")>0){    //program loop while there are queries
            threshold=(int)s.selectFieldWhere("sum(Threshold)-sum(Weight)","Query","QueryId>","1");//held at first node
            h=s.count("Node");  //participants are sum of nodes
            int[] wp=new int[h+1];
            signals=0;
            Arrays.fill(wp, 0);
            s.selectAll("Element");
            if(threshold<6*h){  //update for all counter changes!
                if(s.next()){
                    v_e=s.rs.getFloat(2);    //V(ei)
                    w_e=s.rs.getInt(3);      //W(ei)
                    s.tryUpdate("UPDATE Query SET Weight=Weight+"+String.valueOf(w_e)+" WHERE RangeX<="+String.valueOf(v_e)+" and RangeY>"+String.valueOf(v_e)+";");
                    s.selectAllWhere("Query", "Weight>", "Threshold");
                    while(s.next()){ 
                        System.out.println("ALERT: "+s.rs.getInt(4)+" shares sold in the range ("+s.rs.getFloat(2)+","+s.rs.getFloat(3)+"].");
                    }
                    s.tryUpdate("DELETE FROM Query WHERE Weight >= Threshold;");
                    s.tryUpdate("DELETE FROM Element WHERE Id <="+i+";");
                }
            }else{      //use slack to ensure threshold decreases by threshold/3
                slack=(int)Math.floor(threshold/(2*h));
                while(signals<h && s.next()){   //begin round... q loop
                    i+=1;       //element count
                    v_e=s.rs.getFloat(2);    //V(ei)
                    w_e=s.rs.getInt(3);      //W(ei)
                    index=1;    //node index
                    while(index>0){ //endpoint tree binary seach
                        if(s.selectFieldWhere("RangeLower", "Node", "Ind", String.valueOf(index))<=v_e){
                            s.tryUpdate("UPDATE Node SET Weight=Weight+"+String.valueOf(w_e)+" WHERE Ind="+String.valueOf(index)+";");
                            w_e=(int)s.selectFieldWhere("Weight", "Node", "Ind", String.valueOf(index));
                            while(w_e-wp[index] >=slack){//or q declares end of round
                                wp[index]  += slack;  //weight increased by multiple of slack
                                signals+=1;           //send signal
                                if(signals>h)
                                    break;
                            }
                            //check right 
                            if(s.selectFieldWhere("RangeLower", "Node", "Ind", 
                                    String.valueOf(s.selectFieldWhere("RightChild", "Node", "Ind", String.valueOf(index))))<=v_e){
                                index=(int)s.selectFieldWhere("Ind", "Node", "Ind", String.valueOf(s.selectFieldWhere("RightChild", "Node", "Ind", String.valueOf(index))));
                            }
                            else{
                                index=(int)s.selectFieldWhere("Ind", "Node", "Ind", String.valueOf(s.selectFieldWhere("LeftChild", "Node", "Ind", String.valueOf(index))));
                            }
                        }else{
                            s.tryUpdate("DELETE FROM Element WHERE Id <="+i+";");
                            s.selectAll("Element");
                            break;
                        }
                    }
                    s.tryUpdate("DELETE FROM Element WHERE Id <="+i+";");
                    s.selectAll("Element");
                    //binary search on v_e, updating weights
                //send signal to coordinator/check if they signal end of round
                }   //end round
            }

            System.out.println("End of Round!");
            //update Queries based on Nodes
            for(int k=0;k<=s.count("Node");++k){
                String j=String.valueOf(k);
                int x=(int)s.selectFieldWhere("Weight","Node", "Ind", j);
                if(x>0){
                    int id=(int)s.selectFieldWhere("Query", "Heap", "Node", j);
                    if(id!=-1){
                        s.tryUpdate("UPDATE Query SET Weight=Weight+"+String.valueOf(x)+" WHERE QueryId="+id+";"); 
                    }
                }
            }
            s.tryUpdate("UPDATE Node SET Weight=0 WHERE Ind>0;");
            s.selectAllWhere("Query", "Weight>", "Threshold");
            while(s.next()){ 
                System.out.println("ALERT: "+s.rs.getInt(4)+" shares sold in the range ("+s.rs.getFloat(2)+","+s.rs.getFloat(3)+"].");
            }
            s.tryUpdate("DELETE FROM Query WHERE Weight >= Threshold;");
            m_new=s.count("Query");
            if(m_new<=(m/2) || m_new>=Math.pow(m,2)){
                System.out.println("Rebuild");
                m=m_new;
                s.tryUpdate("DELETE FROM Node WHERE Id > 0;");
                t=new EndpointTree(s);
            }
        }   //start next round
    }
    
    public static void initialize(){
        System.out.println("Creating Tables ... ");
        s.createTable("Element", "Id int IDENTITY(1,1) NOT NULL PRIMARY KEY, Value float NOT NULL, Weight float NOT NULL,Time int NOT NULL");
        s.createTable("Query", "QueryId int IDENTITY(1,1) NOT NULL PRIMARY KEY, RangeX float NOT NULL, RangeY float NOT NULL,  Threshold int NOT NULL, Weight int DEFAULT 0, ActiveFrom int");
        s.createTable("Node", "Id int IDENTITY(1,1) NOT NULL PRIMARY KEY, Ind int, Weight int NOT NULL,Threshold int NOT NULL, RangeLower float NOT NULL,RangeUpper float NOT NULL, LeftChild int NOT NULL, RightChild int NOT NULL");
        s.createTable("Heap", "Id int IDENTITY(1,1) NOT NULL PRIMARY KEY, Query int NOT NULL, Node int NOT NULL");
        load_reset();
    }
    public static void load_reset(){
        s.tryUpdate("DELETE FROM Element WHERE Id >0;");
        System.out.println("NEED TO LOAD ELEMENTS/DATASTREAM ... ");
        fill();
        s.tryUpdate("DELETE FROM Query WHERE QueryId >0;");
        System.out.println("LOADING QUERIES ...");
        s.insert("Query","0.0625, 0.125, 250, 0, 0),(0.1875, 0.25, 480, 0, 0),(0.3125, 0.375, 250, 0, 0),(0.4375, 0.5, 500, 0, 0),(0.5625, 0.625, 300, 0, 0),(0.6875, 0.75, 10, 0, 0),(0.8125, 0.875, 350, 0, 0),(0.9375, 1.0, 10, 0, 0");
    }
    
    public static void fill(){  //insert into DB from file input
        String values;
        try{
            FileInputStream fstream = new FileInputStream("elements.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null)   {
                String[] tokens = strLine.split(" ");
                values= new StringBuilder().append(tokens[0]).append(",").append(tokens[1]).append(",1").toString();
                s.insert("Element", values);
            }
            in.close();
        } catch (Exception e){
          System.err.println("Error: " + e.getMessage());
        }
        /*
        try{
            FileInputStream fstream = new FileInputStream("queries.txt");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null)   {
                String[] tokens = strLine.split(" ");
                values= new StringBuilder().append(tokens[0]).append(",").append(tokens[1]).append(",").append(tokens[2]).append(",0,1").toString();
                s.insert("Query", values);
            }
            in.close();
        } catch (Exception e){
          System.err.println("Error: " + e.getMessage());
        }
        */
        
    }
    
    
    public static void main(String[] args) throws SQLException {
        System.out.println("Connecting to SQL Server ... ");
        s=new SQL("localhost", "1433", "sa","AidanMartin1", "COP");
        //s=new SQL(args[0], args[1], args[2],args[3], args[4]);    //I4getCMD_args       
        System.out.println("Done.");
        initialize();   //make DB, load data
        System.out.println("TESTING: Baseline ... ");   //different sizes?
        long start1 = System.nanoTime();    //start timer
        baseline();
        long end1 = System.nanoTime();   
        System.out.println("Runtime nanoseconds: "+ (end1-start1));      
        
        System.out.println("Resetting data ... ");
        initialize();   //Reset Query/Node, same data
        System.out.println("TESTING: Distributed Tracking ... ");
        EndpointTree t=new EndpointTree(s);
        long start2 = System.nanoTime();      //start timer
        distributed_tracking(t);
        long end2 = System.nanoTime(); 
        System.out.println("Runtime nanoseconds: "+ (end2-start2));
    }
}
