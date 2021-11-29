package com.mycompany.rangethresholding;

import java.sql.SQLException;

/**
 * @author aidanmartin
 */
public class EndpointTree {    
    public EndpointTree(SQL s) throws SQLException{
        String str;
        float[]arr=s.selectEndpoints(); //array of float
        int n=arr.length; // array size
        int threshold;
        for (int i = 0; i < n; i++){    //loop for bottom row
            double u=1000000;//Double.POSITIVE_INFINITY;//1/0F
            if(i+1<n)
                u=arr[i+1];
            str=String.valueOf(arr[i])+" AND RangeY<="+String.valueOf(u);   //get range
            threshold=(int)s.selectFieldWhere("sum(Threshold)", "Query", "RangeX>", str);
            s.selectFieldWhere("QueryId", "Query", "RangeX>", str);
            while(s.next()){    //get relaated queries
                str=s.rs.getInt(1)+","+(n+i);
                s.insert("Heap",str);
            }
            str=new StringBuilder().append(n+i).append(",0,").append(threshold).append(",").append(arr[i])
                    .append(",").append(u).append(",0,0").toString();
            s.insert("Node",str);   //insert the node 
        }
        float Up,Low;
        for (int i=n-1; i>0; --i){  //build up from bottom
            //n+1=initial arr
            Low=i << 1;
            Low=s.selectFieldWhere("RangeLower", "Node", "Ind", String.valueOf((int)Low));            //root=new Node(arr[i], u, threshold);
            Up=i << 1 | 1;
            Up=s.selectFieldWhere("RangeUpper", "Node", "Ind", String.valueOf((int)Up));  
            str=String.valueOf(Low)+" AND RangeY<="+String.valueOf(Up);
            if(String.valueOf(Up).equals("Infinity")){  //set upper as infinity
                str=String.valueOf(Low)+" AND <cast(RangeY as binary_double) = binary_double_infinity";
            }
            threshold=(int)s.selectFieldWhere("sum(Threshold)", "Query", "RangeX>", str); 
            s.selectFieldWhere("QueryId", "Query", "RangeX>", str);
            while(s.next()){
                str=s.rs.getInt(1)+","+i;
                s.insert("Heap",str);
            }
            str=new StringBuilder().append(i).append(",0,").append(threshold).append(",").append(Low)
                    .append(",").append(Up).append(",").append(i << 1).append(",").append(i << 1 | 1)
                    .toString();
            s.insert("Node",str);
        }
    }
    public void updateCounters(SQL s){  //reset counters
        s.updateAll("Node", "Weight", "0");
    }
}
