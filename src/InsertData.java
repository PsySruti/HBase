import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool{

	public String Table_Name = "android";
	public String filePath = "/home/cloudera/Downloads/Android.json";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException, ArrayIndexOutOfBoundsException, StringIndexOutOfBoundsException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);   
        
        boolean isExists = admin.tableExists("asinList");
		
		if(isExists==false) {
			HTableDescriptor htb = new HTableDescriptor("asinList");
			HColumnDescriptor review = new HColumnDescriptor("review");
			
			htb.addFamily(review);
			admin.createTable(htb);
		}
        
        try {
    		@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(filePath));
    	    String line;
    	   
    	    int count = 0;
    	    //iterate over every line of the input file
    	    while((line = br.readLine()) != null) {
    	    	count++;
    	    	if(line.isEmpty())continue;
    	    	String reviewerID = "", asin ="", reviewerName = "",helpful = "",reviewerText="", overall="", summary="",unixReviewTime="",reviewTime="";
    	    	//System.out.println(line);
    	    	
    	    	reviewerID = line.substring(line.indexOf("reviewerID"), line.indexOf("asin"));
    	    		reviewerID =reviewerID.substring(reviewerID.indexOf(": \"") + 3, reviewerID.lastIndexOf("\"")-3);
    	    		if(line.contains("asin") && line.contains("reviewerName") ){
    	    				asin = line.substring(line.indexOf("asin"), line.indexOf("reviewerName"));
    	    				asin = asin.substring(asin.indexOf(": \"")+ 3, asin.lastIndexOf("\"")-3);
    	    				}
    	    		if(line.contains("\"reviewerName\"") && line.contains("\"helpful\"") ){
    	    			reviewerName = line.substring(line.indexOf("\"reviewerName\""), line.indexOf("\"helpful\""));
    	    			reviewerName = reviewerName.substring(reviewerName.indexOf(": \"")+ 3, reviewerName.length()-3);
	    				}
    	    		if(line.contains("\"helpful\"") && line.contains("\"reviewText\"") ){
    	    			helpful = line.substring(line.indexOf("\"helpful\""), line.indexOf("\"reviewText\""));
    	    			
    	    			helpful = helpful.substring(helpful.indexOf(": ")+ 2, helpful.length()-2);
	    				}
    	    		if(line.contains("\"reviewText\"") && line.contains("\"overall\"") ){
    	    			reviewerText = line.substring(line.indexOf("\"reviewText\""), line.indexOf("\"overall\""));
    	    			
    	    			reviewerText = reviewerText.substring(reviewerText.indexOf(": \"")+ 3, reviewerText.length()-3);
	    				}
    	    		if(line.contains("\"overall\"") && line.contains("\"summary\"") ){
    	    			overall = line.substring(line.indexOf("\"overall\""), line.indexOf("\"summary\""));
    	    			
    	    			overall = overall.substring(overall.indexOf(": ")+ 2, overall.length()-2);
	    				}
    	    		if(line.contains("\"summary\"") && line.contains("\"unixReviewTime\"") ){
    	    			summary = line.substring(line.indexOf("\"summary\""), line.indexOf("\"unixReviewTime\""));
    	    			
    	    			summary = summary.substring(summary.indexOf(": \"")+ 3, summary.length()-3);
	    				}
    	    		if(line.contains("\"unixReviewTime\"") && line.contains("\"reviewTime\"") ){
    	    			unixReviewTime = line.substring(line.indexOf("\"unixReviewTime\""), line.indexOf("\"reviewTime\""));
    	    			
    	    			unixReviewTime = unixReviewTime.substring(unixReviewTime.indexOf(": ")+ 2, unixReviewTime.length()-2);
	    				}
    	    		if(line.contains("\"reviewTime\"") ){
    	    			reviewTime = line.substring(line.indexOf("\"reviewTime\""), line.length());
    	    			
    	    			reviewTime = reviewTime.substring(reviewTime.indexOf(": \"")+ 3, reviewTime.length()-2);
	    				}
    	    		
    	    	double overallNum = Double.parseDouble(overall);
    	    	
    	    	
    	    	
    	    	//initialize a put with row key as tweet_url
	            Put put = new Put(Bytes.toBytes(count + "" ));
	            
	            //add column data one after one
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("reviewerID"), Bytes.toBytes(reviewerID));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("asin"), Bytes.toBytes(asin));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("reviewerName"), Bytes.toBytes(reviewerName));
	            
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("helpful"), Bytes.toBytes(helpful));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("reviewText"), Bytes.toBytes(reviewerText));
	            
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("overall"), Bytes.toBytes(overallNum));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("summary"), Bytes.toBytes(summary));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("unixReviewTime"), Bytes.toBytes(unixReviewTime));
	            put.add(Bytes.toBytes("review"), Bytes.toBytes("reviewTime"), Bytes.toBytes(reviewTime));
	            //add the put in the table
    	    	HTable hTable = new HTable(conf, Table_Name);
    	    	hTable.put(put);
    	    	hTable.close();  
    	    	System.out.println("Inserted " + count + " Inserted");
	    	}
    	    
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } //catch (ParseException e){
	    	// TODO Auto-generated catch block
	    	//e.printStackTrace(); } 
        catch (Exception e){
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    }

      return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}