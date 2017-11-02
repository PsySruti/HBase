import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;


public class Num_Item_With_MaxOverall {

public static String Table_Name = "android";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		boolean isExists = admin.tableExists("asinList");
		
		if(isExists==false) {
			HTableDescriptor htb = new HTableDescriptor("asinList");
			HColumnDescriptor review = new HColumnDescriptor("review");
			
			htb.addFamily(review);
			admin.createTable(htb);
		}
		
		int count = 0;
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("review"), 
				Bytes.toBytes("overall"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(5.0)));
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
				count++;
			String overallNum=new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("overall")));
			
			String reviewerID =new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("reviewerID")));
			
			String reviewerName=new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("reviewerName")));
			
			String asin=new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("asin")));
			if(!asin.equals(""))
			{
			Put put = new Put(Bytes.toBytes(asin));
			put.add(Bytes.toBytes("review"),Bytes.toBytes("overall"), Bytes.toBytes(overallNum));
            //add the put in the table
	    	HTable oTable = new HTable(conf, "asinList");
	    	
	    	oTable.put(put);
	    	oTable.close();  
			}
	    	//System.out.println("Inserted " + count + " Inserted");
	    	
	    	
		}
		
		HTable cTable = new HTable(conf, "asinList");
		
		count = 0;
		
		Scan scan2 = new Scan();
		
		//now we extract the result
		scanner = cTable.getScanner(scan2);
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			
				count++;
			
			
		}
		System.out.println("Total Products with 5.0 Overall Score: " + count);

    }
}
