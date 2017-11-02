import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;


public class Num_Review_For_Day {
	
	public static String Table_Name = "android";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		int count = 0;
		String reviewTime = "";
		//define the filter
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("review"), 
				Bytes.toBytes("reviewTime"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("08 2, 2013")));
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			count++;
			double overall=Bytes.toDouble(result.getValue(
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
			
			reviewTime =new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("reviewTime")));
			
			//System.out.println("ReviewerID: " + reviewerID + " || ReviewerName: " + reviewerName +" || Overall: "+overall + " || Asin: "+asin +" || ReviewTime: "+reviewTime);
		}
		System.out.println("ReviewTime: "+reviewTime+" || Total Number of Reviews: "+ count);
    }
};
