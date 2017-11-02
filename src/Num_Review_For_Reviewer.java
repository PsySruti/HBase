import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;


public class Num_Review_For_Reviewer {
	
public static String Table_Name = "android";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		
		int count = 0;
		double sum = 0;
		String asin = "",reviewerID = "",reviewerName="";
		
		//define the filter
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("review"), 
				Bytes.toBytes("reviewerID"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("A2HQWU6HUKIEC7")));
		
		//define the filter
				SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
						Bytes.toBytes("review"), 
						Bytes.toBytes("reviewerName"),
						CompareOp.EQUAL,
						new BinaryComparator(Bytes.toBytes("amazdnu")));
				
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			count ++;
			double overall=Bytes.toDouble(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("overall")));
			
			reviewerID =new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("reviewerID")));
			
			reviewerName=new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("reviewerName")));
			
			asin=new String(result.getValue(
					Bytes.toBytes("review"),
					Bytes.toBytes("asin")));
			//System.out.println("ReviewerID: " + reviewerID + " || ReviewerName: " + reviewerName +" || Overall: "+overall + " || Asin: "+asin);
		}
		System.out.println("ReviewerID: "+ reviewerID + " || ReviewerName: " + reviewerName +" || Number of Reviews: " + count);
    }
}
