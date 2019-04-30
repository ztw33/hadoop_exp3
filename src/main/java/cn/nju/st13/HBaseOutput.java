package cn.nju.st13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class HBaseOutput {
	public static void main(String[] args) {
		try {
			Configuration hconf = HBaseConfiguration.create();
			HTable table = new HTable(hconf,"WuXia");
			Scan scan = new Scan();
			scan.setMaxVersions();
			ResultScanner scanner = table.getScanner(scan);
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(args[0])));
			for(Result result : scanner) {
				String word = new String(result.getRow());
				String avgFrequency = new String(result.getValue(Bytes.toBytes("content"),Bytes.toBytes("avgFrequency")));
				bufferedWriter.write(word+"\t"+avgFrequency);
				bufferedWriter.newLine();
			}
			bufferedWriter.flush();
			bufferedWriter.close();
			scanner.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}
}
