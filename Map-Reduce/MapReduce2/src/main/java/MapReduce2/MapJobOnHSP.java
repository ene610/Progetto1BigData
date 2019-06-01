package MapReduce2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJobOnHSP extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("[,]");

		if(!fields[0].equals("ticker")) {
			int anno = Integer.valueOf(fields[7].substring(0,4));
			Text chiave = new Text(fields[0] + "|" + fields[7].substring(0,4));
			Text valore = new Text(fields[2] + "|" +fields[7]+"|"+fields[6]);
			if(anno > 2003 && anno < 2019)
				context.write(chiave, valore);
		}
	}
}