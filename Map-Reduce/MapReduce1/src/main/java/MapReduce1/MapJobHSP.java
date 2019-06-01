package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJobHSP extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("[,]");
		if(!fields[0].equals("ticker")) {
			int anno = Integer.valueOf(fields[7].substring(0,4));
			Text chiave = new Text(fields[0]);
			Text valore = new Text(fields[2] + "|" +fields[4]+ "|" +fields[5]+"|"+fields[6]+ "|" +fields[7]);
			if(anno > 1997 && anno < 2019)
				context.write(chiave, valore);
		}
	}
}