package MapReduce3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCoppie extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().replace("\t", "").split("\\|");
		String chiave = fields[0] + "|" + fields[1] + "|"+ fields[2];
		String valore = fields[3] + "|" + fields[4];

		context.write(new Text(chiave),new Text(valore));
	}
}
