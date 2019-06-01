package MapReduce3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapName extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		Text chiave = new Text(fields[0].replace("\t", ""));
		Text valore = new Text(fields[1]+"|"+fields[2]+"|"+fields[3]+"|"+fields[4]+"|"+
				fields[5]+"|"+fields[6]+"|"+fields[7]);

		context.write(chiave, valore);
	}

}
