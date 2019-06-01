package MapReduce3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinHS extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields;

		if(value.toString().contains("\""))
			fields = value.toString().split(",\"|\",|,(?!.*\",|[ ])|,(?=[A-z]+,)");
		else
			fields = value.toString().split(",");

		Text chiave = new Text(fields[0]);
		Text valore = new Text("HS|"+fields[2] + "|" + fields[3]);

		if(!(fields[3].equals("sector")||fields[3].equals("N/A")))
			context.write(chiave,valore);
	}
}
