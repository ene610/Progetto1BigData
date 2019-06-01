package MapReduce1;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFilterTop extends Mapper<LongWritable, Text, Text, Text> {

	private final int N  = 10; 
	private TreeMap<Integer, String> TopKMap = new TreeMap<Integer, String>() ;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().replace("\t", "").split("[|]");
		String valore = fields[0].replace("\t", "")+"|"+fields[1]+","+fields[2]+","+fields[3]+","+fields[4];
		TopKMap.put(Integer.valueOf(fields[1]), valore);

		if (TopKMap.size() > N) {
			TopKMap.remove(TopKMap.firstKey());
		}


	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for(String valore : TopKMap.values())
			context.write(new Text(), new Text(valore));
	}
}
