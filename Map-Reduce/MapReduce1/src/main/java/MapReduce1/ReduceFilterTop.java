package MapReduce1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceFilterTop extends Reducer<Text, Text, Text, Text>{



	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		while(arg1.iterator().hasNext()) {
			String[] fields = arg1.iterator().next().toString().split("\\|");
			String chiave = fields[0];
			String valore = fields[1];
			context.write(new Text(chiave),new Text(valore));
		}
	}
}
