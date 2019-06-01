package MapReduce2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSectorYear extends Reducer<Text, Text, Text, Text>{



	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		double sumFirstClose = 0;
		double sumLastClose = 0;
		long sumVolume = 0;
		float sumQuotazione = 0;
		int count = 0;

		while(arg1.iterator().hasNext()) {
			String fields[] = arg1.iterator().next().toString().split("\\|");
			sumFirstClose += Double.valueOf(fields[0]);
			sumLastClose += Double.valueOf(fields[1]);
			sumQuotazione += Float.valueOf(fields[2]);
			sumVolume += Long.valueOf(fields[4]);
			count += Integer.valueOf(fields[3]);
		}

		int aumentoPercentuale = (int) (((sumLastClose-sumFirstClose)/sumFirstClose)*100);
		float avgQuotazione = (sumQuotazione)/count;
		String chiave = arg0.toString().replace("|", " ");
		context.write(new Text(chiave),new Text(sumVolume + "|"+aumentoPercentuale +"|"+ avgQuotazione));
	}

}
