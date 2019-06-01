package MapReduce1;

import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJobHSP extends Reducer<Text, Text, Text, Text>{ 

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		String dataMin= "";
		String dataMax = "";
		double firstClose = 0;
		double lastClose = 0;
		double minPrice=Double.MAX_VALUE;
		double maxPrice=Double.MIN_VALUE;
		int count = 0;
		long sumVolume = 0;

		while(values.iterator().hasNext()) {
			String[] value = values.iterator().next().toString().split("\\|");
			count++;
			if(dataMin.equals("")) {
				firstClose  = Double.valueOf(value[0]);
				lastClose = Double.valueOf(value[0]);
				dataMin = value[4];
				dataMax = value[4];
			}
			else {
				Date current_date = Date.valueOf(value[4]);
				Date first_close_date = Date.valueOf(dataMin);
				Date last_close_date = Date.valueOf(dataMax);

				if(first_close_date.after(current_date)) {
					dataMin = value[4];
					firstClose = Double.valueOf(value[0]);
				}

				if(last_close_date.before(current_date)) {
					dataMax = value[4];
					lastClose = Double.valueOf(value[0]);		
				}

				sumVolume += Long.valueOf(value[3]);

				if(minPrice>Double.valueOf(value[1]))
					minPrice = Double.valueOf(value[1]);
				if(maxPrice<Double.valueOf(value[2]))
					maxPrice = Double.valueOf(value[2]);

			}		
		}

		float avgVolume = sumVolume/count;
		int incrementoPercentuale = (int)(((lastClose-firstClose)/firstClose)*100);
		String valore = key.toString()+"|"+incrementoPercentuale+"|"+minPrice+"|"+maxPrice+"|"+avgVolume;
		arg2.write(new Text(), new Text(valore));
	}
}