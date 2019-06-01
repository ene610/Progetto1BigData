package MapReduce2;

import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJobOnHSP extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		String dataMin= "";
		String dataMax = "";
		String firstClose = "";
		String lastClose = "";
		float sumQuotazione=0;
		int count = 0;
		long sumVolume = 0;

		while(values.iterator().hasNext()) {
			String[] value = values.iterator().next().toString().split("\\|");
			count++;
			sumQuotazione += Float.valueOf(value[0]);
			sumVolume += Long.valueOf(value[2]);

			if(dataMin.equals("")) {
				firstClose  = value[0];
				lastClose = value[0];
				dataMin = value[1];
				dataMax = value[1];
			}
			else {
				Date current_date = Date.valueOf(value[1]);
				Date first_close_date = Date.valueOf(dataMin);
				Date last_close_date = Date.valueOf(dataMax);

				if(first_close_date.after(current_date)) {
					dataMin = value[1];
					firstClose = value[0];
				}

				if(last_close_date.before(current_date)) {
					dataMax = value[1];
					lastClose = value[0];

				}
			}
		}
		String valore = key.toString()+"|"+firstClose+"|"+lastClose+"|"+sumQuotazione+"|"+count+"|"+sumVolume;
		arg2.write(new Text(), new Text(valore));
	}
}