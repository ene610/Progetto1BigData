package MapReduce3;

import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJobOnHSP extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		String dataMin2016  = "";
		String dataMin2017  = "";
		String dataMin2018  = "";

		String dataMax2016 = "";
		String dataMax2017 = "";
		String dataMax2018 = "";

		String firstClose2016 = "0";
		String firstClose2017 = "0";
		String firstClose2018 = "0";

		String lastClose2016 = "0";
		String lastClose2017 = "0";
		String lastClose2018 = "0";

		while(values.iterator().hasNext()) {
			String[] value = values.iterator().next().toString().split("\\|");

			if(value[1].substring(0,4).equals("2016"))
				if(dataMin2016.equals("")) {
					dataMin2016 = value[1];
					dataMax2016 = value[1];
					firstClose2016 = value[0];
					lastClose2016 = value[0];
				}

				else {
					Date current_date = Date.valueOf(value[1]);
					Date first_close_date = Date.valueOf(dataMin2016);
					Date last_close_date = Date.valueOf(dataMax2016);

					if(first_close_date.after(current_date)) {
						dataMin2016 = value[1];
						firstClose2016 = value[0];
					}

					if(last_close_date.before(current_date)) {
						dataMax2016 = value[1];
						lastClose2016 = value[0];

					}
				}

			if(value[1].substring(0,4).equals("2017"))
				if(dataMin2017.equals("")) {
					dataMin2017 = value[1];
					dataMax2017 = value[1];
					firstClose2017 = value[0];
					lastClose2017 = value[0];
				}

				else {
					Date current_date = Date.valueOf(value[1]);
					Date first_close_date = Date.valueOf(dataMin2017);
					Date last_close_date = Date.valueOf(dataMax2017);

					if(first_close_date.after(current_date)) {
						dataMin2017 = value[1];
						firstClose2017 = value[0];
					}

					if(last_close_date.before(current_date)) {
						dataMax2017 = value[1];
						lastClose2017 = value[0];

					}
				}
			if(value[1].substring(0,4).equals("2018"))
				if(dataMin2018.equals("")) {
					dataMin2018 = value[1];
					dataMax2018 = value[1];
					firstClose2018 = value[0];
					lastClose2018 = value[0];
				}

				else {
					Date current_date = Date.valueOf(value[1]);
					Date first_close_date = Date.valueOf(dataMin2018);
					Date last_close_date = Date.valueOf(dataMax2018);

					if(first_close_date.after(current_date)) {
						dataMin2018 = value[1];
						firstClose2018 = value[0];
					}

					if(last_close_date.before(current_date)) {
						dataMax2018 = value[1];
						lastClose2018 = value[0];

					}	
				}
		}

		Text valore = new Text(key.toString()+"|"+firstClose2016+"|"+lastClose2016+"|"+
				firstClose2017+"|"+lastClose2017+"|"+
				firstClose2018+"|"+lastClose2018);
		arg2.write(new Text(),new Text(valore));
	}
}
