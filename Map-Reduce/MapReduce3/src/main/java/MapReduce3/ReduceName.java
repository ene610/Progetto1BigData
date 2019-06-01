package MapReduce3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceName extends Reducer<Text, Text, Text, Text>{


	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		int aumentoPercentuale2016 ;
		int aumentoPercentuale2017 ;
		int aumentoPercentuale2018 ;

		double firstClose2016 = 0;
		double firstClose2017 = 0;
		double firstClose2018 = 0;

		double lastClose2016 = 0;
		double lastClose2017 = 0;
		double lastClose2018 = 0;

		String sector = "";

		while(arg1.iterator().hasNext()) {

			String[] value = arg1.iterator().next().toString().split("\\|");

			firstClose2016 += Double.valueOf(value[1]);
			lastClose2016 += Double.valueOf(value[2]);

			firstClose2017 += Double.valueOf(value[3]);
			lastClose2017 += Double.valueOf(value[4]);

			firstClose2018 += Double.valueOf(value[5]);
			lastClose2018 += Double.valueOf(value[6]);

			sector = value[0];
		}
		if(!(firstClose2016==0 || firstClose2017==0 || firstClose2018==0)) {
			aumentoPercentuale2016 = (int) (((lastClose2016-firstClose2016)/firstClose2016)*100);
			aumentoPercentuale2017 = (int) (((lastClose2017-firstClose2017)/firstClose2017)*100);
			aumentoPercentuale2018 = (int) (((lastClose2018-firstClose2018)/firstClose2018)*100);

			String risultato = aumentoPercentuale2016 + "|" +aumentoPercentuale2017 + "|" +aumentoPercentuale2018 + "|" +
					sector + "|" + arg0.toString();

			arg2.write(new Text(), new Text(risultato));

		}
	}

}
