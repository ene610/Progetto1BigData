package MapReduce3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinHS extends Reducer<Text, Text, Text, Text>{

	HashMap<String, String> hs = new HashMap<String, String>();
	HashMap<String, String> hsp = new HashMap<String, String>();

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String hs = "";
		String hsp = "";

		while(arg1.iterator().hasNext()) {
			String alba = arg1.iterator().next().toString();
			String[] fields = alba.split("\\|");

			if(fields[0].equals("HS"))
				hs = fields[1]+"|"+fields[2];

			else 
				hsp = fields[1]+"|"+fields[2]+"|"+fields[3]+"|"+fields[4]+"|"+fields[5]+"|"+fields[6];

		}
		//messa questa if poichè sono scartate le hs con sect : N/A e le hsp con firstclose = 0 
		if(!hs.equals("") && !hsp.equals(""))
			context.write(new Text(), new Text(hs + "|" + hsp));;
	}
}
