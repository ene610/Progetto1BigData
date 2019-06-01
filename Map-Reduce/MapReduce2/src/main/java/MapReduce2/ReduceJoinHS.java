package MapReduce2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinHS extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ArrayList<String> hsp = new ArrayList<String>();
		String hs = "";

		while(arg1.iterator().hasNext()) {
			String[] fields = arg1.iterator().next().toString().split("\\|");
			if(fields[0].equals("HS"))
				hs = fields[1];
			else 
				hsp.add(fields[1]+"|"+fields[2]+"|"+fields[3]+"|"+fields[4]+"|"+fields[5]+"|"+fields[6]);	
		} 
		for(String valueHsp : hsp)
			if(!hs.equals(""))
				context.write(new Text(), new Text(hs + "|" + valueHsp));;
	}
}
