package MapReduce3;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceCoppie extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		HashMap<String, String> nameSector= new HashMap<String, String>();

		while(arg1.iterator().hasNext()) {
			String fields[] = arg1.iterator().next().toString().split("\\|");

			nameSector.put(fields[1], fields[0]);
		}

		for(String nameAzienda1 : nameSector.keySet() ) {
			for(String nameAzienda2 : nameSector.keySet()) {
				if(!nameSector.get(nameAzienda1).equals(nameSector.get(nameAzienda2)))
					context.write(arg0,new Text(nameAzienda1 + "|" + nameAzienda2));
			}
		}

	}

}
