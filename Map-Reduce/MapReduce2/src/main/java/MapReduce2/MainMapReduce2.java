package MapReduce2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainMapReduce2 {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "tickerYearFirstLastClose");

		job1.setJarByClass(MainMapReduce2.class);
		job1.setMapperClass(MapJobOnHSP.class);
		job1.setReducerClass(ReduceJobOnHSP.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		Path inputPathHSP = new Path(args[0]);
		Path outputPath = new Path(args[2]+"/job1");
		Path inputPathHS = new Path(args[1]);

		FileInputFormat.addInputPath(job1, inputPathHSP);
		FileOutputFormat.setOutputPath(job1, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath);
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "JoinTable");
		job2.setJarByClass(MainMapReduce2.class);

		job2.setReducerClass(ReduceJoinHS.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		Path inputPathFromJ1 = new Path(args[2]+ "/job1/part-r-00000");
		Path outputJob2 = new Path(args[2]+ "/job2");

		MultipleInputs.addInputPath(job2, inputPathFromJ1,TextInputFormat.class, MapJoinHSP.class);
		MultipleInputs.addInputPath(job2, inputPathHS,TextInputFormat.class, MapJoinHS.class);


		FileOutputFormat.setOutputPath(job2, outputJob2);
		outputPath.getFileSystem(conf).delete(outputJob2);
		job2.waitForCompletion(true);

		Job job3 = new Job(conf, "reduceOnName");
		job3.setJarByClass(MainMapReduce2.class);

		job3.setReducerClass(ReduceSectorYear.class);
		job3.setMapperClass(MapSectorYear.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		Path pathFromJ2 = new Path(args[2]+ "/job2/part-r-00000");
		Path outputJob3 = new Path(args[2]+ "/job3");

		FileInputFormat.addInputPath(job3, pathFromJ2);
		FileOutputFormat.setOutputPath(job3, outputJob3);

		outputPath.getFileSystem(conf).delete(outputJob3);
		job3.waitForCompletion(true);

	}
}
