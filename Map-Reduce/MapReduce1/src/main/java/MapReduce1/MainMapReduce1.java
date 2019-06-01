package MapReduce1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainMapReduce1 {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Job1");

		job1.setJarByClass(MainMapReduce1.class);
		job1.setMapperClass(MapJobHSP.class);
		job1.setReducerClass(ReduceJobHSP.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		System.out.println(args[0] + " " + args[1]+ " " + args[2]);
		System.out.println(args[0] + " " + args[1]+ " " + args[2]);
		System.out.println(args[0] + " " + args[1]+ " " + args[2]);

		Path inputPathHSP = new Path(args[0]);
		Path inputPathHS = new Path(args[1]);
		Path outputPath = new Path(args[2]+"/job1");

		FileInputFormat.addInputPath(job1, inputPathHSP);
		FileOutputFormat.setOutputPath(job1, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath);
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "JoinTable");
		job2.setJarByClass(MainMapReduce1.class);
		job2.setMapperClass(MapFilterTop.class);
		job2.setReducerClass(ReduceFilterTop.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		Path inputPathFromJ1 = new Path(args[2]+ "/job1/part-r-00000");
		Path outputJob2 = new Path(args[2]+ "/job2");

		FileInputFormat.addInputPath(job2, inputPathFromJ1);
		FileOutputFormat.setOutputPath(job2, outputJob2);
		outputPath.getFileSystem(conf).delete(outputJob2);
		job2.waitForCompletion(true);

	}
}
