// Matric Number: A0210908L
// Name: Moon Geonsik
// References : 
// WordCount.java
// https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {
	
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
		
		private Text word = new Text();
		private IntWritable whichFile = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, whichFile);
			}
		}
	}

	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable whichFile = new IntWritable(2);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, whichFile);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int firstFreq = 0;
			int secondFreq = 0;

			for (IntWritable val : values) {
				if (val.get().equals(1)) {
					firstFreq++;
					System.out.println("key : " + key + " value : " + val.get());
				} else {
					secondFreq++;
					System.out.println("key : " + key + " value : " + val.get());
				}
			}
			System.out.println("firstFreq : " + firstFreq + " secondFreq : " + secondFreq);
			if (firstFreq > 0 && secondFreq > 0) {
				result.set(firstFreq < secondFreq ? firstFreq : secondFreq);
				System.out.println("writing result for word : " + key);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top common words");
		job.setJarByClass(TopkCommonWords.class);
//		job.setMapperClass(TokenizerMapper.class);		
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper3.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper4.class);
		// MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
