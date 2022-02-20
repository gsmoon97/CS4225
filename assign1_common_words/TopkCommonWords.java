// Matric Number: A0210908L
// Name: Moon Geonsik
// References : 
// WordCount.java
// https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
// https://stackoverflow.com/questions/42048028/mapreduce-use-hadoop-configuration-object-to-read-in-a-text-file

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

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
		
		Set<String> stopwords = new HashSet<String>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path path = new Path(conf.get("stopwords.path"));
		}
		
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
				if (val.equals(new IntWritable(1))) {
					firstFreq++;
				} else {
					secondFreq++;
				}
			}
			result.set(1);
			context.write(key, result);
			context.write(key, result);
//			int smallerFreq = (firstFreq < secondFreq) ? firstFreq : secondFreq;
//			
//			if (smallerFreq > 0) {
//				result.set(smallerFreq);
//				context.write(key, result);
//			}
//			if ((firstFreq > 0) && (secondFreq >= 0)) {
//				result.set((firstFreq < secondFreq) ? firstFreq : secondFreq);
//				if (firstFreq > secondFreq) {
//					result.set(firstFreq);
//				} else {
//					result.set(secondFreq);
//				}
//				context.write(key, result);
//			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("stopwords.path", args[2]);
		Job job = Job.getInstance(conf, "top common words");
		job.setJarByClass(TopkCommonWords.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
//