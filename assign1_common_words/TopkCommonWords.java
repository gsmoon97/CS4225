// Matric Number: A0210908L
// Name: Moon Geonsik
// References : 
// 	WordCount.java
// 	https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
// 	https://stackoverflow.com/questions/26209773/hadoop-map-reduce-read-a-text-file
// 	https://stackoverflow.com/questions/5911174/finding-key-associated-with-max-value-in-a-java-map
// 	https://stackoverflow.com/questions/21105413/java-java-util-map-entry-and-collection-sortlist-comparator

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
		HashSet<String> stopwords = new HashSet<>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path path = new Path(conf.get("stopwords.path"));
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))); 
			String stopword = br.readLine();
			while (stopword != null) {
				stopwords.add(stopword);
				stopword = br.readLine();
			}
		}
		
		private Text word = new Text();
		private IntWritable whichFile = new IntWritable(1); // first file

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if(!stopwords.contains(word.toString())) {
					context.write(word, whichFile);
				}
			}
		}
	}

	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		HashSet<String> stopwords = new HashSet<>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path path = new Path(conf.get("stopwords.path"));
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))); 
			String stopword = br.readLine();
			while (stopword != null) {
				stopwords.add(stopword);
				stopword = br.readLine();
			}
		}
		
		private Text word = new Text();
		private IntWritable whichFile = new IntWritable(2); // second file

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if(!stopwords.contains(word.toString())) {
					context.write(word, whichFile);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
		private Map<String, Integer> wordMap;
		private int topK = 20;

		@Override
	    public void setup(Context context) 
	    		throws IOException, InterruptedException {
			wordMap = new HashMap<>();
	    }
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int firstFreq = 0;
			int secondFreq = 0;
			
			for (IntWritable val : values) {
				if (val.get() == 1) {
					firstFreq++;
				} else {
					secondFreq++;
				}
			}
			
			int smallerFreq = (firstFreq < secondFreq) ? firstFreq : secondFreq; // consider the smaller frequency
			if (smallerFreq > 0) {
				wordMap.put(key.toString(), smallerFreq);
			}
		}
		
		@Override
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			while (topK > 0) {
				int maxFreq = Collections.max(wordMap.values());
				
				List<Map.Entry<String, Integer>> maxFreqWords = new ArrayList<>();
				wordMap
					.entrySet()
					.stream()
					.filter(e -> e.getValue() == maxFreq)
					.forEach(e -> maxFreqWords.add(e)); // common words with maximum frequency
				Collections.sort(maxFreqWords, (e1, e2) -> e2.getKey().compareTo(e1.getKey())); // descending lexicographic order
				
				for (Map.Entry<String, Integer> e : maxFreqWords) {
					if(topK < 0) {
						return; // return top 20 common words
					}
					Text word = new Text(e.getKey());
					IntWritable freq = new IntWritable(e.getValue());
					context.write(freq, word); //swap the order for output
					topK--;
				}
				wordMap.entrySet().removeIf(e -> e.getValue() == maxFreq);
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("stopwords.path", args[2]);
		
		Job job = Job.getInstance(conf, "top common words");
		job.setJarByClass(TopkCommonWords.class);
		
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
//