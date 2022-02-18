// Matric Number: A0210908L
// Name: Moon Geonsik
// References : 
// WordCount.java
// https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
// https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program

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

public class TopkCommonWords{

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text file = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String fileName = fileSplit.getPath().getName();
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        file.set(fileName);
        context.write(word, file);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int smallerFreq = 0;
      HashMap<Text, Integer> commonWordMap = new HashMap<>();
      for (Text val : values) {
        int count = commonWordMap.getOrDefault(val, 0);
        count++;
        commonWordMap.put(val, count);
      }
      smallerFreq = Collections.min(commonWordMap.values());
      if (commonWordMap.size() > 1) {
        result.set(smallerFreq);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "top common words");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
    // MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
