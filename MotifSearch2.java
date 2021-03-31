import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Writable;

public class MotifSearch2 {

  public static class ResultWritable implements Writable {

    private Text matchedMotif;
    private IntWritable dist;
    private IntWritable seqId;
    private IntWritable index;
    private IntWritable totalDist;

    public ResultWritable() {
      dist = new IntWritable(0);
      seqId = new IntWritable(0);
      index = new IntWritable(0);
      totalDist = new IntWritable(0);
      matchedMotif = new Text();
    }

    public void write(DataOutput output) throws IOException {
      matchedMotif.write(output);
      seqId.write(output);
      dist.write(output);
      index.write(output);
      totalDist.write(output);
    }

    public void readFields(DataInput in) throws IOException {
      matchedMotif.readFields(in);
      seqId.readFields(in);
      dist.readFields(in);
      index.readFields(in);
      totalDist.readFields(in);
    }

    public Text getMotif() {
      return matchedMotif;
    }

    public void setMotif(String text) {
      matchedMotif = new Text(text);
    }

    public IntWritable getDist() {
      return dist;
    }

    public void setDist(int val) {
      dist.set(val);
    }

    public IntWritable getSeq() {
      return seqId;
    }

    public void setSeq(int val) {
      seqId.set(val);
    }

    public IntWritable getIndex() {
      return index;
    }

    public void setIndex(int val) {
      index.set(val);
    }

    public IntWritable getTotalDist() {
      return totalDist;
    }

    public void setTotalDist(int val) {
      totalDist.set(val);
    } 

    @Override
    public String toString() {
      return matchedMotif + " " + dist + " " + seqId + " " + index + " " + totalDist;
    }
  }


  public static class TokenizerMapper extends Mapper<Object, Text, Text, ResultWritable>{

    private static final int length = 8;
    private Text word = new Text();
    private ResultWritable result = new ResultWritable();

    private static char getNextChar(char input) {
      if(input == 'a') return 'c';
      if(input == 'c') return 'g';
      if(input == 'g') return 't';
      else return 'a';
    }

    private static int GetDiff(char[] a, char[] b) {
      int diff = 0;
      for (int i = 0; i < a.length; i++) {
        if(a[i] != b[i]) diff++;
      }

      return diff;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      ArrayList<String> list = new ArrayList<String>();
      String str = "aaaaaaaa";
      char[] arr = str.toCharArray();
      int strpos = 0;

      while(!String.valueOf(arr).equals("tttttttt")) {
        list.add(String.valueOf(arr));
        arr[0] = getNextChar(arr[0]);
        while(arr[strpos] == 'a') {
          strpos++;
          arr[strpos] = getNextChar(arr[strpos]);
        }

        strpos = 0;
      }

      list.add(String.valueOf(arr)); // Get Full t's since above loop is not inculsive
      
      

      String line = value.toString();

      for(String motif : list) {
        int minDiff = 1000000; //INF
        int linePos = 0;
        String matchedMotif = "";
        int pos = 0;
        while (pos+8 <= line.length()) {
          char[] substr = line.substring(pos, pos+8).toCharArray();
          int newDiff = GetDiff(motif.toCharArray(), substr);
          if(newDiff <= minDiff) {
            minDiff = newDiff;
          }
          pos++;
        }
        word.set(motif);
        result.set(minDiff);
        context.write(word, result); 
      }
    }
  }

  public static class FinalMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    }
  }

  public static class ResultReducer extends Reducer<Text,ResultWritable,Text,ResultWritable> {
    
    private int lowest = 1000000;
    private IntWritable lowestVal = new IntWritable(0);
    private Text lowestKey;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      if(sum < lowest) {
        lowest = sum;
        lowestVal.set(lowest);
        lowestKey = new Text(key);
      }
    }

    
    public void cleanup(Context context) throws IOException, InterruptedException{
      context.write(lowestKey, lowestVal);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MotifSearch.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(ResultReducer.class);
    job.setReducerClass(ResultReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // job.waitForCompletion(true);
    // Configuration conf2 = new Configuration();
    // Job job2 = Job.getInstance(conf2, "Mapping The consensus string");
    // job2.setJarByClass(MotifSearch.class);
    // job2.setMapperClass();


    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}