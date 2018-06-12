import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable maxTemp = new IntWritable(1);
    private Text year = new Text();
    
    public void configure(JobConf job) {
    }
    
    protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
  
    	  String line = value.toString();
    	  String quality = line.substring(92,93);
    	  String[] passable =  {"0", "1", "4", "5", "9"};
    	  
    	  if(Arrays.asList(passable).contains(quality)) {
    		  String temp = line.substring(87,92);
    		  int max = Integer.parseInt(temp);
    		  if(max != 9999) {
    			  maxTemp.set(max);
    			  year = new Text(line.substring(15,19));
    			  output.collect(year, maxTemp);
    		  }
    	  }
  }

	protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}
    }
	

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}

  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	  int max = -100;
    int value = -100;
    while (values.hasNext()) {
      value = values.next().get();
      if(value > max){
        max = value;
      }
    }
    output.collect(key, new IntWritable(max));
  }

   protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
   }
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), exercise1.class);
    conf.setJobName("exercise1");

    // conf.setNumReduceTasks(0);

    // conf.setBoolean("mapred.output.compress", true);
    // conf.setBoolean("mapred.compress.map.output", true);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new exercise1(), args);
    System.exit(res);
  }
}
