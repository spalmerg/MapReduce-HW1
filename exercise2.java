import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

    private FloatWritable col4 = new FloatWritable();
    private Text combo = new Text();
    
    public void configure(JobConf job) {
    }
    
    protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
   
    	  String[] line = value.toString().split(",");
   	  
    	  if(line[line.length - 1].equals("false")) {
    		  String[] subset = Arrays.copyOfRange(line, 29, 33);
    		  String stringSubset = Arrays.toString(subset);
    		  Float target = Float.parseFloat(line[3]); //col4 value and count
    		  
    		  combo.set(stringSubset);
    		  col4.set(target);
    		  output.collect(combo, col4);
    	  }
  }

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }
	

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

  public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	  float sum = 0;
	  int count = 0; 
	  while (values.hasNext()) {
		  sum += values.next().get();
		  count++;
	    }
	  
	  output.collect(key, new FloatWritable(sum/count));
  }

   protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
   }
    }
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), exercise2.class);
    conf.setJobName("exercise2");

    // conf.setNumReduceTasks(0);

    // conf.setBoolean("mapred.output.compress", true);
    // conf.setBoolean("mapred.compress.map.output", true);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(FloatWritable.class);

    conf.setMapperClass(Map.class);
//    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new exercise2(), args);
    System.exit(res);
  }
}