
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class subramaniam_exercise2 {

  public static class OneGramMap
       extends Mapper<Object, Text, Text, Text>{

    private Text keyDesc = new Text();
    private Text volume = new Text();
        
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	 String[] tokens = value.toString().split("\\s+");
    	Boolean validYearFlag = false;
        String yearonFile = tokens[1];
        int volumeonFile = Integer.parseInt(tokens[3]);
        validYearFlag = yearonFile.matches("^\\d+");
        if(validYearFlag){
    	        double vol = volumeonFile;
        		double vol_squared = vol * vol;
        		volume.set(Double.toString(vol)+", "+Double.toString(vol_squared));
        		keyDesc.set("Volumes");
        		context.write(keyDesc,volume);
        }
      }
    }
  
  public static class TwoGramMap
  extends Mapper<Object, Text, Text, Text>{

private Text keyDesc = new Text();
private Text volume = new Text();
   
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	
	Boolean validYearFlag = false;
    String[] tokens = value.toString().split("\\s+");
    int volumeonFile = 0;
 	String yearonFile = tokens[2];
 	volumeonFile = Integer.parseInt(tokens[4]);
    validYearFlag = yearonFile.matches("^\\d+");
    if(validYearFlag){
	    double vol = volumeonFile;
   		double vol_squared = vol * vol;
   		volume.set(Double.toString(vol)+", "+Double.toString(vol_squared));
   		keyDesc.set("Volumes");
   		context.write(keyDesc,volume);
   }
 }
}

  public static class Reduce
       extends Reducer<Text,Text,Text,DoubleWritable> {
    private DoubleWritable standard_dev = new DoubleWritable();
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      double sum_squared = 0;
      int counter = 0;
      for (Text val : values) {
    	  	String[] tokens = val.toString().split(",");
			counter ++;
			sum += Double.parseDouble(tokens[0]);
			sum_squared += Double.parseDouble(tokens[1]);
			}
      double average = sum/counter;
      double average_whole_square = Math.pow(average,2);
      double average_squared = sum_squared/counter;
      double difference = average_squared - average_whole_square;
      standard_dev.set(Math.sqrt(difference));
      context.write(new Text("Standard Deviation is: "), standard_dev);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "subramaniam_exercise2");
    job.setJarByClass(subramaniam_exercise2.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OneGramMap.class);
	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TwoGramMap.class);
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
