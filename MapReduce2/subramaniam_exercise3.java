import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class subramaniam_exercise3 {

  public static class Map
       extends Mapper<Object, Text, Text, NullWritable>{

    
    private Text artistInfo = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Boolean validYearFlag = false;
      String[] tokens = value.toString().split(",");
      String year = tokens[165];
      int intYear;
      validYearFlag = year.matches("\\d{4}");
      if(validYearFlag){
    	  intYear = Integer.parseInt(year);
    	  if(intYear >= 2000 && intYear <= 2010){
    		  artistInfo.set(tokens[2]+","+tokens[3]+","+tokens[1]);
    		  context.write(artistInfo, NullWritable.get());
    	  }
      }
    		  
      
    }
  }

  
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "subramaniam_exercise3");
    job.setJarByClass(subramaniam_exercise3.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    //job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

