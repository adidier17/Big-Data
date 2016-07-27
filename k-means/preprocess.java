import java.io.IOException;
import java.util.ArrayList;
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

public class preprocess {

  public static class Map
       extends Mapper<Object, Text, Text, NullWritable>{
	  
	  private Text row = new Text();
 public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
	 String[] features = value.toString().split("\t");
	 String rowVal = ""; 
	 for(int i = 19;i<features.length;i++){
		 try{
			 if(i!=20 && i!=21){
		 rowVal = rowVal + features[i] + '\t';
		 }
		 }catch(Exception e){
			 System.out.println("Warning: exception "+e.getStackTrace().toString());
		 }
	 }
	 if(rowVal!=""){
	 row.set(rowVal);
	 context.write(row,NullWritable.get());
	 }
    }
  }
public static class Reduce
       extends Reducer<IntWritable,Text,Text,NullWritable> {
	  
	  
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    		}
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "preprocess");
    job.setJarByClass(preprocess.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    //job.setReducerClass(Reduce.class);
    //job.setMapOutputKeyClass(IntWritable.class);
    //job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //conf.set("Flag",args[2]);
    //job.addCacheFile(new Path("/user/asubramaniam/hw4/input1/centroids.txt").toUri());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


