import java.io.BufferedReader;
import java.io.FileReader;
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
import org.apache.hadoop.util.StringUtils;

public class standardmap {

  public static class Map
       extends Mapper<Object, Text, Text, NullWritable>{
	  
	  ArrayList<Double> minVals = new ArrayList<Double>();
	  ArrayList<Double> maxVals = new ArrayList<Double>();
	  @Override
	  protected void setup(
	          Mapper<Object, Text, Text,NullWritable>.Context context)
	          throws IOException, InterruptedException {
	      if (context.getCacheFiles() != null
	              && context.getCacheFiles().length > 0) {
	    	  try{
                  BufferedReader fileIn = new BufferedReader(new FileReader("minmax.txt"));
                  String line;
                  int count = 0;
                  while ((line=fileIn.readLine()) != null){
                              String columns[] = line.split(",");
                              double[] columnValues = new double[columns.length];
                              for(int i = 0;i<columns.length;i++){
                            	  if(count==0){
                            		  minVals.add(Double.parseDouble(columns[i]));
                            	  }
                            	  else{
                            		  maxVals.add(Double.parseDouble(columns[i]));
                            	  }
                            	
                              }
                              count++;
                 } //while
                  fileIn.close();
          }//try
          catch (IOException ioe) {
                     System.err.println("Caught exception while getting cached file: " + StringUtils.stringifyException(ioe));
          }//catch
	      }
	      super.setup(context);
	  }
	  
	  private Text row = new Text();
 public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
	 String[] features = value.toString().split("\t");
	 String rowVal = ""; 
	 for(int i = 0;i<features.length;i++){
		 try{
		 double standardVal = (Double.parseDouble(features[i]) - minVals.get(i))/(maxVals.get(i) - minVals.get(i));
		 rowVal = rowVal + standardVal + " ";
	     
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
    Job job = Job.getInstance(conf, "standardmap");
    job.setJarByClass(standardmap.class);
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
    job.addCacheFile(new Path("/user/asubramaniam/medicare/input/minmax.txt").toUri());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



