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

//Please Note: This code is only for datasets that can fit into memeory
public class standard {

  public static class Map
       extends Mapper<Object, Text, IntWritable, Text>{

 public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     context.write(new IntWritable(1), value);
    }
  }
public static class Reduce
       extends Reducer<IntWritable,Text,Text,NullWritable> {
	  
	  
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    		//find min and max for each column
    		ArrayList<Double> minValues = new ArrayList<Double>();
    		ArrayList<Double> maxValues = new ArrayList<Double>();
    		ArrayList<double[]> rows = new ArrayList<double[]>();
    		for (Text val : values) {
    			try{
         	  	String[] columns = val.toString().trim().split("\t");
         	  	double[] rowValues = new double[columns.length];
         	  	//iterate through columns
         	  	for(int i =0;i<columns.length;i++){
         	  		double value = Double.parseDouble(columns[i]);
         	  		if(minValues.size()==columns.length){
         	  			if(value < minValues.get(i)){
         	  				minValues.set(i, value);
         	  		}
         	  			if(value > maxValues.get(i)){
         	  				maxValues.set(i, value);
         	  		}
         	  	 }
         	  		else{
         	  			minValues.add(value);
         	  			maxValues.add(value);
         	  		}
         	  		rowValues[i] = value;
         	  		}
         	  	rows.add(rowValues);
         	  	}catch(Exception e){
         	  	 System.out.println("Warning: exception "+e.getStackTrace().toString());
         	  	}
    		}
    			
    		//normalize values
    		//iterate through rows
    		for(int j =0;j<rows.size();j++){
    				double[] row = rows.get(j);
    				String rowString = "";
    				//iterate through columns
    				for(int i = 0;i<minValues.size();i++){
    				double normVal = (row[i]-minValues.get(i))/
    						  (maxValues.get(i)-minValues.get(i));
    				rowString = rowString + " " + normVal;
    			}
    			context.write(new Text(rowString),NullWritable.get());
    		}
    	}
    	
    }

  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "standard");
    job.setJarByClass(standard.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //conf.set("Flag",args[2]);
    //job.addCacheFile(new Path("/user/asubramaniam/hw4/input1/centroids.txt").toUri());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

