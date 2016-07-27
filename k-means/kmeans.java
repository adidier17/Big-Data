
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class kmeans {

  public static class Map
       extends Mapper<Object, Text, IntWritable, Text>{
	   ArrayList<double[]> centroids = new ArrayList<double[]>();
	  
	   @Override
	  protected void setup(
	          Mapper<Object, Text, IntWritable,Text>.Context context)
	          throws IOException, InterruptedException {
	      if (context.getCacheFiles() != null
	              && context.getCacheFiles().length > 0) {
	    	  try{
                  BufferedReader fileIn = new BufferedReader(new FileReader("centroids.txt"));
                  String line;
                  while ((line=fileIn.readLine()) != null){
                              String columns[] = line.split(",");
                              double[] columnValues = new double[columns.length];
                              for(int i = 0;i<columns.length;i++){
                            	  columnValues[i] = Double.parseDouble(columns[i]);
                              }
                              centroids.add(columnValues);
                             
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
    private final static IntWritable cluster = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] features = value.toString().trim().split("\\s+");
	  double variable;
	  ArrayList<Double> distances = new ArrayList<Double>();
	  for(int j =0;j<centroids.size();j++){
		  double distance = 0;
	      for(int i = 0;i<features.length;i++){
		     variable = Double.parseDouble(features[i]);
		     distance += Math.pow((variable - centroids.get(j)[i]), 2);
		   }
	  distances.add(distance);
	}
	  int minDistCluster = distances.indexOf(Collections.min(distances));
	  // Emit the nearest center and the row
	  row.set(value.toString());
	  cluster.set(minDistCluster);
      context.write(cluster,row );
     }
  }
  
public static class Reduce
       extends Reducer<IntWritable,Text,Text,NullWritable> {
	  
	  
    Text newCentroids = new Text();
    /*
	 * Reduce function will emit all the points allocated to each cluster and calculate
	 * the new centroid for these points
	 */
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	//update centroids
    	int counter = 0;
        ArrayList<Double> rowSums = new ArrayList<Double>();
        String newCentroid = "";
        //iterate through rows
    	for (Text val : values) {
     	  	String[] columns = val.toString().trim().split("\\s+");
     	  	counter++;
     	  	//iterate through columns
     	  	for(int i = 0;i<columns.length;i++){
     	  		if(rowSums.size()==columns.length){
     	  			rowSums.set(i,rowSums.get(i)+ Double.parseDouble(columns[i]));
     	  		}
     	  		//first row
     	  		else{
     	  			rowSums.add(Double.parseDouble(columns[i]));
     	  		}
     	  	}
    	}
    	//means of each column
    	for(int i = 0;i<rowSums.size();i++){
    		newCentroid = newCentroid + (rowSums.get(i)/counter) + ",";
    	}
    	//remove trailing ','
    	newCentroid = newCentroid.substring(0,newCentroid.length()-1);
    	newCentroids.set(newCentroid);
            context.write(newCentroids,NullWritable.get());
    	}
    }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "kmeans");
    job.setJarByClass(kmeans.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    //job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //conf.set("Flag",args[2]);
    job.addCacheFile(new Path("/user/asubramaniam/hw4/inputCentroids/centroids.txt").toUri());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

