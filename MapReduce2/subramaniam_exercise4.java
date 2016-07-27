import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class subramaniam_exercise4 {

  public static class Map
       extends Mapper<Object, Text, Text, DoubleWritable>{

    
    private Text artistInfo = new Text();
    private final static DoubleWritable duration = new DoubleWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] tokens = value.toString().split(",");
      String year = tokens[165];
      String artist = tokens[2];
      //capitalize first letter so that sorting is consistent
      artist = artist.substring(0,1).toUpperCase() + artist.substring(1);
      artistInfo.set(artist);
      duration.set(Double.parseDouble(tokens[3]));
      context.write(artistInfo, duration);
     }
  }
  
//Partitioner class
	
  public static class KeyPartitioner extends
  Partitioner < Text, DoubleWritable >
  {
     @Override
     public int getPartition(Text key, DoubleWritable value, int numReduceTasks)
     {
        String artistName = key.toString().toUpperCase();
        char firstChar = artistName.charAt(0);
        //send non-letter names to first reduce task
        if(!(Character.isLetter(firstChar))){
        	return 0;
        }
        else{
        	int firstAscii = (int)firstChar;
        	//any character other than a-y goes to the last reduce task
        	if(firstAscii>=90){
        		return 4;
        	}
        	//65%5 = 13; subtract 13 to start from 0
        	return((firstAscii/numReduceTasks)-13);
        }
     }
  }

  public static class Reduce
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    Text maxDur = new Text();
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	double max = Double.MIN_VALUE;
    	for (DoubleWritable val : values) {
    		double temp = val.get();
                   if(temp > max){
                       max = temp;
                    }
    	    }
    	    maxDur.set(key);
            context.write(maxDur, new DoubleWritable(max));
    	}
    }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "subramaniam_exercise4");
    job.setJarByClass(subramaniam_exercise4.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setPartitionerClass(KeyPartitioner.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
