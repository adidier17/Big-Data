import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class subramaniam_exercise1 {

  public static class OneGramMap
       extends Mapper<Object, Text, Text, Text>{

    private Text volumeVal = new Text();
    private Text year = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Boolean validYearFlag = false;
      String[] tokens = value.toString().split("\\s+");
      String yearonFile = tokens[1];
      String volumeonFile = tokens[3];
      String word = tokens[0];
      //Check that year only contains 4 numbers
      validYearFlag = yearonFile.matches("^\\d{4}");
      if(validYearFlag){
    	  if(word.toLowerCase().contains("nu")){
          year.set(yearonFile + " nu");
    	  volumeVal.set(volumeonFile+",1.0");
    	  context.write(year,volumeVal );
    	  }
    	  if(word.toLowerCase().contains("chi")){
              year.set(yearonFile + " chi");
        	  volumeVal.set(volumeonFile+",1.0");
        	  context.write(year,volumeVal );
        	}
    	  if(word.toLowerCase().contains("haw")){
              year.set(yearonFile + " haw");
        	  volumeVal.set(volumeonFile+",1.0");
        	  context.write(year,volumeVal );
        }
      }
    }
  }
  
 public static class TwoGramMap
  extends Mapper<Object, Text, Text, Text>{

private Text volumeVal = new Text();
private Text year = new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
 Boolean validYearFlag = false;
 String[] tokens = value.toString().split("\\s+");
 String yearonFile = tokens[2];
 String volumeonFile = tokens[4];
 String word2 = tokens[1];
 String word1 = tokens[0];
 //check that year only contains 4 digits
 validYearFlag = yearonFile.matches("^\\d{4}");
 if(validYearFlag){
	  if(word1.toLowerCase().contains("nu")||word2.toLowerCase().contains("nu")){
     year.set(yearonFile + " nu");
	  volumeVal.set(volumeonFile+",1.0");
	  context.write(year,volumeVal );
	  }
	  if(word1.toLowerCase().contains("chi")||word2.toLowerCase().contains("chi")){
         year.set(yearonFile + " chi");
   	  volumeVal.set(volumeonFile+",1.0");
   	  context.write(year,volumeVal );
   	}
	  if(word1.toLowerCase().contains("haw")||word2.toLowerCase().contains("haw")){
         year.set(yearonFile + " haw");
   	  volumeVal.set(volumeonFile+",1.0");
   	  context.write(year,volumeVal );
   }
 }
}
}

 public static class AvgCombiner extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         double sum = 0.0, count = 0.0;
         for (Text val : values) {
             String[] tokens = val.toString().split(",");
			 count++;
		        sum += Double.parseDouble(tokens[0]); 
		    }
		 context.write(key, new Text(Double.toString(sum)+","+Double.toString(count)));
     }
 }
  public static class Reduce
       extends Reducer<Text,Text,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	double sum = 0.0, count = 0.0, avg = 0.0;
        for (Text val : values) {
            String[] tokens = val.toString().split(",");
            count = count + Double.parseDouble(tokens[1]);
		        sum += Double.parseDouble(tokens[0]); 
		    }
        avg = sum/count;
        result.set(avg);
		 context.write(key,result );
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "subramaniam_exercise1");
    job.setJarByClass(subramaniam_exercise1.class);
    job.setCombinerClass(AvgCombiner.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    //job.setNumReduceTasks(1);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OneGramMap.class);
	MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TwoGramMap.class);
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

