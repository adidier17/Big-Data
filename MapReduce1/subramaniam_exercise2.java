import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class subramaniam_exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

	private final static FloatWritable fourthColValue = new FloatWritable();
	private Text compositeKey = new Text();
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    String[] tokens = value.toString().split(",");
            String compKey = tokens[29] + "," + tokens[30] + "," + tokens[31] + "," + tokens[32];
            compositeKey.set(compKey);            
            String flag = tokens[tokens.length - 1];
            float valueToAverage = Float.parseFloat(tokens[3]); 
            if(flag.equalsIgnoreCase("false")){
               fourthColValue.set(valueToAverage);               
               output.collect(compositeKey,fourthColValue);
	}
       }  

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

      
    float sum = 0;
    float count = 0;
    float avg = 0;
    Text compKeyGroup = new Text();	
     public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	
public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    while (values.hasNext()) {
		float temp = values.next().get();
                sum += temp;
                count++;
              }
	    compKeyGroup.set(key);
            avg = sum/count;
            output.collect(compKeyGroup, new FloatWritable(avg));
	
}

}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), IBMClass.class);
	conf.setJobName("subramaniam_exercise2");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new subramaniam_exercise2(), args);
	System.exit(res);
    }
}
