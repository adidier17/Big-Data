package com.msia.app;

import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class standardize_med {

  public static class Map
       extends Mapper<Object, Text, Text, Text>{

        
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
    	//StringTokenizer line = new StringTokenizer(value.toString());
    	//String line = value.toString();
	    String[] columns = value.toString().split("\t");
	    //ArrayList<Double> values = new ArrayList<Double>();
	    String stringValue = "";
	    int[] col_req = {20,21,22,23,24,25,26,27,28};
	    try{
	    	stringValue = String.valueOf(Double.parseDouble(columns[col_req[0]]));
	    	for(int i=1; i<col_req.length; i++){	
	    	stringValue = stringValue + "," + Double.parseDouble(columns[col_req[i]]);
	    	}
	    	//stringValue = Double.parseDouble(columns[20]) + "," +Double.parseDouble(columns[23]) + "," + Double.parseDouble(columns[25]) + "," +Double.parseDouble(columns[27]);
	    	// + "," + Double.parseDouble(columns[23]) + "," +Double.parseDouble(columns[25]) + "," +Double.parseDouble(columns[27]) + Double.parseDouble(columns[24]) + "," +Double.parseDouble(columns[26]) + "," +Double.parseDouble(columns[28]);
	    	//context.write(new Text("key"),new Text(stringValue));
	    	}
	    catch(Exception e){}
	    	context.write(new Text("key"),new Text(stringValue));
	    
	    
	    
//	    context.write(new Text("key"),new Text(stringValue));
        		
        }
    	}
        
  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	ArrayList<Double> min_values = new ArrayList<Double>();
    	ArrayList<Double> max_values = new ArrayList<Double>();
    	String minValue = "";
    	String maxValue = "";
    	
    	double count = 0;
    	for (Text val : values) 
    	{
    		ArrayList<Double> observation = new ArrayList<Double>();
			//assign individual observation
			//String line = val.next().toString();
    		String [] token = val.toString().split("\\,|\\s+|\\t+");
			//convert tokens to double and store in array -->this will be converted to Text for emission
    		for (int i=0; i<token.length; i++)
    			{
    				try{
    				observation.add(Double.parseDouble(token[i].trim()));
    				}
    				catch(NumberFormatException ne){
    	    	    	System.out.println("Reducer observation for loop");
    	    	    }
    	    		catch(IndexOutOfBoundsException ie){
    	    			System.out.println("Reducer observation for loop");
    	    		}
    			}
    		//if first observation in values list
    		if (count == 0)
    		{	
    				for(int i=0; i<observation.size(); i++)
    				{
    					try{
    					//add individual variable value to running total and establishes size of sum array
						min_values.add(observation.get(i));
						max_values.add(observation.get(i));
    					}
    					catch(IndexOutOfBoundsException ie){
        	    			System.out.println("Min max observation loop");
        	    		}
					}
			}			
    		//else
    		else
    		{
    			for(int i=0; i<observation.size(); i++)
    				{
    					try{
    					if(observation.get(i) < min_values.get(i)){
    						min_values.set(i,observation.get(i));
    					}
    					if(observation.get(i) > max_values.get(i)){
    						max_values.set(i,observation.get(i));
    					}
    					}
    					catch(IndexOutOfBoundsException ie){
        	    			System.out.println("Min max assignment loop");
        	    		}
    				}
			}								
    		//increment observation count
    		count++;
		}
    	try{
    	minValue = Double.toString(min_values.get(0));
    	maxValue = Double.toString(max_values.get(0));
    	}
    	catch(NumberFormatException ne){
	    	System.out.println("Min max string");
	    }
		catch(IndexOutOfBoundsException ie){
			System.out.println("Min max string");
		}
    	
    	for (int i=1; i<min_values.size(); i++)
		{
	    	try{
    		minValue = minValue+","+ Double.toString(min_values.get(i)).trim();
	    	maxValue = maxValue+","+ Double.toString(max_values.get(i)).trim();
	    	}
	    	catch(NumberFormatException ne){
		    	System.out.println("Min max for loop");
		    }
			catch(IndexOutOfBoundsException ie){
				System.out.println("Min max for loop");
			}
		}
	    
	    context.write(new Text("min_values"),new Text(minValue));
	    context.write(new Text("max_values"),new Text(maxValue));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "standardize_med");
    job.setJarByClass(standardize_med.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    //job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}