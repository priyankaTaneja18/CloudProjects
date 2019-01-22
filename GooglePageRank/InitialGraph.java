package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/*
*This class first call CountPages to calculate total number of pages 
*It thens sets the inital values of the page rank for all nodes to 1/n
*/
public class InitialGraph extends Configured implements Tool {
	
	
	private static final Logger LOG = Logger .getLogger( InitialGraph.class);    	   
	public static void main( String[] args) throws  Exception {
		
	
	}

	public int run( String[] args) throws  Exception {

		Configuration conf= getConf();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[2]))){
		fs.delete(new Path(args[2]),true);
		}
		//*******************************************************************
		//Find total number of pages
		//*******************************************************************
		Job job1  = Job .getInstance(getConf(), " countPage ");
		job1.setJarByClass( this .getClass());
		//Input folder path will be given by arg[0] which we provide from command line
		FileInputFormat.addInputPaths(job1,  args[0]);
		//Output folder path will be given by arg[1]/CP to store countPages output
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]+"/CP"));
		//Setting Mapper and Reducer class for the job
		job1.setMapperClass( CountPageMapper .class);
		job1.setReducerClass( CountPageReducer .class);
		//Setting the type of output key and value that will be retudrned by the Reducer
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( IntWritable .class);
		//Wait till job completes it return 0 on success
		job1.waitForCompletion( true) ;
		
		//To read the total number of pages from hdfs output directory
		
		Path pageOutput = new Path(args[1]+"/CP"+"/part-r-00000");
		BufferedReader bufferrd = new BufferedReader(new InputStreamReader(fs.open(pageOutput)));
		String text_input = "";
		String auxl = "";

		while ((auxl = bufferrd.readLine()) != null) 
		      {
		              text_input += auxl;
		      }     

		String tot = text_input.split("\t")[1];
		System.out.println("###"+tot);
		bufferrd.close();
		//Delete the CountPage output directory once we have stored its output in local
		if(fs.exists(new Path(args[ 1]+"/CP"))){
		fs.delete(new Path(args[ 1]+"/CP"),true);
		}
		
		//store the total pages in the configuration.
		 conf.set("totalPages", tot);

		//********************************************************************************
		//Create the Initial Graph
		//********************************************************************************

		//Create a new instance of the job.
		Job job  = Job .getInstance(getConf(), " InitialGraph ");
		job.setJarByClass( this .getClass());
		//Input folder path will be given by arg[0] which we provide from command line
		FileInputFormat.addInputPaths(job,  args[0]);
		//Output folder path will be given by arg[1]/IG to store countPages output
		FileOutputFormat.setOutputPath(job,  new Path(args[1]+"/IG"));
		//Setting Mapper and Reducer class for the job
		job.setMapperClass( InitialGraphMapper .class);
		job.setReducerClass( InitialGraphReducer .class);
		//Setting the type of output key and value that will be returned by the Reducer
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( Text .class);
		fs.close();
		//Wait till job completes it return 0 on success
		return job.waitForCompletion( true)  ? 0 : 1;

		
		
	}
	
	/*Mapper Class for Initial Graph
	*
	* Argument 1 and 2 tell the types of the input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Will produce intermediate output for Reducer.
	*Input: xml text  and OutPut: node OneOutlink
	*/
	public static class InitialGraphMapper extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			
			String title="";
			String line= lineText.toString();
			//Title
			Pattern pattern = Pattern.compile("<title>(.+?)</title>");
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()){
				title = matcher.group(1);
			}

			//Text
			Pattern patternText = Pattern.compile("<text.*?>(.+?)</text>");
			Matcher matcherText = patternText.matcher(line);

			if (matcherText.find()){

				// checking the outlinks [[]]
				Pattern patternOutlink = Pattern.compile("\\[\\[(.*?)\\]\\]");
				Matcher matcherOutlink = patternOutlink.matcher(matcherText.group(1));
				while(matcherOutlink.find()){
					String link = matcherOutlink.group(1);
					System.out.println(link);
					context.write(new Text(title), new Text(link) );
				}
			}
			
			
		}

		

	}
	
	/**
	*Reducer Class for Initial Graph
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Reducer
	*Input: <node,[outlinks]>
	*Output: node   initialPageRank###<Outlink1@@@outlink2...>
	*/
	public static class InitialGraphReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text key,  Iterable<Text > nodes,  Context context)
	         throws IOException,  InterruptedException {   
	    	  //Setting up the initial page rank as suggested (1/N)
			  Integer totalCounts = Integer.parseInt(context.getConfiguration().get("totalPages"));
			  System.out.println("$$$"+totalCounts);
	    		  Double valDouble = new Double(1.0/totalCounts);
			  
			  String val=null;
			  boolean firstTime=true;
	    		  for(Text txt:nodes){
				if(firstTime){
					 val=txt.toString();
					firstTime=false;
				}else{
	    			  val += "@@@"+txt.toString();
				}
	    		  }
	    	  		
	    		  context.write(key, new Text(valDouble.toString()+"###"+val));
	         
	   	 
		}
	}


	/*
	*Mapper for CountPages to count the number of pages
	*Input: xml file
	*Output: node 1
	*/

	public static class CountPageMapper extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
	
		 
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
 			
			String title=null;
			String line= lineText.toString();
			//Title
			Pattern pattern = Pattern.compile("<title>(.+?)</title>");
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()){
				title = matcher.group(1);
			}
			if(title!=null){
            		context.write(new Text("Pages"), new IntWritable(1));
			}
			
			
   		 	
		}


	}
	
	/**
	*Reducer Class for CountPages
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Reducer
	*Input: <node,[1,1,1]>
	*Output: Pages totalCount
	*/
	public static class CountPageReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text key,  Iterable<IntWritable>  counts,  Context context)
	         throws IOException,  InterruptedException {   
	    	  //Setting up the initial page rank as suggested (1/N)
	    		   Integer total = 0;
	    		  for(IntWritable count:counts){
				 total += count.get();
				}
	    		  
	    	  	
	    		  context.write(key, new IntWritable(total));
	         
	   	 
		}
	}
	
	
	
}
