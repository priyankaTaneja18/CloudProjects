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
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.xml.sax.SAXException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
*This class is used for clean up and sorting purpose/
*It uses only one reducer and deletes all temp folders.
*It also displays the final output after sorting the pages as per its pank rank
*/
public class Sort extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Sort.class); 
	public static int last_itr=0;
	public static void main( String[] args) throws  Exception {
		//Call the run method for TermFrequency.java using ToolRunner 
		//int res= ToolRunner.run(new Sort(),args);
		//System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		//Create a new instance of the job.
		Job job  = Job .getInstance(getConf(), " Sort ");
		job.setJarByClass( this .getClass());
		Configuration conf= getConf();
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(SortComp.class);
 		
		//Input folder path will be given by arg[1]/OP<lastIteration>
		
		FileInputFormat.addInputPaths(job,  args[1]+"/OP"+last_itr);
		//Output folder path will be given by arg[2] which we provide from command line
		
		FileOutputFormat.setOutputPath(job,  new Path(args[2]));
		
		//Setting Mapper and Reducer class for the job
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		//Setting the type of output key and value that will be returned by the Reducer
		job.setOutputKeyClass( DoubleWritable .class);
		job.setOutputValueClass( Text .class);
		//Wait till job completes it return 0 on success

			
		job.waitForCompletion( true) ;
		//CleanUp Process delete all temporary folders
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[ 1]))){
		fs.delete(new Path(args[ 1]),true);
		}
		fs.close();
		
		return 0;
	}
	
	/*Mapper Class
	*
	* Argument 1 and 2 tell the types of the input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Will produce intermediate output for Reducer
	*Input: node   intermediatePageRank###<Outlink1@@@outlink2...>
	*Output: PageRank Node
	*/
	public static class Map extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
	
		
		public void map( LongWritable offset,  Text line,  Context context)
				throws  IOException,  InterruptedException {

			
			
			String lineText= line.toString();			
			String node = lineText.split("\t")[0];
			System.out.println("node:: "+node);
			String split1=lineText.split("\t")[1];
			
			String pageRank= split1.split("###")[0];
			
			System.out.println("pageRank::"+ pageRank);
			
			
			context.write(new DoubleWritable(Double.parseDouble(pageRank)),new Text(node));	
			
			

		}
	

	}
	
	/**
	*Reducer Class
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Reducer
	*Output: node page rank (dwscending order of page rank	)
	*/
	public static class Reduce extends Reducer<DoubleWritable , Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( DoubleWritable key,   Iterable<Text>  counts,  Context context)
	         throws IOException,  InterruptedException {   
			
			for(Text node :counts){
			
	    		context.write(node, key);
	         	}
	   	 
		}
	}

}
	class SortComp extends WritableComparator {

		 		//Constructor		 
		 		protected SortComp() {
		 			super(DoubleWritable.class, true);
		 		}
		 		
		 		//Suppress warning
		 		@SuppressWarnings("rawtypes")

		 		
		 		@Override
		 		public int compare(WritableComparable var1, WritableComparable var2) 
		 		{
		 			DoubleWritable var3 = (DoubleWritable)var1;
		 				 			
		 			DoubleWritable var4 = (DoubleWritable)var2;
		 			
		 			//Multiplying by -1 for reverse sort
		 			return -1 * var3.compareTo(var4);
		 		}
		 	}
	

