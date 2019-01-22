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
import org.xml.sax.SAXException;

/*
*This class computes the page rank for the nodes
*
*/
public class PageRank extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( PageRank.class); 
	private static Integer n=0;
	public static Integer itr=0;
	public static void main( String[] args) throws  Exception {
		
	}

	public int run( String[] args) throws  Exception {
		//Create a new instance of the job.
		Job job  = Job .getInstance(getConf(), " PageRank ");
		job.setJarByClass( this .getClass());
		Configuration conf= getConf();
 		conf.set("itr", itr.toString());
		//Input folder path will be given by arg[1]/temp/IG for iteration 1
		//Input folder path will be given by arg[1]/temp/OP<previous iteration numbers> for other iterations
		//Output  folder path will be the temp path that we create for storing intermediate results like arg[1]/temp/OP1
		System.out.println("iteration:: "+itr);
		if(itr==1){
		FileInputFormat.addInputPaths(job,  args[1]+"/IG");
		FileOutputFormat.setOutputPath(job,  new Path(args[ 1]+"/OP"+itr));
		}else{
		int iteration = itr-1;
		FileInputFormat.addInputPaths(job,  args[1]+"/OP"+iteration);
		FileOutputFormat.setOutputPath(job,  new Path(args[ 1]+"/OP"+itr));
		}
		//Setting Mapper and Reducer class for the job
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		//Setting the type of output key and value that will be returned by the Reducer
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( Text .class);
		//Wait till job completes it return 0 on success
		return job.waitForCompletion( true)  ? 0 : 1;
	}
	
	/*Mapper Class
	*
	* Argument 1 and 2 tell the types of the input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Will produce intermediate output for Reducer
	*Input: node pageRank###OL1@@@OL@@@OL3...
	*/
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	
		
		public void map( LongWritable offset,  Text line,  Context context)
				throws  IOException,  InterruptedException {

			
			
			String lineText= line.toString();			
			String node = lineText.split("\t")[0];
			System.out.println("node:: "+node);
			String split1=lineText.split("\t")[1];
			System.out.println("Split1: "+split1);
			String prevPR = split1.split("###")[0];
			String[] v= split1.split("###");
			String links=null;
			if(v.length==2){
			links= split1.split("###")[1];
			
			System.out.println("prevpr::"+ prevPR);
			ArrayList<String> outlinks= new ArrayList<>();		
			if(!links.equals("")){
			for(String s: links.split("@@@")){
				outlinks.add(s);
			}
			}
			//ArrayList<String> outlinks= (ArrayList<String>)Arrays.asList(links.split(","));
			
			System.out.println("outlinks::"+ outlinks);	
			int totalOutlinks= outlinks.size();
			Double intermediateValue;
			for(int i=0;i< outlinks.size();i++){
				intermediateValue= (Double)Double.parseDouble(prevPR)/totalOutlinks;
				context.write(new Text(outlinks.get(i)), new Text(intermediateValue.toString()));		
		
			}
			}
			context.write(new Text(node), new Text(prevPR+"$$"+links));	
			

		}
	

	}
	
	/**
	*Reducer Class
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Reducer
	*Input: <node,[div1,div2,$$OL1@@@OL2@@@]>
	*Output: node   intermediatePageRank###<Outlink1@@@outlink2...>
	*/
	public static class Reduce extends Reducer<Text , Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text key,   Iterable<Text>  counts,  Context context)
	         throws IOException,  InterruptedException {   
			Double division=0.0;
			Double dampFact= 0.85;
			String storeOutlink=null;
			boolean outlinkExists=false;	    	 	
	    		for(Text value: counts){
					String val= value.toString();
					System.out.println(val);
					if(val.contains("$$")){
						outlinkExists=true;
						String[] v= val.split("\\$\\$");
						if(v.length==2)
							storeOutlink=v[1] ;	
					}
					else{
						division += Double.parseDouble(val);


					}
			 }
			if(!outlinkExists){return;};
			Double  pgrnk = ((1-dampFact)) + (dampFact * division);	        	  	        	  
		        String  pageRank = pgrnk.toString();
	    		context.write(key, new Text(pageRank+"###"+storeOutlink));
	         	
	   	 
		}
	}
	
}
