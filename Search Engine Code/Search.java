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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.lang.*;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.fs.ContentSummary;


/*
*Demo search engine, where user will enter their query 
*job accepts the query and list the documents with their tfidf scores
*
*/
public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Search.class);
	// sQuery will be the search query given by user and declared as class variable to have its scope withing the class.
	private static String sQuery=null;   
	public static void main( String[] args) throws  Exception {
		
		//Call the run method for TermFrequency.java using ToolRunner 
		int res  = ToolRunner .run( new Search(), args);

		System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		

		//Create a new instance of the job.
		Job job  = Job .getInstance(getConf(), " searchEngine ");
		job.setJarByClass( this .getClass());

		//Please note the input directory for the map MUST be the output directory of Reducer2 of TFIDF file.
		FileInputFormat.addInputPaths(job, args[0]);
		//Setting the type of output key and value that will be returned by the Reducer
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));

		//Get the configuration for setting the sQuery in the configuration to access it everywhere within the scope.
		Configuration conf= job.getConfiguration();	
		//Argument 3 will be our search query which will be given within ""
		conf.set("searchQuery", args[2]);

		//Setting Mapper and Reducer class for the job
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		//Setting the type of output key and value that will be returned by the Reducer
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( DoubleWritable .class);

		return job.waitForCompletion( true)  ? 0 : 1;
	}

	/*
	**Mapper Class
	*
	*Argument 1 and 2 tell the types of the input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Will produce intermediate output for Reducer
	*Input form: "is#####filename tfidfScore"
	*Output form: "<filename, tfidfScore>...."
	*/
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {

		private Text currentWord  = new Text();


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String searchText=context.getConfiguration().get("searchQuery");
			
			searchText=searchText.toLowerCase();

			String searchWord[]= searchText.split("\\s+");

			String line  = lineText.toString().toLowerCase();
			String currentWord;
			//Split the input to fetch the word and "filename tfidfscore" pair	
			String words[]=line.split("#####");
			String filenameValue= words[1];
			currentWord  = words[0];
			//Fetch the filename
			Text filename= new Text(filenameValue.split("\\s+")[0]);
			//Fetch the tfidf score
			DoubleWritable value= new DoubleWritable(Double.parseDouble(filenameValue.split("\\s+")[1]));
			//if the input query words matches the words in file, write the tdidf score for those words 
			for(String matchWord:searchWord){
				if(matchWord.equalsIgnoreCase(currentWord)){		
					context.write(filename,value);	
				}
			}


		}
	}

	/**
	*Reducer Class-- lists the files with tfidf scores,  which matches the query
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Reducer
	*Input Form: <filename,["score1,score2"]>...
	*OutForm: filename <commulative TFIDF score>
	*/
	public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			double sum  = 0.0;
			//Find the commulative tf idf score
			for ( DoubleWritable count  : counts) {
				sum  += count.get();
			}
			context.write(word,  new DoubleWritable(sum));
		}
	}
}
