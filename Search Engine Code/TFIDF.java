package org.myorg;

//import all relevant packages
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.lang.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import java.util.*;
import java.util.Iterator;
import java.util.Map;

/*
*Class: TFIDF - it will have two map and reduce tasks in chain and only one driver.
*When One MapReduce ends, its output will be the input of the second MapReduce.
*
*/
public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( TFIDF.class);    	   
	public static void main( String[] args) throws  Exception {
		//Call the run method for TFIDF.java using ToolRunner 
		int res  = ToolRunner .run( new TFIDF(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		//Get the configuration for finding the filecount
		Configuration conf= getConf();
		FileSystem fs = FileSystem.get(conf);
		//Tell the path of the directory containing files
		Path p= new Path(args[0]);
		//Use content summary to get the filecount
		ContentSummary cs = fs.getContentSummary(p);
		long totalFiles= cs.getFileCount();
		//Set the total number of files count in the configuration
		conf.set("totalFiles", String.valueOf(totalFiles));

		//Create a new instance of the first job
		Job job  = Job .getInstance(conf, " termfrequency ");
		job.setJarByClass( this .getClass());					
		//Input directory path will be given by arg[0] which we provide from command line
		FileInputFormat.addInputPaths(job,  args[0]);
		//Intermediate output directory's path will be given by arg[1] which we provide from command line
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		//Setting Mapper and Reducer class for the first job
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		//Setting the type of output key and value of the first job that will be returned by the first Reducer
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( IntWritable .class);
		//Wait till job one completes before starting job 2
		job.waitForCompletion(true);

		//Start Job 2 by creating its instance
		Job job2 = Job.getInstance(conf,"tfidf");
		job2.setJarByClass(this .getClass());

		//Input directory path will be given by arg[1] which is the output directory for MapReduce1 
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		//Setting Mapper and Reducer class for the second job
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		//Setting the type of output key and value of the second job that will be returned by the second Reducer
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass( Text .class);

		return (job2.waitForCompletion(true) ? 0 : 1); 

	}

	/**
	*Map1 used for finding the term frequency (Job1)
	*Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Input: accpets the text in the file
	*Output: <word,1>....
	*/
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text word  = new Text();
		//Setting the Pattern for the words that we will get in the files
		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString().toLowerCase();
			Text currentWord  = new Text();
			//Finding the fileName where the current word is present
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//append the word with its filename
				currentWord  = new Text(word+"#####"+fileName);
				//write current word with its file with frequency 1 in the format that was provided in the question. This will be the intermediate output.
				context.write(currentWord,one);
			}
		}
	}
	
	/**
	*Reducer Class of Job1 
	* Argument 1 and 2 tells the types of input key and value types
	*Argument 3 and 4 tells the types of output key value of the Reducer
	*Output of the reducer one will be the input of Mapper of Job 2
	*Input: <word,[1,1,1]>
	*Output: word#####filename tf
	*/
	public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;
			//For each word, count the frequency of each word appearing in a particular file
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}
			//Perform logarithm operation on the sum and add 1 for calculating wf(t,d)
			double logSum=  1 + Math.log10(sum);
			context.write(word,  new DoubleWritable(logSum));
		}
	}

	/**
	*Job2 used to calculate the tfidf score using the document and term frequency we calculated above. Map of Job 2, provides intermediate data for each word with file and its termfrequency score.
	*Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of the output key value of the Mapper
	*Input form: say, <"Hadoop",,["file1=1.3010,file2=1"]>
	*Output:<word,filename=score>..
	*/
	public static class Map2 extends Mapper<LongWritable ,  Text ,  Text , Text > {


		private Text tf  = new Text();

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			Text currentWord= new Text();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			//Split the line with delimiter: ##### to get the word 
			String words[]=line.toString().split("#####");
			currentWord  = new Text(words[0]);
			//Replace the space with "=" to get "word,filename=count" format
			tf= new Text(words[1].replaceAll("(\\s)+","="));
			context.write(currentWord,tf);
			
		}
	}

	/**
	*Reducer Class of Job2 will do the computation for tfidf based on every word's idf score in each file 
	* Argument 1 and 2 tells the types of input key and value types
	*Argument 3 and 4 tells the types of output key value of the Reducer
	*Input of the reducer  will be the output of Mapper of Job 2
	*Input form: <word,[filename=termfrequency,..]>
	*Output: word#####filename tfidfScore
	*/
	public static class Reduce2 extends Reducer<Text , Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<Text> fileCount,  Context context)
				throws IOException,  InterruptedException {
			//initialise document frequency to zero
			int docFreq  = 0;
			//ArrayList to store the filenames and Termfrequency values
			ArrayList<Double> tfValues= new ArrayList<Double>();
			ArrayList<String> tfFilenames= new ArrayList<String>();
			
			double tf=0;
			//For the term in the document, increase its doc freq and segregates its filename and termfreq value
			for ( Text count  : fileCount) {    
				docFreq++;
				tfValues.add(Double.parseDouble(count.toString().split("=")[1]));	
				tfFilenames.add(count.toString().split("=")[0]);

			}
			//Fetch the total number of files in the directory from the configuration
			double totalFiles= Double.parseDouble(context.getConfiguration().get("totalFiles"));
			//compute idf
			double idf=  Math.log10(1+ (totalFiles/(double)docFreq));

			double tfidf=  0.0;
			Text w;
			//compute tfidf for each word in the document and write it in the context.
			for(int i=0;i<tfValues.size();i++){
				tfidf= idf * tfValues.get(i);

				w= new Text(word+"#####"+tfFilenames.get(i));
				context.write(w,  new DoubleWritable(tfidf));	
			}



		}

	}
}
