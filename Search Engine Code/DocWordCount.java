package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
/*
 *
 *DocWordCount class is used to find the document frequency using Map Reduce
 *
*/
public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( DocWordCount.class);    	   
	public static void main( String[] args) throws  Exception {
		//Call the run method for DocWordCount.java using ToolRunner 
		int res  = ToolRunner .run( new DocWordCount(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		//Create a new instance of the job.
		
		Job job  = Job .getInstance(getConf(), " wordcount ");
		job.setJarByClass( this .getClass());
		//Input folder path will be given by arg[0] which we provide from command line
		FileInputFormat.addInputPaths(job,  args[0]);
		//Output folder path will be given by arg[1] which we provide from command line  
		FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
		//Setting Mapper and Reducer class for the job
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		//Setting the type of output key and value that will be returned by the Reducer
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( IntWritable .class);
		//Wait till job completes it return 0 on success
		return job.waitForCompletion( true)  ? 0 : 1;
	}

	/*Mapper Class
	*
	* Argument 1 and 2 tells the type of input key and value
	*Argument 3 and 4 tells the type of output key value of the Mapper
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
	* Argument 1 and 2 tells the types of input key and value
	*Argument 3 and 4 tells the types of output key value of the Reducer
	*Input: <word,[1,1,1]>
	*Output: word#####filename count
	*
	*/
	public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;
			//For each word, count the frequency of each word appearing in a particular file
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}
			context.write(word,  new IntWritable(sum));
		}
	}
}
