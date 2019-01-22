package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;






public class PageRankDriver extends Configured  {
	
	
	private static final Logger LOG = Logger .getLogger( PageRankDriver.class);    	   
	public static void main( String[] args) throws  Exception {
		
		//Run Initial Graph
		int res  = ToolRunner .run( new InitialGraph(), args);
		if(res==0){
	
		res=1;
		//RunPageRank Algorithm with 10 iterations
		for(int i=1;i<=10;i++){
		 PageRank.itr=i;
		 res  = ToolRunner .run( new PageRank(), args);
		}
			
		//Run sorting on the basis of page rank
		Sort.last_itr= PageRank.itr;
		res= ToolRunner.run(new Sort(),args);

		}
		System .exit(res);
	}

	
}
