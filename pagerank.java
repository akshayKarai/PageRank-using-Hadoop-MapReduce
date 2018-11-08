/*
 * Name		: Akshay Karai
 * Email	: akshaysan3238@gmail.com
 */
package pagerank;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {
	private static int diffMax=0;
	private static final String N = "N";
	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner.run( new PageRank(), args);
		System .exit(res);
	}
	
	public int run( String[] args) throws  Exception {
		
		FileSystem fs = FileSystem.get(getConf());
		long numbDocs;
		
		// calculating the number of documents "N"
		Job jobGetCnt  = Job.getInstance(getConf(), "pagerank");
		jobGetCnt.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobGetCnt,  args[0]);
		FileOutputFormat.setOutputPath(jobGetCnt,  new Path(args[1]+ "getCount"));
		jobGetCnt.setMapperClass( MapGetCount.class);
		jobGetCnt.setReducerClass( ReduceGetCount.class);
		jobGetCnt.setMapOutputValueClass(IntWritable.class);
		jobGetCnt.setOutputKeyClass( Text.class);
		jobGetCnt.setOutputValueClass(IntWritable.class);
		jobGetCnt.waitForCompletion(true);
		fs.delete(new Path(args[1]+"getCount"), true);
		
		//Obtains the value of N that has been computed in first job
		numbDocs = jobGetCnt.getCounters().findCounter("N", "N").getValue();
		
		// assigning initial page rank of 1/N to each page
		Job jobSetp  = Job.getInstance(getConf(), "pagerank");
		jobSetp.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobSetp,  args[0]);
		FileOutputFormat.setOutputPath(jobSetp,  new Path(args[1]+"0"));
		jobSetp.getConfiguration().setStrings(N, numbDocs + "");
		jobSetp.setMapperClass( MapSetup.class);
		jobSetp.setReducerClass( ReduceSetup.class);
		jobSetp.setOutputKeyClass( Text.class);
		jobSetp.setOutputValueClass( Text.class);
		jobSetp.waitForCompletion(true);
		
		 int iter=1;
		 Job jobPagerank  = Job.getInstance(getConf(), "pagerank");
		 
		//This job calculates PageRank of each page using the outlinks it has and iterates the same procedure till it either converges or for specific number of times.
		while(iter<=10){
			if(iter>1){
				diffMax=1;
				jobPagerank  = Job.getInstance(getConf(), "pagerank");
			}
			
			jobPagerank.setJarByClass( this.getClass());
			FileInputFormat.addInputPaths(jobPagerank,  args[1]+(iter-1));
			FileOutputFormat.setOutputPath(jobPagerank, new Path(args[1]+iter));    
			jobPagerank.setMapperClass( MapPageRank.class);
			jobPagerank.setReducerClass( ReducePageRank.class);
			jobPagerank.setOutputKeyClass( Text.class);
			jobPagerank.setOutputValueClass( Text.class);
			
			jobPagerank.waitForCompletion(true);
			
			fs.delete(new Path(args[1]+(iter-1)), true);
			
			if(iter>1){
				diffMax=(int) jobPagerank.getCounters().findCounter("convergence", "convergence").getValue(); 	//variable used to decide whether the pageranks have been converged or not, gets its value from counters
			}
			iter++;
		}
		//job to sort the urls based on PageRanks 
		Job jobSrt  = Job.getInstance(getConf(), "pagerank");
		jobSrt.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobSrt,  args[1]+(iter-1));
		FileOutputFormat.setOutputPath(jobSrt,  new Path(args[1]));
		jobSrt.setMapperClass( MapSort.class);
		jobSrt.setReducerClass( ReduceSort.class);
		jobSrt.setMapOutputKeyClass(Text.class);
		jobSrt.setMapOutputValueClass(Text.class);
		jobSrt.setOutputKeyClass( Text.class);
		jobSrt.setOutputValueClass( Text.class);
		jobSrt.waitForCompletion(true);
		fs.delete(new Path(args[1]+(iter-1)), true);
		diffMax = (int) jobGetCnt.getCounters().findCounter("convergence", "convergence").getValue();

		return 1;
	}
	
	//Mapper class to calculate the N
	public static class MapGetCount extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern titlePattrn = Pattern
				.compile("<title>(.*?)</title>");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			if (line != null && !line.isEmpty()) {
				Text documentTitle = new Text();

				Matcher titleMatch = titlePattrn.matcher(line); //matches each line to check if it contains the title

				if (titleMatch.find()) {
					documentTitle = new Text(titleMatch.group(1).trim());
					context.write(new Text(documentTitle), new IntWritable(1)); //if the title is present in the line then it is sent to the reducer
				}

			}

		}
	}

	//Reducer class to calculate the N
	
	public static class ReduceGetCount extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> counts,
				Context context) throws IOException,
				InterruptedException {
					
			context.getCounter("N", "N").increment(1); //increments the N counter for calculating the N for each title or key
		}
		
	}
	
	//Mapper class for the calculation of the initial pagerank using setup and puts it in the outlinks
	public static class MapSetup extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		private static final Pattern titlePattrn = Pattern.compile("<title>(.*?)</title>"); //pattern to check the title
		private static final Pattern textPattrn = Pattern.compile("<text(.*?)</text>");
		private static final Pattern linkPattrn = Pattern .compile("\\[\\[(.*?)\\]\\]"); //pattern to check the outlinks
		
		double numbDocs;

		public void setup(Context context) throws IOException, InterruptedException{
			numbDocs = context.getConfiguration().getDouble(N, 1); //gettinh  n value from the configuration using getConfiguration
		}


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			if(line != null && !line.isEmpty() ){
				
				Text documentTitle  = new Text();
				Text documentLinks  = new Text();
				
				String links = null;
				String txtLine = null;
				
				Matcher titleMatch = titlePattrn.matcher(line);
				
				Matcher textMatcher = textPattrn.matcher(line);
				if(titleMatch.find()){
					documentTitle  = new Text(titleMatch.group(1).trim());
		    	  }
				Matcher linksMatchr = null;

				while(textMatcher.find()){
					txtLine  = textMatcher.group(1).trim();		
					linksMatchr = linkPattrn.matcher(txtLine);								
					double initialPageRank = (double)1/(numbDocs);  //calculating the initial page rank					
					StringBuilder stringBuilder = new StringBuilder("!!##"+initialPageRank+"!!##");					
					
					int flag=0;
					
					while(linksMatchr != null && linksMatchr.find()){						
						links=linksMatchr.group().replace("[[", "").replace("]]", "");						
						if(flag==1){
							stringBuilder.append("!@#"); //adding the delimiter to differentiate between the two outlinks
						}
						stringBuilder.append(links.trim());
						flag=1;					
					}			
				documentLinks = new Text(stringBuilder.toString().trim());
				context.write(documentTitle,documentLinks);				
				}
			}
		}
	}
	
// here this another reduce will write initial setup to the file to complete the next job
	public static class ReduceSetup extends Reducer<Text ,  Text ,  Text ,  Text > {
		public void reduce( Text word,  Text counts,  Context context)
				throws IOException,  InterruptedException {
			context.write(word,counts);
		}
	}	
	
	//This Mapper class calculates the page rank. 
	public static class MapPageRank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
	
			String line  = lineText.toString().trim();
			String[] lineSplit=line.split("!!##"); //splits to get the initial rank and outlinks
			float currentRank =Float.parseFloat(lineSplit[1]); //initial page rank 
			String documentTitle = lineSplit[0].trim(); //gets the title of the page
					
			if(lineSplit.length<3){
				//this condition is handled in order to handle the sink cases
				context.write(new Text(documentTitle),new Text(lineSplit[1]+"@@!@@!@@"+" ")); 
			}

			else if(lineSplit.length==3){
				
				String[] listLinks=lineSplit[2].split("!@#");
				float hashTemp=currentRank/listLinks.length; //calculates the rank/number of outlinks value
				
				for(int u=0;u<listLinks.length;u++){
					context.write(new Text(listLinks[u].trim()+""), new Text(hashTemp+"!!##val##!!")); //for each outlink writes its url and rank/outlinks value
				}
				
				if(documentTitle!=null){
					context.write(new Text(documentTitle),new Text(lineSplit[1]+"@@!@@!@@"+lineSplit[2].trim()+"")); //writes the url and its outlinks for reducer
				}
			}
		}
	}
	
	
	
	//Page rank for each page is calculated using this Reduce page

	public static class ReducePageRank extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter("convergence", "convergence").setValue(1);
		}

		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {			        
			
			String documentTitle=word.toString().trim();
			float newRank=0.0f;
			float val=0.0f;
			float preRank=0.0f;
			String links=null;
			
					
				for(Text value:counts){
					
					String keyValue = value.toString().trim();
									
					if(keyValue.contains("!!##val##!!")){
						keyValue=keyValue.replace("!!##val##!!","");
						val=val+Float.parseFloat(keyValue.toString()); //calculating the sum of pagerank which is divided by the outlinks of the current page
					}
					else{
						
						String[] hashTempOutput= value.toString().trim().split("@@!@@!@@");
						
						if(hashTempOutput[0].length()>0){
							preRank=Float.parseFloat(hashTempOutput[0].trim());
					
							if(hashTempOutput.length==2){
								links = hashTempOutput[1].trim();
							}
							else{
								links = " ";
							}
						}
					}
				}
				
				if(links!=null){
					if(links.length()>0){
				
						newRank=(float) ((val*0.85)+0.15); //calculating the page rank including the damping factor
					
						float diff=preRank-newRank; //calculating the diffrnce of new pagerank and based on max diff the task is continued or haulted
						diff = Math.abs(diff);
								
						if(diff>0.001){
							
							context.getCounter("maxDiff", "maxDiff").setValue(0);
						}
						context.write(new Text(documentTitle), new Text("!!##"+newRank+"!!##"+links) );
					}
				}
		}
	}
	
	//Mapper class sorts the last page rank
	public static class MapSort extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {  
			
			String line  = lineText.toString().trim();
			String[] vals=line.split("!!##");
			
			double rank= Double.parseDouble(vals[1]);
			context.write(new Text("Ranking"), new Text(vals[0].trim()+"!!##"+rank)); //just some unique string is taken as key to pass to the reducer so that all the filenames and their tfidf are accessible at the same time
			//the file name is combined with its combined pagerank with a delimeter and will be passed as value to the reducer
		
		}
	}


	//Reducer class sorts the last page rank
	public static class ReduceSort extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
			
			HashMap<String,Double> ranks = new HashMap<String, Double>();
	
			for (Text count : counts){
			
				String[] urlRank=count.toString().split("!!##"); //the value splits into two  file name and pagerank which is combined
				
				String url = urlRank[0].trim();
					
				double pagerank = Double.parseDouble(urlRank[1]);
			
				ranks.put(url, pagerank);
		        	
		  	}
			
			List<Map.Entry<String, Double> > list = 
		               new LinkedList<Map.Entry<String, Double> >(ranks.entrySet()); 
		  
		        // Sort the list 
		        Collections.sort(list, new Comparator<Map.Entry<String, Double> >(){ 
		            public int compare(Map.Entry<String, Double> obj1,  
		                               Map.Entry<String, Double> obj2) 
		            { 
		                return (obj2.getValue()).compareTo(obj1.getValue()); 
		            } 
		        }); 
		          
		        //  data transfered from sorted list to hashmap  
		        HashMap<String, Double> hashTemp = new LinkedHashMap<String, Double>(); 
		        for (Map.Entry<String, Double> aa : list) { 
		            hashTemp.put(aa.getKey(), aa.getValue()); 
		        } 
			
			for (java.util.Map.Entry<String, Double> pair : hashTemp.entrySet()) { //iterating pagerank in ascending order
		        
		        context.write(new Text(pair.getKey().toString()), new Text(pair.getValue()+""));  
		        
			}
			
		}
	}

}
	