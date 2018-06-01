import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.lang.Math;
import java.math.BigInteger;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Grouping_mining {
	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{
		public ArrayList<String> Compare = new ArrayList<String>();
		public ArrayList<String> val = new ArrayList<String>();
	//	private Set<String> stopWordList = new HashSet<String>();
		private BufferedReader fis;
		public void setup(Context context) throws IOException{
			 Configuration conf = context.getConfiguration();
			 FileSystem fs = FileSystem.get(conf);
			 URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
			 Path getPath = new Path(cacheFiles[0].getPath());  
			 //System.out.println("De:"+getPath.toString());
			 BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
			 String setupData = null;
			  Compare.clear();
			 while ((setupData = bf.readLine()) != null) {
				 //System.out.println("Here:"+setupData.split("\t")[1]);
				 //stopWordList.add(setupData);
				 String t = setupData.split("\t")[0]+","+setupData.split("\t")[1];
				// System.out.println("Here is:"+t);
				 Compare.add(t);
			
				 }
			}
		/*protected void setup(Context context) throws java.io.IOException,
		InterruptedException {

	try {

		Path[] stopWordFiles = new Path[0];
		stopWordFiles = context.getLocalCacheFiles();
		System.out.println("BBB:"+stopWordFiles.toString());
		if (stopWordFiles != null && stopWordFiles.length > 0) {
			for (Path stopWordFile : stopWordFiles) {
				readStopWordFile(stopWordFile);
			}
		}
	} catch (IOException e) {
		System.err.println("Exception reading stop word file: " + e);

	}

}
		private void readStopWordFile(Path stopWordFile) {
			try {
				fis = new BufferedReader(new FileReader(stopWordFile.toString()));
				String stopWord = null;
				while ((stopWord = fis.readLine()) != null) {
					stopWordList.add(stopWord);
				}
			} catch (IOException ioe) {
				System.err.println("Exception while reading stop word file '"
						+ stopWordFile + "' : " + ioe.toString());
			}
		}
        */

	/*public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			 String temp = conf.get("C_Path");
			 stopWordList.clear();
			// Path [] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
			// System.out.println("Hi: "+cacheFiles[0].toString());
			 Path pt = new Path(temp);
			 FileSystem fs = FileSystem.get(new Configuration());
			 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			 String line;
			 line=br.readLine();
			 
			 while (line != null){
				 BigInteger temp_c = new BigInteger(line,10);
				 stopWordList.add(temp_c.toString());
				 line = br.readLine();
			 }
			// br.close();
			 //fs.close();
			 
			 
            
  }*/
 
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   //System.out.println("Find me:"+Compare);
   StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
   while (itr.hasMoreTokens()) {
	   
	String T = itr.nextToken();
//	System.out.println("See: "+T);
	//BigInteger T_num = new BigInteger(T.split("\t")[0],2);
	BigInteger T_num = new BigInteger(T,2);
	
	for(String C : Compare) {
		//System.out.println("See Me:"+C);
		
		BigInteger C_num = new BigInteger(C.split(",")[0],2);
		
		//System.out.println("T:"+C_num+" and Canditiate is is:"+T_num);
		//System.out.println("And result: "+C_num.and(T_num));
		//BigInteger r = T_num.and(C_num);
		//System.out.println("And Result is:"+r);
		//BigInteger C_bin = new BigInteger(C_num.toString(),2);
		//if(C_num.equals(T_num.and(C_num))) {
	//		context.write(new Text(C_num.toString()), new IntWritable(Integer.parseInt(T.split("\t")[1])));
	//	}
		
		if(T_num.equals(C_num.and(T_num))) {
			context.write(new Text(T_num.toString()), new IntWritable(Integer.parseInt(C.split(",")[1])));
		}
	}
	
   }
 
  
 }
}
	

public static class IntSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
  int sum = 0;
  for (IntWritable val : values) {
    sum += val.get();
  }
 
  result.set(sum);
  context.write(key, result);

}
}
public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
   int sum = 0;
   Configuration conf = context.getConfiguration();
	 String temp = conf.get("Support");
	 String k = conf.get("k");
	 int support = Integer.parseInt(temp);
   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   if(sum>=support) {
	   System.err.println("Reduce key is:"+key);
	  BigInteger r = new BigInteger(key.toString(),10);
	   System.err.println("Reduce bigInt is:"+r);
	   System.err.println("Binary is:"+r.toString(2));
	 // int r = Integer.parseInt(key.toString());
	  int l = 0;
	//  for(int i=0;i<key.toString().length();i++)
//			l += Integer.parseInt(Character.toString(key.toString().charAt(i)));
//	if(Integer.parseInt(k)==sum)
         context.write(new Text(r.toString(2)), result);
   }
 }
}
public static class PruneMapper
extends Mapper<Object, Text, Text, Text>{
	//public ArrayList<String> Compare = new ArrayList<String>();
	//private Set<String> Compare = new HashSet<String>();
	public ArrayList<String> Compare = new ArrayList<String>();
	private int sp = 10;
   public void setup(Context context) throws IOException{
	     Compare.clear();
		 Configuration conf = context.getConfiguration();
		 String k = conf.get("k");
		 FileSystem fs = FileSystem.get(conf);
		 URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		 int final_index = (Integer.parseInt(k)-1)*10;
		 for(int i=final_index-9;i<=final_index;i++) {
			 BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[i].getPath()))));
			// System.out.println("Now Path is:"+new Path(cacheFiles[i].getPath()).toString());
			 String setupData = null;
			 while ((setupData = bf.readLine()) != null) {
			//	 System.out.println("Now k is: "+ k+"k's Fline is:"+setupData);
				 String l = setupData.split("\t")[0];
				 BigInteger temp_c = new BigInteger(l,2);
				 Compare.add(temp_c.toString());
				 }
		 }	
		}
/*public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		 String temp = conf.get("F_Path");
		 Compare.clear();
		 
		for(int i=0;i<10;i++) {
		 String s = "/part-r-0000"+Integer.toString(i);
		 Path pt = new Path(temp+s);
		 FileSystem fs = FileSystem.get(new Configuration());
		 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		 String line;
		 line=br.readLine();
		 if(line==null) {
			 continue;
		 }
		 while (line != null){
			 String l = line.split("\t")[0];
			// System.out.println("See Me:"+l);
			 BigInteger temp_c = new BigInteger(l,2);
			 Compare.add(temp_c.toString());
			 line = br.readLine();
		 }
		}
		// br.close();
		 //fs.close();
		}*/

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
//System.out.println("Find me:"+Compare);
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
   
String T = itr.nextToken();

   sp++;

BigInteger T_num = new BigInteger(T,2);
for(String C : Compare) {
	
	BigInteger C_num = new BigInteger(C);
	//System.out.println("Distrubute Canditiate is:"+C_num+" and T is:"+T_num);
	//BigInteger C_bin = new BigInteger(C_num.toString(),2);
	//BigInteger r = T_num.and(C_bin);
	//System.out.println("And Result is:"+r);
	
	if(C_num.equals(T_num.and(C_num))) {
		//context.write(new Text(C_num.toString()), new IntWritable(1));
		//int len = T_num.toString().length();
		//context.write(new Text(Integer.toString(sp%10)), new Text(T_num.toString()));
		//System.out.println("I'm IN!!!!!!!!!! T_num is:"+T_num);
		//context.write(new Text(T_num.toString()), new Text(T_num.toString()));
		context.write(new Text(Integer.toString(sp%10)), new Text(T_num.toString()));
		break;
	}
	else {
		continue;
	}
}

}

}
}

public static class PruneReducer
extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {


for (Text val : values) {
	
	int temp = Integer.parseInt(val.toString());
    context.write(new Text(Integer.toBinaryString(temp)), null);
    //break;
}
	//int temp = Integer.parseInt(values.toString(),2);
   //context.write(new Text(Integer.toBinaryString(temp)), null); 

}
}
public static class MergeMapper 
extends Mapper<Object, Text, Text, IntWritable>{
private String T = new String();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
 T = itr.nextToken().toString();
   context.write(new Text(T), new IntWritable(1));

}
}
}
public static class MergeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	 

	 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		 Configuration conf = context.getConfiguration();
		 String temp = conf.get("k");
	//(Text val:values) {
		 
		  int sum = 0;
		  for(int i=0;i<key.toString().length();i++)
				sum += Integer.parseInt(Character.toString(key.toString().charAt(i)));
		  if(sum==Integer.parseInt(temp)+1)
		      context.write(new Text(key), null);
	 // }
	   
	  
	  
	 
	 }
	}

public static class GenMapper 

extends Mapper<Object, Text, Text, Text>{

private String T = new String();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	 String k = conf.get("k");
	 int l = Integer.parseInt(k)-1;
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {

T = itr.nextToken().toString();

//context.write(new Text(Integer.toString(T.split("\t")[0].length())), new Text(T.split("\t")[0]));

//System.err.println("k is:"+l);

	  String c = T.split("\t")[0];
	//  System.err.println("Combine input val is:"+c);
	  int num = 0;
	  String temp = "";
	  for(int i=0;i<c.length()-1;i++) {
		  
		  temp = temp + Character.toString(c.charAt(i));
		  num += Integer.parseInt(Character.toString(c.charAt(i)));
		 // System.out.println("comBine temp is:"+temp+" comBine num is:"+num);
		  if(num==l) {
			  //System.err.println("Combine key:"+c.length()+","+temp+" Value:"+c);
			  context.write(new Text(c.length()+","+temp), new Text(c));
			  break;
		  }
	  }
 

}	




}
}
public static class GenCombiner extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	 String k = conf.get("k");
	 int l = Integer.parseInt(k)-1;
	 System.err.println("k is:"+l);
	for(Text val:values) {
	  String c = val.toString();
	  System.err.println("Combine input val is:"+c);
	  int num = 0;
	  String temp = "";
	  for(int i=0;i<c.length()-1;i++) {
		  
		  temp = temp + Character.toString(c.charAt(i));
		  num += Integer.parseInt(Character.toString(c.charAt(i)));
		 // System.out.println("comBine temp is:"+temp+" comBine num is:"+num);
		  if(num==l) {
			  System.err.println("Combine key:"+c.length()+","+temp+" Value:"+c);
			  context.write(new Text(c.length()+","+temp), new Text(c));
			  break;
		  }
	  }
  }
  
}
}
public static class GenReducer
extends Reducer<Text,Text,Text,Text> {

//ArrayList<BigInteger> C = new ArrayList<BigInteger>();
	
public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	 String k = conf.get("k");
	 List<BigInteger> C = new ArrayList<BigInteger>();
	 C.clear();
for (Text val : values) {
	System.err.println("Reduce Input key is:"+key+" val is:"+val);
	BigInteger t = new BigInteger(val.toString(),2);
    //System.out.println("t is:"+t);
    C.add(t);
}
  
 // Collections.sort(C);
 //System.out.println("k is:"+k+" Reduce Array list:"+C);
  if(C.size()>1) {
	  for(int i=C.size()-1;i>=0;i--) {
		  for(int j=i-1;j>=0;j--) {
			  BigInteger Ci = C.get(i);
			  BigInteger Cj = C.get(j);
			 // System.err.println("Ci is:"+Ci+" Cj is:"+Cj+" Or result is:"+Ci.or(Cj).toString(2));			
			  context.write(new Text(Ci.or(Cj).toString(2)), null);
		  }
	  }
  }
    
  
  
}
}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  if (otherArgs.length != 6) {
	    System.err.println("Usage: <in><out><Can_T_Bit_Map Path><minsup><max length><Transactions>");
	   
	    System.exit(2);
	  }
	  Date date; 
		long start, end; 
		date = new Date(); start = date.getTime(); 
	  Path inputPath = new Path(args[0]);
	  String output = args[1];
	  char o_detect = output.charAt(output.length()-1);
	  if(o_detect!='/') {
		  output = output + '/';
	  }
	  Path outputPath = new Path(output);
	  outputPath.getFileSystem(conf).delete(outputPath, true);
	  double Min_sup = Double.parseDouble(args[3]);
	  String C_Path = args[2];
	  int max_len = Integer.parseInt(args[4]);
	  char detect = C_Path.charAt(C_Path.length()-1);
	  if(detect!='/') {
		  C_Path = C_Path + "/";
	  }
	 // Path CompPath = new Path(C_Path);
	//Path inputPath = new Path("/ethonwu/support_0.2_T_Bit_Map_nospace.txt");
	//Path inputPath = new Path("/ethonwu/retail.txt");
	//Path midoutputPath = new Path("/ethonwu/mid_len_output/");
	//Path outputPath = new Path("/ethonwu/finaloutput/");
	//Path loadPath = new Path("");
	  
	  
	/*FileSystem lfs = FileSystem.get(conf);
	BufferedReader lbr = new BufferedReader(new InputStreamReader(lfs.open(inputPath)));
	int lines =0;
	while (lbr.readLine() != null) {
        lines++;
    }
	lbr.close();*/
	int Transactions = Integer.parseInt(args[5].toString());
	System.out.println("Transactions number are: "+Transactions);
	double Support = (float)Transactions*Min_sup;
	int supp = (int)Support;
	conf.set("Support", Integer.toString(supp));
    Path C;
	String ComeparePath = C_Path+"2";
	int Task = 10;
	for(int i=2;i<=max_len;i++) {
	
	Task = (int)((i+5)/5)*10;
	conf.set("k", Integer.toString(i));
	conf.set("C_Path", ComeparePath);
	C = new Path(ComeparePath);
	Path F_output = new Path(outputPath+"/"+Integer.toString(i));
	DistributedCache.addCacheFile(inputPath.toUri(), conf);
	System.out.println("Now Find "+i+"-itemset!!");	
	Job job = new Job(conf, "T_Bit_Map Mining by Stage by stage in "+i+"-itemnset");
	job.setJarByClass(Grouping_mining.class);
	
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumCombiner.class);
	job.setReducerClass(IntSumReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	//DistributedCache.addCacheFile(C.toUri(), job.getConfiguration());
	job.setNumReduceTasks(Task);
	//FileInputFormat.addInputPath(job, inputPath);
	FileInputFormat.addInputPath(job, C);
	FileOutputFormat.setOutputPath(job,F_output);
	
	
	job.waitForCompletion(true);
	//System.exit(0);
	//ComeparePath = C_Path + Integer.toString(i+1);
	
	//New Feature usging Apriori_gen()
	
	if((i+1)<=max_len) {
	 	Path Prune_output = new Path("/temp/"+Integer.toString(i+1));
		Path temp = new Path("/tmp_mrege/");
		temp.getFileSystem(conf).delete(temp, true);
		Job job2 = new Job(conf, "Apriori Gen"+Integer.toString(i+1)+"-itemset");
		job2.setJarByClass(Grouping_mining.class);
		
		job2.setMapperClass(GenMapper.class);
		//job2.setCombinerClass(GenCombiner.class);
		job2.setReducerClass(GenReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//DistributedCache.addCacheFile(C.toUri(), job.getConfiguration());
		job2.setNumReduceTasks(Task);
		//FileInputFormat.addInputPath(job, inputPath);
		FileInputFormat.addInputPath(job2, F_output);
		FileOutputFormat.setOutputPath(job2,Prune_output);
		job2.waitForCompletion(true);
		ComeparePath = Prune_output.toString();
	/*
		Job job3 = new Job(conf, "Merge"+Integer.toString(i+1)+"-itemnset");
		
		job3.setJarByClass(Grouping_mining.class);
		
		job3.setMapperClass(MergeMapper.class);
		job3.setReducerClass(MergeReducer.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setNumReduceTasks(Task);
		FileInputFormat.addInputPath(job3,temp);
		FileOutputFormat.setOutputPath(job3,Prune_output);
		job3.waitForCompletion(true);
		ComeparePath = Prune_output.toString();*/
	}
	
	
	//New Feature usging Apriori_gen()
	
	//System.exit(0);
	
	// Job2 Prune Candiate
  /*
    if((i+1)<=max_len) {
	//String Prune_path = C_Path+(i+1)+"/part-r-00000";
    	for(int j=0;j<10;j++) {
    		String s = "/part-r-0000"+Integer.toString(j);
    		Path t = new Path(F_output+s);
    		DistributedCache.addCacheFile(t.toUri(), conf);
    	}
   
    	Path Prune_path = new Path(C_Path+Integer.toString(i+1)+"/");
    	Path Prune_output = new Path("/temp/"+Integer.toString(i+1));
	//conf.set("F_Path",F_output.toString());
	//Path temp = new Path("/tmp_mrege/");
	//temp.getFileSystem(conf).delete(temp, true);
	Job job2 = new Job(conf, "Prune"+Integer.toString(i+1)+"-itemnset");
	
	job2.setJarByClass(Grouping_mining.class);
	
	job2.setMapperClass(PruneMapper.class);
	job2.setReducerClass(PruneReducer.class);
	
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	job2.setNumReduceTasks(10);
	FileInputFormat.addInputPath(job2,Prune_path);
	//FileOutputFormat.setOutputPath(job2,temp);
	FileOutputFormat.setOutputPath(job2,Prune_output);
	job2.waitForCompletion(true);
	*/
   /* Job job3 = new Job(conf, "Merge"+Integer.toString(i+1)+"-itemnset");
	
	job3.setJarByClass(Grouping_mining.class);
	
	job3.setMapperClass(MergeMapper.class);
	job3.setReducerClass(MergeReducer.class);
	
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(IntWritable.class);
	job3.setNumReduceTasks(1);
	FileInputFormat.addInputPath(job3,temp);
	FileOutputFormat.setOutputPath(job3,Prune_output);
	job3.waitForCompletion(true);
	*/
	//ComeparePath = Prune_output.toString();
	//System.out.println("Out Path is: "+ComeparePath);
      // }
    
    
	}
	
	date = new Date(); end = date.getTime();   
	System.out.printf("Run Time is:%f",(end-start)*0.001F);
	System.out.println("Finish!!");
	System.exit(0);
   }
}
