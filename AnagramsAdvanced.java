import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;

//**********************************************************************************************************************************************
//THIS SECTION IS FOR THE MAPPER

public class AnagramsAdvanced {
	
	public static class AnMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable>{
		//stopwords
		private String stopWords = "'tis,'twas,a,able,about,across,after,ain't,all,almost,also,am,among,an,and,any,are,aren't,as,at,be,because,been,but,by,can,can't,cannot,could,could've,couldn't,dear,did,didn't,do,does,doesn't,don't,either,else,ever,every,for,from,get,got,had,has,hasn't,have,he,he'd,he'll,he's,her,hers,him,his,how,how'd,how'll,how's,however,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,just,least,let,like,likely,may,me,might,might've,mightn't,most,must,must've,mustn't,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,shan't,she,she'd,she'll,she's,should,should've,shouldn't,since,so,some,than,that,that'll,that's,the,their,them,then,there,there's,these,they,they'd,they'll,they're,they've,this,tis,to,too,twas,us,wants,was,wasn't,we,we'd,we'll,we're,were,weren't,what,what'd,what's,when,when,when'd,when'll,when's,where,where'd,where'll,where's,which,while,who,who'd,who'll,who's,whom,why,why'd,why'll,why's,will,with,won't,would,would've,wouldn't,yet,you,you'd,you'll,you're,you've,your";
		
		/*  
		Splits the text into string tokens
		Ignores stopwords
		Each string token (word) has punctuation and numbers removed
		A copy of the word is sorted alphabetically to create a key (Input: care -> Key: acer)
		The mapper creates a key-word pair (Example: acer, care)
		*/
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				//get next token
				String word = itr.nextToken();
				//if the word is not a stopword
				if(stopWords.contains(word) == false && word.length() > 1){
					//remove punctuation, probably unnecessary given next line
					word = word.replaceAll("\\p{Punct}", "").toLowerCase();
					//remove everything but lower case letters
					word = word.replaceAll("[^a-z]+","");
					//split letters into array
					char[] wordArray = word.toCharArray();
					//sort the array to create a key
					Arrays.sort(wordArray);
					//create variable for key
					String wordKey = new String(wordArray);
					//write to context
					context.write(new compositeKeyWritable(new Text(wordKey), new Text(word), NullWritable.get()));
				}
			
			}
		}
	}
	//END OF SECTION
	//**********************************************************************************************************************************************
	
	
	//**********************************************************************************************************************************************
	//THIS SECTION IS FOR SECONDARY SORTING
	
	/*
	Secondary sorting sorts the output of the mapper by key
	This results in the output of the reducer being sorted by key (a sort of alphabetical order)
	*/
	
	public class CompositeKeyWritable implements Writable,
		WritableComparable<CompositeKeyWritable>{
			
			/*
			Custom class
			Builds a composite key out of the key-value pair
			*/
			
			private String wordKey;
			private String word;
			
			
			//constructor
			public CompositeKeyWritable(){}
			
			public CompositeKeyWritable(String wordKey, String word){
				this.wordKey = wordKey;
				this.word = word;
			}
			@Override
			public String toString(){
				return (new StringBuilder().append(wordKey).append("\t").append(word)).toString();
			}
			
			public void readField(DataInput dataInput) throws IOException{
				wordKey = WritableUtils.readString(dataInput);
				word = WritableUtils.readString(dataInput);
			}
			
			public void write(DataOutput dataOutput) throws IOException{
				WritableUtils.writeString(dataOutput, wordKey);
				WritableUtils.writeString(dataOutput, word);
			}
			
			public int compareTo(compositeKeyWritable objKeyPair){
				int result = wordKey.compareTo(objKeyPair.wordKey);
				if(0 == result){
					result = word.compareTo(objKeyPair.word);
				}
				return result;
			}
			
			public String getWordKey(){
				return wordkey;
			}
			
			public void setWordKey(String wordKey){
				this.wordKey = wordKey;
			}
			
			public String getWord(){
				return word;
			}
			
			public void setWord(String word){
				this.word = word;
			}
		}
		
	public class SSortPartitioner extends Partitioner<CompositeKeyWritable, NullWritable>{
		/*
		Returns the natural key (word key) for partitioning
		*/
		
		@Override
		public int getPartition(CompositeKeyWritable key, NullWritable value, int numReduceTasks){
			return (key.getWordKey().hashcode() % numReduceTasks);
		}
	}
	
	public class SSortCompKeyComparator extends WritableComparator{
		/*
		Orders the output of the mapper by composite key
		The reducer output will be sorted by key*/
		
		protected SSortCompKeyComparator(){
			super(CompositeKeyWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable x, WritableComparable y){
			CompositeKeyWritable k1 = (CompositeKeyWritable) x;
			CompositeKeyWritable k2 = (CompositeKeyWritable) y;
			int compResult = k1.getWordKey().compareTo(k2.getWordKey());
			if (compResult == 0){
				return k1.getWord().compareTo(k2.getWord());
		}
		return cmpResult;
		}
	}
	
	public class SSortGroupComparator extends WritableComparator {
		/*
		Groups the output of the mapper by word key 
		For the reducer input
		*/
		protected SSortGroupComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable x, WritableComparable y) {
			CompositeKeyWritable k1 = (CompositeKeyWritable) x;
			CompositeKeyWritable k2 = (CompositeKeyWritable) y;
			return k1.getWordKey().compareTo(k2.getWordKey());
		}
	}
	//END OF SECTION
	//**********************************************************************************************************************************************
	
	
	
	//**********************************************************************************************************************************************
	//THIS SECTION IS FOR THE COMBINER
	
	public static class AnCombiner extends Reducer<Text, Text, Text, Text>{
		/* 
		Removes some duplicates locally
		Optimises execution time
		*/
		protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			//hashset only allows unique elements
			Set<Text> uniqueWords = new HashSet<Text>();
			for(Text value : values){
				//if a new word is added to the set
				if(uniqueWords.add(value)){
					//write the pair
					context.write(key,value);
				}
			}
		}
	}
	//END OF SECTION
	//**********************************************************************************************************************************************
	
	
	//**********************************************************************************************************************************************
	//THIS SECTION IS FOR THE REDUCER
	
	public static class AnReducer extends Reducer<Text, Text, IntWritable, Text>{
		
		/*
		Removes duplicate words by adding them to a hashset
		Add the words from the hashset to a list so it can be sorted alphabetically
		Add the words from the list to a string
		Output the number of anagrams and the set of anagrams
		*/
		
		//used because output is IntWritable type
		private IntWritable count = new IntWritable();
		//used because the output is Text type
		private Text outputWord = new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			//use treeset to hold unique anagrams and order them
			Set<Text> uniqueAnagrams = new TreeSet<Text>();
			//size of list
			int size = 0;
			//empty string
			String anagram = "";
			//for each word
			for(Text w : values){
				String s = w.toString();
				//if a unique anagram is added
				if(uniqueAnagrams.add(s)){
					//increment the size counter
					size++;
				}
			}
			//loop through the now ordered set
			for(String ww : uniqueAnagrams){
				//add each word to the string
				anagram = anagram + ww.toString() +", ";
			}
			if(size > 1){
				//basically changing the variable type
				count.set(size);
				outputWord.set(anagram);
				context.write(count, outputWord);
			}
		}
	}
	//END OF SECTION
	//**********************************************************************************************************************************************
		
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(AnagramsAdvanced.class);
		job.setMapperClass(AnMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setCombinerClass(AnCombiner.class);
		job.setReducerClass(AnReducer.class);
		job.setPartitionerClass(SSortPartitioner.class);
		job.setSortComparatorClass(SSortCompKeyComparator.class);
		job.setGroupingComparatorClass(SSortGroupComparatorr.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(8);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
