import java.io.IOException;
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

public class AnagramFinder {
    public static class AnagramMapper extends
            Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
			//stopwords
			String stopWords = "'tis,'twas,a,able,about,across,after,ain't,all,almost,also,am,among,an,and,any,are,aren't,as,at,be,because,been,but,by,can,can't,cannot,could,could've,couldn't,dear,did,didn't,do,does,doesn't,don't,either,else,ever,every,for,from,get,got,had,has,hasn't,have,he,he'd,he'll,he's,her,hers,him,his,how,how'd,how'll,how's,however,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,just,least,let,like,likely,may,me,might,might've,mightn't,most,must,must've,mustn't,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,shan't,she,she'd,she'll,she's,should,should've,shouldn't,since,so,some,than,that,that'll,that's,the,their,them,then,there,there's,these,they,they'd,they'll,they're,they've,this,tis,to,too,twas,us,wants,was,wasn't,we,we'd,we'll,we're,were,weren't,what,what'd,what's,when,when,when'd,when'll,when's,where,where'd,where'll,where's,which,while,who,who'd,who'll,who's,whom,why,why'd,why'll,why's,will,with,won't,would,would've,wouldn't,yet,you,you'd,you'll,you're,you've,your";
            List<String> stopList = Arrays.asList(stopWords.split(","));
			StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
				//used to check if it is a stopword
                String checkWord = itr.nextToken();
				//remove punctuation and numbers and change to all lowercase
				String anagramWord = checkWord.replaceAll("\\p{Punct}", "").toLowerCase();
				anagramWord = anagramWord.replaceAll("[^a-z]","");
		    	//if not a stop word and has more than 1 letter
				if(stopList.contains(checkWord) == false && anagramWord.length() > 1 ){
					//turn the word to a char array
					char[] arr = anagramWord.toCharArray();
					//sort the array alphabetically
					//creates a key that all anagrams will share. Example Input: race, care -> Key: acer)
					Arrays.sort(arr);
					String wordKey = new String(arr);
					//write both the key and the word as a pair
					context.write(new Text(wordKey), new Text(anagramWord));
				}
            }

        }

    }
	
    public static class AnagramReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String anagram = null;
			int size = 0;
			//list of anagrams for sorting
			//Hashset only holds unique values
			HashSet<Text> anagramList = new HashSet<Text>();
            for (Text val : values) {
				//add each unique word to a list
				if(anagramList.add(val) && !anagramList.contains(val) ){
					//count the number of unique anagrams
		    		size++;
				}
            }
			//convert hashset to an arraylist so it can be sorted
			List<Text> sortedAnagramList = new ArrayList<>(anagramList);
			//if there is more than one word that is an anagram
			if(sortedAnagramList.size() > 1) {
				//sort the list
				Collections.sort(sortedAnagramList);
				for(Text word : sortedAnagramList) {
					//now add all the words to a string
					if (anagram == null) {
						//start of string
						anagram = word.toString();
					} 
					else {
						//rest of the words
						anagram = anagram + ',' + word.toString();
					}
					
				}
				Text count = new Text(String.valueOf(size));
				context.write(count, new Text(anagram)); // Output ; 2 race,care
			}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(AnagramFinder.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
