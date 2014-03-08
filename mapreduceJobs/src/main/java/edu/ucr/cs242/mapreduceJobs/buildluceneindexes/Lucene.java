package edu.ucr.cs242.mapreduceJobs.buildluceneindexes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucr.cs242.mapreduceJobs.getWordCounts.Stemmer;
import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts;



public class Lucene {
	
	public static Map<String,String> wordmap = new HashMap<String,String>();
	
	public static Map<String,String> readTweetFile(String tweetFile) {
		Map<String,String> wordmap = new HashMap<String,String>();
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(tweetFile));
			line = br.readLine();
			while (line != null) {
				// extract UserId
				String count = line.substring(0,line.indexOf('\t'));
				line = line.substring(line.indexOf('\t')+1);
				
				String word = line.substring(0,line.indexOf('\t'));
				line = br.readLine();
				if (Integer.parseInt(count) >= 1000){
					wordmap.put(word, count);
				}
			}
		} catch (IOException e) {
			System.err.println("Failed to read userFile: " + e.getMessage());
		}
		return wordmap;
	}

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "WordCounts");
		job.setJarByClass(Lucene.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(PageRankPartitioner.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(PagePankReducer.class);
		

		//initialize wordmap

		return job;
	}

	public static final class PageRankPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class WordCountMapper extends Mapper<Text, Text, Text, Text> {
	
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//Map<String,String> wordmap = readTweetFile("wordcounts.log");

			
			int tabLocation = value.toString().indexOf('\t');
			String tid = (value.toString().substring(0,tabLocation));
			String tweetText = value.toString().substring(tabLocation + 1);
			
			String uid = key.toString();
			
			

			for (String virginWord : tweetText.split("\\s+")) {
				// Remove all special characters
				String word = virginWord.replaceAll("[^a-zA-Z0-9]", "");

				// Make lower case
				word = word.toLowerCase();

				// stem
				char[] w = new char[word.length()];
				for (int i = 0; i < word.length(); i++) {
					w[i] = word.charAt(i);
				}
				Stemmer s = new Stemmer();
				s.add(w, w.length);
				s.stem();
				String stemmedWord = s.toString();
				if (true){//wordmap.containsKey(stemmedWord)) {
					context.write(new Text(uid + ":" + tid + ":" + stemmedWord), new Text("1"));
				}

			}
		}
	}


	public static class PagePankReducer extends Reducer<Text, Text, Text, Text> {
		
		private IndexWriter writer;
		//Map<String,String> wordmap = readTweetFile("wordcounts.log");
		/*
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
			writer.close();
        
        }
		
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
			Directory dir = FSDirectory.open(new File("/user/group42/indexfolder"));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40,
					analyzer);

			// Create will overwrite the index everytime
			iwc.setOpenMode(OpenMode.APPEND);

			// Create an index writer
			writer = new IndexWriter(dir, iwc);
        	
        }
		*/
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			if (!valuesIt.hasNext())
				return;
			
			double tf = 0;
            while (valuesIt.hasNext()) {
                String value = valuesIt.next().toString();
                tf += Double.parseDouble(value);
            }
            
            String[] pieces = key.toString().split("\\:");
            //double librarycount = Double.parseDouble(wordmap.get(pieces[2]));
            //double idf = Math.log(10326522 / librarycount);
            //double tfidf = tf * idf;
            double tfidf = tf;
            
            //DO HASHTAG STUFF!!!!!
            
            
            
			/*
			Document doc = new Document();
			doc.add(new StringField("word", pieces[2], Field.Store.YES));
			doc.add(new DoubleField("score", tfidf, Field.Store.YES));
			doc.add(new StringField("tweetid", pieces[1], Field.Store.YES));
			doc.add(new StringField("uid", pieces[0], Field.Store.YES));
			writer.addDocument(doc);
			*/
            context.write(key, new Text(String.valueOf(tfidf)));
		}
	}
}
