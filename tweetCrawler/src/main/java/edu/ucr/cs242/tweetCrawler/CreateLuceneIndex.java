package edu.ucr.cs242.tweetCrawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class CreateLuceneIndex {
	private class Tweet {
		String tid;
		String tweetText;
		String uid;

		public Tweet(String id, String user, String text) {
			this.tid = id;
			this.tweetText = text;
			this.uid = user;
		}

	}

	private List<Tweet> tweets;

	public CreateLuceneIndex(String tweetFile, String luceneDir)
			throws IOException {
		tweets = new ArrayList<Tweet>();
		readTweetFile(tweetFile);

		System.out.println("Indexing to directory '" + luceneDir + "'...");

		Directory dir = FSDirectory.open(new File(luceneDir));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40,
				analyzer);

		// Create will overwrite the index everytime
		iwc.setOpenMode(OpenMode.CREATE);

		// Create an index writer
		IndexWriter writer = new IndexWriter(dir, iwc);

		// Add files to index
		indexTweets(writer);

		// This makes write slower but search faster.
		writer.forceMerge(1);

		writer.close();

	}

	private void indexTweets(IndexWriter writer) throws IOException {
		for (Tweet tweet : tweets) {
			Document doc = new Document();
			doc.add(new TextField("text", tweet.tweetText, Field.Store.YES));
			doc.add(new StoredField("id", tweet.tid));
			doc.add(new StoredField("uid", tweet.uid));
			writer.addDocument(doc);
		}

	}

	private void readTweetFile(String tweetFile) {
		System.out.println("Reading File");
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(tweetFile));
			line = br.readLine();
			do {
				// extract UserId
				String userId = line.substring(0,line.indexOf('\t'));
				line = line.substring(line.indexOf('\t')+1);
				
				String tid = line.substring(0,line.indexOf('\t'));
				line = line.substring(line.indexOf('\t')+1);
				
				String text = line;
				Tweet tweet = new Tweet(tid, userId, text);
				tweets.add(tweet);
				line = br.readLine();
			} while (line != null);
		} catch (IOException e) {
			System.err.println("Failed to read userFile: " + e.getMessage());
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage [userFile] [luceneDir]");
			System.exit(-1);
		}
		long startTime = System.currentTimeMillis();
		CreateLuceneIndex luceneCreator = new CreateLuceneIndex(args[0],
				args[1]);
		long endTime = System.currentTimeMillis();
		System.out.println("Total execution time:" + (endTime-startTime));
	}
}
