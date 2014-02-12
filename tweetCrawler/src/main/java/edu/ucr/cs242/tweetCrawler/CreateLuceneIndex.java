package edu.ucr.cs242.tweetCrawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

		public Tweet(String id, String text) {
			this.tid = id;
			this.tweetText = text;
		}

	}

	private List<Tweet> tweets;

	public CreateLuceneIndex(String tweetFile, String luceneDir)
			throws IOException {
		readUserFile(tweetFile);

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
			System.out.println(doc.toString());
			writer.addDocument(doc);
		}

	}

	private void readUserFile(String tweetFile) {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(tweetFile));
			line = br.readLine();
			do {
				// extract UserId
				Long userId = Long.parseLong(line.substring(0,
						line.indexOf('\t')));
				String tid = "0";
				String text = "Testing";
				tweets.add(new Tweet(tid, text));
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
		CreateLuceneIndex luceneCreator = new CreateLuceneIndex(args[0],
				args[1]);
	}
}
