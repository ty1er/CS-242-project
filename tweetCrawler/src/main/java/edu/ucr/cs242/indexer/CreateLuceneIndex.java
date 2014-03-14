package edu.ucr.cs242.indexer;

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


	public CreateLuceneIndex(String tweetFile, String luceneDir)
			throws IOException {

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
		indexTweets(writer, tweetFile);
		System.out.println("done");
		
		// This makes write slower but search faster.
		writer.forceMerge(1);

		writer.close();

	}

	private void indexTweets(IndexWriter writer, String tweetFile) throws IOException {
		System.out.println("Reading File");
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(tweetFile));
			line = br.readLine();
			int count = 0;
			while (line != null) {
				// extract UserId
				String word = line.substring(0,line.indexOf('\t'));
				line = line.substring(line.indexOf('\t')+1);
				
				String[] pieces = line.split("\\:");
				
				Document doc = new Document();
				doc.add(new TextField("text", word, Field.Store.YES));
				doc.add(new StoredField("id", pieces[0]));
				doc.add(new StoredField("score", pieces[1]));

				writer.addDocument(doc);
				line = br.readLine();
				count ++;
				if (count % 100 == 0){
					System.out.println("100 words");
					count = 0;
				}
			}
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
