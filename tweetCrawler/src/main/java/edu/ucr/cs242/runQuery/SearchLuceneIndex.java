package edu.ucr.cs242.runQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class SearchLuceneIndex {

	IndexReader reader;
	IndexSearcher searcher;
	Analyzer analyzer;
	QueryParser parser;
	ScoreDoc[] hits;
	public Map<Double,String> sortedMap;
	
    private static class ScoreComparator implements Comparator{

		@Override
		public int compare(Object o1, Object o2) {
			if(Double.parseDouble(o2.toString()) > Double.parseDouble(o1.toString())){
				return 1;
			}
			return -1;
		}
      
    }

	public SearchLuceneIndex(String luceneDir, String tweetDir, String allwords)
			throws IOException {
		String[] words = allwords.split("\\:");
		reader = DirectoryReader.open(FSDirectory.open(new File(luceneDir)));
		searcher = new IndexSearcher(reader);
		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		parser = new QueryParser(Version.LUCENE_40, "text", analyzer);

		HashMap<String, Double> tidToScore = getMap(words[0]);
		if (tidToScore == null) {
			System.out.println("error");
			// Quit and output bad HTML
		}
		for (int j = 1; j < words.length; j++) {
			HashMap<String, Double> combination = new HashMap<String, Double>();
			HashMap<String, Double> nextWord = getMap(words[j]);
			Set set = tidToScore.entrySet();
			// Get an iterator
			Iterator i = set.iterator();
			// Display elements
			while (i.hasNext()) {
				Map.Entry me = (Map.Entry) i.next();
				if (nextWord.containsKey(me.getKey())) {
					double score1 = Double
							.parseDouble(me.getValue().toString());
					double score2 = Double.parseDouble(nextWord
							.get(me.getKey()).toString());
					combination.put(me.getKey().toString(), score1 + score2);
				}
				tidToScore = combination;
			}
		}
		
		HashMap<Double, String> scoreToTid = new HashMap<Double, String>();
		Set set = tidToScore.entrySet();
		Iterator i = set.iterator();
		int j = 0;
		while (i.hasNext()) {
			Map.Entry me = (Map.Entry) i.next();
			scoreToTid.put(Double.parseDouble(me.getValue().toString()), me.getKey().toString());
		}	
		
        List<Double> keys = new LinkedList<Double>(scoreToTid.keySet());

        Collections.sort(keys, new ScoreComparator());
        sortedMap = new LinkedHashMap<Double,String>();
        for(Double key: keys){
            sortedMap.put(key.doubleValue(), scoreToTid.get(key));
        }


	}
	public String getTweetText(String tweetDir, String tid){
		int fileNum;
		if (tid.length() > 5){
			fileNum = tid.substring(0, 5).hashCode() % 1000;
		}
		else{
			fileNum = tid.hashCode() % 1000;
		}
		String fileName = String.valueOf(fileNum);
		while (fileName.length() < 5){
			fileName = "0" + fileName;
		}
		fileName = "part-r-" + fileName;
		String fullPath = tweetDir + "/" + fileName;
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fullPath));
			line = br.readLine();
			while (line != null) {
				// extract UserId
				String[] pieces = line.split("\"");
				if (pieces[1].equals(tid)){
					br.close();
					return pieces[3];
				}
				line = br.readLine();
			}
		} catch (IOException e) {
			System.err.println("Failed to read userFile: " + e.getMessage());
		}
		return "Error";
	}

	public HashMap<String, Double> getMap(String word) throws IOException {
		HashMap<String, Double> tidToScore = new HashMap<String, Double>();
		word = word.replaceAll("[^a-zA-Z0-9]", "");

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

		// Check if stop word
		Set<Object> set = StandardAnalyzer.STOP_WORDS_SET;
		if (!set.contains(stemmedWord) && !stemmedWord.equals("")) {

		} else {
			return null;
		}
		Query query = null;
		String special = stemmedWord;
		TopDocs results = null;
		try {
			query = parser.parse(special);
			try {
				results = searcher.search(query, 100000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		hits = results.scoreDocs;
		while (hits.length > 0) {
			for (int i = 0; i < hits.length; i++) {
				Document doc = searcher.doc(hits[i].doc);
				tidToScore.put(doc.get("id"),
						Double.parseDouble(doc.get("score")));
			}
			ScoreDoc[] newhits = searcher.searchAfter(hits[hits.length - 1],
					query, null, 100000).scoreDocs;
			hits = newhits;
		}
		return tidToScore;

	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("Usage [indexfolder] [Tweetindex] [searchwords separated by spaces]");
			System.exit(-1);
		}
		String words = args[2];
		for (int i = 3; i < args.length; i++) {
			words += (":" + args[i]);
		}
		SearchLuceneIndex luceneSearcher = new SearchLuceneIndex(args[0], args[1], words);

		System.out.println("Done");
	}
}
