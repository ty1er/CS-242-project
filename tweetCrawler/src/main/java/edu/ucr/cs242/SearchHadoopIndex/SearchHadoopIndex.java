package edu.ucr.cs242.SearchHadoopIndex;

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


public class SearchHadoopIndex {
	
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

	public SearchHadoopIndex(String indexDir, String tweetDir, String allwords)
			throws IOException {
		String[] words = allwords.split("\\:");

		HashMap<String, Double> tidToScore = getMap(words[0], indexDir);
		if (tidToScore == null) {
			System.out.println("error");
			// Quit and output bad HTML
		}
		for (int j = 1; j < words.length; j++) {
			HashMap<String, Double> combination = new HashMap<String, Double>();
			HashMap<String, Double> nextWord = getMap(words[j], indexDir);
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
	public String getIndexEntry(String indexDir, String key, Boolean word){
		int fileNum;
		if (word && key.length() > 5){
			fileNum = key.substring(key.length()-6, key.length()-1).hashCode() % 1000;
		}
		else if (!word && key.length() > 5){
			fileNum = key.substring(0, 5).hashCode() % 1000;
		}
		else{
			fileNum = key.hashCode() % 1000;
		}
		String fileName = String.valueOf(fileNum);
		while (fileName.length() < 5){
			fileName = "0" + fileName;
		}
		fileName = "part-r-" + fileName;
		String fullPath = indexDir + "/" + fileName;
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fullPath));
			line = br.readLine();
			while (line != null) {
				// extract UserId
				String[] pieces = line.split("\"");
				if (pieces[1].equals(key)){
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

	public HashMap<String, Double> getMap(String word, String indexDir) throws IOException {
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
		String tidColonScore = getIndexEntry(indexDir, stemmedWord, true);
		String[] pieces = tidColonScore.split("\\,");
		for (String piece : pieces){
			String[] subpieces = piece.split("\\:");
			tidToScore.put(subpieces[0],
					Double.parseDouble(subpieces[1]));
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
		SearchHadoopIndex HadoopSearcher = new SearchHadoopIndex(args[0], args[1], words);

		System.out.println("Done");
	}
}
