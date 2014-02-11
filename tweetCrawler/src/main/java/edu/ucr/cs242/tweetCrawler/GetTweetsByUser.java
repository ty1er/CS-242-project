package edu.ucr.cs242.tweetCrawler;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class GetTweetsByUser {

	private final Set<Long> userSet;
	private final Twitter twitter;
	private final ExecutorService userThreadpool;
	private final ExecutorService retweetThreadpool;

	// input file format 'userId numberOfFollowers'
	public GetTweetsByUser(String userNamesFile) {
		userSet = new THashSet<Long>(0);
		readUserFile(userNamesFile);
		twitter = new TwitterFactory().getInstance();
		userThreadpool = Executors.newFixedThreadPool(1);
		retweetThreadpool = Executors.newScheduledThreadPool(20);

		crawlTweets("katyperry");
		
	}

	private void readUserFile(String userNamesFile) {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(userNamesFile));
			line = br.readLine();
			do {
				// extract UserId
				Long userId = Long.parseLong(line.substring(0,
						line.indexOf('\t')));
				userSet.add(userId);
				line = br.readLine();
			} while (line != null);
		} catch (IOException e) {
			System.err.println("Failed to read userFile: " + e.getMessage());
		}
	}

	private void crawlTweets(String user) {
		try {
			int numRequests = 0;

			Paging paging = new Paging(1, 200);
			List<Status> tweets = twitter.getUserTimeline(user, paging);
			numRequests++;

			for (Status tweet : tweets) {
				if (tweet.isRetweet()){
					continue;
					
				}
				List <Long> reTweetList = new ArrayList<Long>();
				if (tweet.getRetweetCount() > 0){
					IDs reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200,
						-1);
					numRequests++;
					long[] retweetIds = reTweetReturn.getIDs();
					for (long retweetId : retweetIds) {
						if (userSet.contains(retweetId)) {
							reTweetList.add(retweetId);
						}
					}
				}

				System.out.println("User:" + tweet.getUser().getScreenName()
						+ " Date:" + tweet.getCreatedAt() + " Text:"
						+ tweet.getText() + " Number of reTweets:"
						+ tweet.getRetweetCount() + " Number from our data set:" + reTweetList.size() + " Users from our set who retweeted:"
						+ reTweetList);
			}

			System.exit(0);
		} catch (TwitterException te) {
			te.printStackTrace();
			System.out.println("Failed to search tweets: " + te.getMessage());
			System.exit(-1);
		}
	}

	/**
	 * Usage: java twitter4j.examples.search.SearchTweets [query]
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage [userFile]");
			System.exit(-1);
		}
		GetTweetsByUser tweetCrawler = new GetTweetsByUser(args[0]);
	}
}