package edu.ucr.cs242.tweetCrawler;

import java.util.ArrayList;
import java.util.List;

import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

public class GetTweetsByUser {
    /**
     * Usage: java twitter4j.examples.search.SearchTweets [query]
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("java twitter4j.examples.search.SearchTweets [username]");
            System.exit(-1);
        }
        Twitter twitter = new TwitterFactory().getInstance();
        try {
        	Query query = new Query("from:" + args[0]);
        	query.setSince("2011-01-01");
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                	List<Status> retweets = twitter.getRetweets(tweet.getId());
                	List <String> reTweetList = new ArrayList<String>();
                    for (Status retweet : retweets) {
                    	if (true){// This should be our check to confirm that the user of the retweet is one of our users.
                    		reTweetList.add(retweet.getUser().getScreenName());
                    	}
                    }
                    System.out.println("User:" + tweet.getUser().getScreenName() + " Text:" + tweet.getText() + " Users who retweeted:" + reTweetList);
                }
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }
}