package edu.ucr.cs242.tweetCrawler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import twitter4j.IDs;
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
        	int numRequests = 0;
        	query.setSince("2013-02-10");
        	query.setUntil("2014-02-10");
        	query.setResultType("recent");
        	query.setCount(100);
            QueryResult result;
            do {
                result = twitter.search(query);
                numRequests++;
                List<Status> tweets = result.getTweets();
                //Get all tweets from the month
                while (result.nextQuery() != null){
                	result = twitter.search(result.nextQuery());
                	List<Status> moreTweets = result.getTweets();
                	numRequests++;
                	tweets.addAll(moreTweets);
                }
                System.out.println("Number of requests to get all tweets:" + numRequests);
            	System.out.println("Number of tweets by " + args[0] + ":" + tweets.size() + " Starting:" + tweets.get(0).getCreatedAt());
                for (Status tweet : tweets) {
                	IDs reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200, -1);
                	numRequests++;
                	long[] retweetIds = reTweetReturn.getIDs();
                	List<Long> listIDs = new ArrayList<Long>();

                	for (int i =0; i < retweetIds.length; i++){
                		listIDs.add(retweetIds[i]);
                	}
                	
                	//Get all retweets
                	System.out.println("Retweet next cursor was:" + reTweetReturn.getNextCursor());
                	while (reTweetReturn.getNextCursor() > 0){
                		reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200, reTweetReturn.getNextCursor());
                		retweetIds = reTweetReturn.getIDs();
                    	for (int i =0; i < retweetIds.length; i++){
                    		listIDs.add(retweetIds[i]);
                    	}
                		numRequests++;
                	}
                	System.out.println("Number of requests to get all tweets + retweets:" + numRequests);
             
                	List <Long> reTweetList = new ArrayList<Long>();
                    for (long retweetId : listIDs) {
                    	if (true){// TODO: This should be our check to confirm that the user of the retweet is one of our users.
                    		reTweetList.add(retweetId);
                    	}
                    }

                    System.out.println("User:" + tweet.getUser().getScreenName() + " Date:" + tweet.getCreatedAt() + " Text:" + tweet.getText() + " Number of reTweets:" + reTweetList.size() + " Users who retweeted:" + reTweetList);
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