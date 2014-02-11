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

    //input file format  'userId numberOfFollowers'
    public GetTweetsByUser(String userNamesFile) {
        userSet = new THashSet<Long>(0);
        readUserFile(userNamesFile);
        twitter = new TwitterFactory().getInstance();
        userThreadpool = Executors.newFixedThreadPool(1);
        retweetThreadpool = Executors.newScheduledThreadPool(20);
    }

    private void readUserFile(String userNamesFile) {
        String line = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(userNamesFile));
            line = br.readLine();
            do {
                //extract UserId
                Long userId = Long.parseLong(line.substring(0, line.indexOf('\t')));
                userSet.add(userId);
                line = br.readLine();
            } while (line != null);
        } catch (IOException e) {
            System.err.println("Failed to read userFile: " + e.getMessage());
        }
    }

    private void crawlTweets(String user) {
        try {
            Query query = new Query("from:" + user);
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
                while (result.nextQuery() != null) {
                    result = twitter.search(result.nextQuery());
                    List<Status> moreTweets = result.getTweets();
                    numRequests++;
                    tweets.addAll(moreTweets);
                }
                System.out.println("Number of requests to get all tweets:" + numRequests);
                System.out.println("Number of tweets by " + user + ":" + tweets.size() + " Starting:"
                        + tweets.get(0).getCreatedAt());
                for (Status tweet : tweets) {
                    IDs reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200, -1);
                    numRequests++;
                    long[] retweetIds = reTweetReturn.getIDs();
                    List<Long> listIDs = new ArrayList<Long>();

                    for (int i = 0; i < retweetIds.length; i++) {
                        listIDs.add(retweetIds[i]);
                    }

                    //Get all retweets
                    System.out.println("Retweet next cursor was:" + reTweetReturn.getNextCursor());
                    while (reTweetReturn.getNextCursor() > 0) {
                        reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200, reTweetReturn.getNextCursor());
                        retweetIds = reTweetReturn.getIDs();
                        for (int i = 0; i < retweetIds.length; i++) {
                            listIDs.add(retweetIds[i]);
                        }
                        numRequests++;
                    }
                    System.out.println("Number of requests to get all tweets + retweets:" + numRequests);

                    List<Long> reTweetList = new ArrayList<Long>();
                    for (long retweetId : listIDs) {
                        // TODO: This should be our check to confirm that the user of the retweet is one of our users.
                        if (userSet.contains(retweetId)) {
                            reTweetList.add(retweetId);
                        }
                    }

                    System.out.println("User:" + tweet.getUser().getScreenName() + " Date:" + tweet.getCreatedAt()
                            + " Text:" + tweet.getText() + " Number of reTweets:" + reTweetList.size()
                            + " Users who retweeted:" + reTweetList);
                }
            } while ((query = result.nextQuery()) != null);
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