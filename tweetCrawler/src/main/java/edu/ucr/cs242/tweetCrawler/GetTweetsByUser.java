package edu.ucr.cs242.tweetCrawler;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class GetTweetsByUser {

    private final Set<Long> userSet;
    private final Twitter twitter;
    private final ScheduledExecutorService retweetThreadpool;

    // input file format 'userId numberOfFollowers'
    public GetTweetsByUser(String userNamesFile) {
        userSet = new THashSet<Long>(0);
        readUserFile(userNamesFile);
        twitter = new TwitterFactory().getInstance();
        retweetThreadpool = Executors.newScheduledThreadPool(20);
    }

    class GetTimelineCallable implements Callable<Long> {
        private final ScheduledExecutorService scheduledExecutorService;
        private final Long userId;

        public GetTimelineCallable(ScheduledExecutorService executorService, Long userId) {
            scheduledExecutorService = executorService;
            this.userId = userId;
        }

        public Long call() throws Exception {
            try {
                Paging paging = new Paging(1, 200);
                ResponseList<Status> tweets = twitter.getUserTimeline(userId, paging);

                Long getRetweetsTimeout = 0l;
                for (Status tweet : tweets) {
                    Future<Long> retweetTimelineFuture = scheduledExecutorService.schedule(new GetRetweetsCallable(
                            tweet), getRetweetsTimeout, TimeUnit.SECONDS);
                    getRetweetsTimeout = retweetTimelineFuture.get();
                }

                return getScheduleInterval(tweets.getRateLimitStatus());
            } catch (TwitterException te) {
                te.printStackTrace();
                System.out.println("Failed to search tweets: " + te.getMessage());
                return 0l;
            }
        }
    }

    class GetRetweetsCallable implements Callable<Long> {

        private final Status tweet;

        public GetRetweetsCallable(Status tweet) {
            this.tweet = tweet;
        }

        public Long call() throws Exception {
            if (tweet.isRetweet()){
                return 0l;
                
            }
            long timeout = 0;
            List <Long> reTweetList = new ArrayList<Long>();
            if (tweet.getRetweetCount() > 0){
                IDs reTweetReturn = twitter.getRetweeterIds(tweet.getId(), 200, -1);
                long[] retweetIds = reTweetReturn.getIDs();
                for (long retweetId : retweetIds) {
                    if (userSet.contains(retweetId)) {
                        reTweetList.add(retweetId);
                    }
                }
                timeout = getScheduleInterval(reTweetReturn.getRateLimitStatus());
            }

            System.out.println("User:" + tweet.getUser().getScreenName() + " Date:" + tweet.getCreatedAt() + " Text:"
                    + tweet.getText() + " Number of reTweets:" +tweet.getRetweetCount() + " Number from our data set:" + reTweetList.size() + " Users from our set who retweeted:"
                    + reTweetList);

            return timeout;
        }

    }

    public void crawlTweets() {
        crawlTweets(userSet);
    }
    
    public void crawlTweets(Collection<Long> users) {
        Long getTimelineTimeout = 0l;
        for (final Long user : users) {
            Future<Long> timelineTimeoutFuture = retweetThreadpool.schedule(new GetTimelineCallable(retweetThreadpool,
                    user), getTimelineTimeout, TimeUnit.SECONDS);
            try {
                getTimelineTimeout = timelineTimeoutFuture.get();
            } catch (Exception e) {
                System.out.println("Failed to retrieve timeline for user #" + user + ":" + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void readUserFile(String userNamesFile) {
        String line = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(userNamesFile));
            line = br.readLine();
            do {
                // extract UserId
                Long userId = Long.parseLong(line.substring(0, line.indexOf('\t')));
                userSet.add(userId);
                line = br.readLine();
            } while (line != null);
        } catch (IOException e) {
            System.err.println("Failed to read userFile: " + e.getMessage());
        }
    }

    private long getScheduleInterval(RateLimitStatus limit) {
        return limit.getSecondsUntilReset() / limit.getRemaining();
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
//        tweetCrawler.crawlTweets(new Long[] { 12l });
        tweetCrawler.crawlTweets();
    }
}