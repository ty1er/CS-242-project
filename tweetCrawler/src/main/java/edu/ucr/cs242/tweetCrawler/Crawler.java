package edu.ucr.cs242.tweetCrawler;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class Crawler {

    private final Set<Long> userSet;
    private final Twitter twitterAPI;
    private final ScheduledExecutorService crawlThreadpool;
    private final BufferedWriter tweetLog;
    private final BufferedWriter retweetLog;

    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    /**
     * @param userNamesFile
     *            input file, containing the list of unique users and number of their followers (unrelated) in the format 'userId\tnumberOfFollowers'
     * @throws IOException 
     */
    public Crawler(String userNamesFile, String tweetFile, String retweetFile) throws IOException {
        userSet = new THashSet<Long>(0);
        readUserFile(userNamesFile);
        twitterAPI = new TwitterFactory().getInstance();
        crawlThreadpool = Executors.newScheduledThreadPool(20);
        tweetLog = new BufferedWriter(new FileWriter(tweetFile));
        retweetLog = new BufferedWriter(new FileWriter(retweetFile));
    }

    public void crawlTweets(int crawlerId, int crawlerTotalNum) {
        crawlTweets(userSet, crawlerId, crawlerTotalNum);
    }

    /**
     * Main crawler method
     * @param users collection of user IDs, from which tweets will be collected
     * @param crawlerId crawler's ID (needed for distributed crawling)
     * @param crawlerTotalNum total number of crawler instances (needed for distributed crawling)
     */
    public void crawlTweets(Collection<Long> users, int crawlerId, int crawlerTotalNum) {
        Long getTimelineTimeout = 0l;
        int i = 0;
        for (final Long user : users) {
            //simple crawling distribution: each crawler process only 1/crawlerTotalNum of all users
            if (i == crawlerId) {
                logger.info("Retrieving tweets from user #" + user);
                Future<Long> timelineTimeoutFuture = crawlThreadpool.schedule(new GetTimelineCallable(crawlThreadpool,
                        user), getTimelineTimeout, TimeUnit.SECONDS);
                try {
                    getTimelineTimeout = timelineTimeoutFuture.get();
                } catch (Exception e) {
                    logger.error("Failed to retrieve timeline for user #" + user + ":" + e.getMessage());
                }
            }
            i = (i+1) % crawlerTotalNum;
        }
    }    
    
    
    /**
     * Callable class, which retrieves user's timeline
     */
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
                ResponseList<Status> tweets = twitterAPI.getUserTimeline(userId, paging);

                Long getRetweetsTimeout = 0l;
                for (Status tweet : tweets) {
                    tweetLog.write(tweet.getUser().getId() + "\t" + tweet.getId() + "\t" + tweet.getText());
                    tweetLog.newLine();
                    Future<Long> retweetTimelineFuture = scheduledExecutorService.schedule(new GetRetweetsCallable(
                            tweet), getRetweetsTimeout, TimeUnit.SECONDS);
                    getRetweetsTimeout = retweetTimelineFuture.get();
                }

                return getScheduleInterval(tweets.getRateLimitStatus());
            } catch (TwitterException te) {
                logger.error("Failed to search tweets: " + te.getMessage());
                return 0l;
            }
        }
    }

    /**
     * Callable class, which retrieves tweet's retweeter IDs
     */
    class GetRetweetsCallable implements Callable<Long> {

        private final Status tweet;

        public GetRetweetsCallable(Status tweet) {
            this.tweet = tweet;
        }

        public Long call() throws Exception {
            if (tweet.isRetweet()) {
                return 0l;

            }
            StringBuilder sb = new StringBuilder();
            long timeout = 0;
            if (tweet.getRetweetCount() > 0) {
                sb.append(tweet.getId()).append('\t');
                IDs reTweetReturn = twitterAPI.getRetweeterIds(tweet.getId(), 200, -1);
                long[] retweetIds = reTweetReturn.getIDs();
                int i = 0;
                for (long retweetId : retweetIds) {
                    if (userSet.contains(retweetId)) {
                        sb.append(retweetId).append(',');
                        i++;
                    }
                }
                retweetLog.append(sb.toString());
                retweetLog.newLine();
                if (i>0)
                    logger.info("Discovered " + i + " retweets for tweet #" + tweet.getId());
                timeout = getScheduleInterval(reTweetReturn.getRateLimitStatus());
            }

//            System.out.println("User:" + tweet.getUser().getScreenName() + " Date:" + tweet.getCreatedAt() + " Text:"
//                    + tweet.getText() + " Number of reTweets:" + tweet.getRetweetCount() + " Number from our data set:"
//                    + reTweetList.size() + " Users from our set who retweeted:" + reTweetList);

            return timeout;
        }

    }


    /**
     * Method constructs a set of unique user IDs from the user-followers file
     * @param userNamesFile file containing userId and number of user's followers
     */
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
            logger.error("Failed to read userFile: " + e.getMessage());
        }
    }

    /**
     * Method returns timeout before the next Twitter API call (to distribute API calls uniformly)
     * @param limit status, retrieved from Twitter API call
     * @return timeout
     */
    private long getScheduleInterval(RateLimitStatus limit) {
        return limit.getSecondsUntilReset() / limit.getRemaining();
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            logger.info("Usage: Crawler [userFollowerFile] [outputTweetFile] [outputRetweetFile] [crawlerId] [crawlerTotalNum]");
            System.exit(-1);
        }
        try {
            Crawler tweetCrawler = new Crawler(args[0], args[1], args[2]);
            tweetCrawler.crawlTweets(Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        } catch (NumberFormatException e) {
            logger.error("Error while parsing program arguments:" + e.getMessage());
        } catch (IOException e) {
            logger.error("Error while writing output file:" + e.getMessage());
        }
    }
}