package edu.ucr.cs242.tweetCrawler;

import gnu.trove.set.hash.THashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
import twitter4j.User;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class Crawler {

    private final Set<Long> userSet;
    private final Set<Long> crawledSet;
    private final Twitter twitterAPI;
    private final ListeningScheduledExecutorService crawlThreadpool;
    private final BufferedWriter tweetLog;
    private final BufferedWriter retweetLog;
    private final BufferedWriter favoritesLog;
    private final BufferedWriter crawlLog;
    private final List<ListenableFuture<RateLimitStatus>> totalFutureList;
    private final int crawlerId;
    private final int crawlerTotalNum;
    
    private final int CRAWLED_TWEETS_PRE_USER = 50;

    private static Logger logger = LoggerFactory.getLogger(Crawler.class);

    /**
     * @param userNamesFile
     *            input file, containing the list of unique users and number of their followers (unrelated) in the format 'userId\tnumberOfFollowers'
     * @throws IOException
     */
    public Crawler(String userNamesFile, String tweetFile, String retweetFile, String favoritesFile, String crawlLogFile, int crawlerId, int crawlerTotalNum) throws IOException {
        this.userSet = new THashSet<Long>(0);
        this.crawledSet = new THashSet<Long>(0);
        readUserFile(userNamesFile);
        readCrawlLogFile(crawlLogFile);
        this.twitterAPI = new TwitterFactory().getInstance();
        this.crawlThreadpool = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(50));
        this.tweetLog = new BufferedWriter(new FileWriter(tweetFile, true));
        this.retweetLog = new BufferedWriter(new FileWriter(retweetFile, true));
        this.favoritesLog = new BufferedWriter(new FileWriter(favoritesFile, true));
        this.crawlLog = new BufferedWriter(new FileWriter(crawlLogFile, true));
        this.totalFutureList = new LinkedList<ListenableFuture<RateLimitStatus>>();
        this.crawlerId = crawlerId;
        this.crawlerTotalNum = crawlerTotalNum;
    }

    public void crawlTweets() throws InterruptedException, ExecutionException {
        crawlTweets(userSet);
    }

    /**
     * Main crawler method
     * 
     * @param users
     *            collection of user IDs, from which tweets will be collected
     * @param crawlerId
     *            crawler's ID (needed for distributed crawling)
     * @param crawlerTotalNum
     *            total number of crawler instances (needed for distributed crawling)
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TwitterException 
     */
    public void crawlTweets(final Collection<Long> users) {
        long[] usersForLookup = new long[100];
        int i = 0;
        int j = 0;
        for (final Long userId : users) {
            if (i < 100)
                usersForLookup[i++] = (long)userId;
            else {
                logger.info("Looking up users of batch #" + j++ /*+ " with timeout " + totalTimeout
                        + "s"*/);
                ResponseList<User> fileredUsers = null;
                try{
                    fileredUsers = twitterAPI.lookupUsers(usersForLookup);
                } catch (TwitterException te) {
                    if (te.exceededRateLimitation()) {
                        logger.error("UserLookup API call limit was exeeded. Rescheduling task after " + te.getRateLimitStatus().getSecondsUntilReset() + " s");
                        try {
                            crawlThreadpool.schedule(new Runnable() {
                                
                                public void run() {
                                    crawlTweets(users);
                                }
                            }, te.getRateLimitStatus().getSecondsUntilReset(), TimeUnit.SECONDS).get();
                        } catch (Exception e) {
                            logger.error("Error during userLookup for userIds " + Joiner.on(", ").join(Longs.asList(usersForLookup)));
                            te.printStackTrace();                            
                        }
                    }
                    else {
                        logger.error("Error during userLookup for userIds " + Joiner.on(", ").join(Longs.asList(usersForLookup)));
                        te.printStackTrace();
                    }
                }

                int getTimelineTimeout = 0;
                int totalTimeout = 0;
                int k = 0;
                int callsLeft = Integer.MAX_VALUE;
                ListenableScheduledFuture<RateLimitStatus> timelineTimeoutFuture = null;
                List<ListenableScheduledFuture<RateLimitStatus>> timelineFutureList = new LinkedList<ListenableScheduledFuture<RateLimitStatus>>();
                for (final User user : fileredUsers) {
                    //check if the user was already crawled
                    if (crawledSet.contains(user.getId())) {
                        logger.info("User #" + user.getId() + " was already crawled");
                        continue;
                    }
                    //check if user is protected
                    if (user.isProtected())
                        continue;
                    //if API call limit is exhausted do a blocking call;
                    if (callsLeft == 0) {
                        logger.info("All GetTimeline API calls were schduled");
                        try {
                            RateLimitStatus limitStatus = timelineTimeoutFuture.get();
                            getTimelineTimeout = 0;
                            totalTimeout = 0;
                            logger.info("Sleeping for 5 minutes, wating for the next hour");
                            //should not execute this branch, add 5 minute sleep just in case
                            if (limitStatus.getLimit() == 0)
                            Thread.sleep(30000);
                        } catch(Exception e) {
                            logger.error("Failed to retrieve timeline for user #" + user.getId() + ":" + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                    //simple crawling distribution: each crawler process only 1/crawlerTotalNum of all users
                    if (k == crawlerId) {
                        totalTimeout += getTimelineTimeout;
                        logger.info("Scheduling timeline retrieval for user #" + user.getId() + " with timeout " + totalTimeout
                                + "s");
                        timelineTimeoutFuture = crawlThreadpool.schedule(
                                new GetTimelineCallable(user), totalTimeout, TimeUnit.SECONDS);
                        callsLeft--;
                        totalFutureList.add(timelineTimeoutFuture);
                        timelineFutureList.add(timelineTimeoutFuture);
                        //if we have not figured out the timeout
                        if (getTimelineTimeout == 0)
                            try {
                                RateLimitStatus limitStatus = timelineTimeoutFuture.get();
                                getTimelineTimeout = getScheduleInterval(limitStatus);
                                totalTimeout += getTimelineTimeout;
                                callsLeft = limitStatus.getRemaining();
                            } catch (Exception e) {
                                logger.error("Failed to retrieve timeline for user #" + user.getId() + ":" + e.getMessage());
                                e.printStackTrace();
                            }
                    }
                    k = (k + 1) % crawlerTotalNum;
                }
                try {
                    Futures.allAsList(timelineFutureList).get();
                } catch (Exception e) {
                    logger.error("Failed to retrieve timelines for users batch #" + j + " " + Joiner.on(", ").join(Longs.asList(usersForLookup)));
                    e.printStackTrace();
                }
                i = 0;
                usersForLookup = new long[100];
            }
        }

        try {
            Futures.allAsList(totalFutureList).get();
        } catch (Exception e) {
            logger.error("Failure during crawling");
            e.printStackTrace();
        }
        finally {
            crawlThreadpool.shutdown();
            logger.info("!!!! Crawling ended!!!!");
        }
    }

    /**
     * Callable class, which retrieves user's timeline
     */
    class GetTimelineCallable implements Callable<RateLimitStatus> {
        private final User user;

        public GetTimelineCallable(User user) {
            this.user = user;
        }

        public RateLimitStatus call() throws IOException, ExecutionException, InterruptedException {
            try {
                Paging paging = new Paging(1, 200);
                ResponseList<Status> tweets = twitterAPI.getUserTimeline(user.getId(), paging);

                logger.info("GetTimeline API calls remaining:" + tweets.getRateLimitStatus().getRemaining());

                int getRetweetsTimeout = 0;
                int totalTimeout = 0;
                int callsLeft = Integer.MAX_VALUE;
                ListenableScheduledFuture<RateLimitStatus> retweetFuture = null;
                LinkedList<ListenableFuture<RateLimitStatus>> retweetCrawlFutureList = new LinkedList<ListenableFuture<RateLimitStatus>>();
                int i = 0;
                for (Status tweet : tweets) {
                    if (tweet.isRetweet()) {
                        if (userSet.contains(tweet.getRetweetedStatus().getUser().getId())) {
                            retweetLog.append(tweet.getRetweetedStatus().getId() + "\t" + tweet.getRetweetedStatus().getUser().getId() + "\t" + tweet.getUser().getId() +",\n");
                            retweetLog.flush();
                        }
                        continue;
                    }
                    //ignore mention conversations
                    if (tweet.getInReplyToScreenName() != null)
                        continue;
                    //break if exceeded tweet quota per user
                    if (i++ >= CRAWLED_TWEETS_PRE_USER)
                        break;
                    //if API call limit is exhausted do blocking call;
                    if (callsLeft == 0) {
                        logger.info("All GetRetweet API calls were scheduled");
                        RateLimitStatus limitStatus = retweetFuture.get();
                        getRetweetsTimeout = 0;
                        totalTimeout = 0;
                        //should not execute this branch, add 5 minute sleep just in case
                        logger.info("Sleeping for 5 minutes, wating for the next hour");
                        if (limitStatus.getLimit() == 0)
                            Thread.sleep(30000);
                    }
                    tweetLog.write(tweet.getUser().getId() + "\t" + tweet.getId() + "\t"
                            + tweet.getText().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ') + "\n");
                    tweetLog.flush();
                    if (tweet.getFavoriteCount() > 0) {
                        favoritesLog.append(tweet.getId() + "\t" + tweet.getFavoriteCount() + "\n");
                        favoritesLog.flush();
                    }
                    if (tweet.isRetweeted()) {
                        totalTimeout += getRetweetsTimeout;
                        logger.info("Scheduling retweet retrieval for tweet #" + tweet.getId() + " with timeout "
                                + totalTimeout + "s");
                        retweetFuture = crawlThreadpool.schedule(
                                new GetRetweetsCallable(tweet), totalTimeout, TimeUnit.SECONDS);
                        callsLeft--;
                        retweetCrawlFutureList.add(retweetFuture);
                        totalFutureList.add(retweetFuture);
                        //if we have not figured out the timeout
                        if (getRetweetsTimeout == 0) {
                            RateLimitStatus limitStatus = retweetFuture.get();
                            getRetweetsTimeout = getScheduleInterval(limitStatus);
                            totalTimeout += getRetweetsTimeout;
                            callsLeft = limitStatus.getRemaining();
                        }
                    }
                }
                Futures.allAsList(retweetCrawlFutureList).addListener(new Runnable() {
                    
                    public void run() {
                        try {
                            crawlLog.append(user.getId() + "\n");
                            crawlLog.flush();
                        } catch (IOException e) {
                            logger.error("Failed to append user#" + user.getId() + " record to crawlLog: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }, crawlThreadpool);

                return tweets.getRateLimitStatus();
            } catch (TwitterException te) {
                if (te.exceededRateLimitation()) {
                    logger.error("GetTimeline API call limit was exeeded. Rescheduling task after " + te.getRateLimitStatus().getSecondsUntilReset() + " s");
                    crawlThreadpool.schedule(this, te.getRateLimitStatus().getSecondsUntilReset(), TimeUnit.SECONDS);
                    return te.getRateLimitStatus();
                }
                return null;
            }
        }
    }

    /**
     * Callable class, which retrieves tweet's retweeter IDs
     */
    class GetRetweetsCallable implements Callable<RateLimitStatus> {

        private final Status tweet;

        public GetRetweetsCallable(Status tweet) {
            this.tweet = tweet;
        }

        public RateLimitStatus call() throws IOException {
            StringBuilder sb = new StringBuilder();

            sb.append(tweet.getId()).append('\t').append(tweet.getUser().getId()).append('\t');
            try {
                IDs reTweetReturn = twitterAPI.getRetweeterIds(tweet.getId(), 200, -1);
    
                logger.info("GetRetweet API calls remaining:" + reTweetReturn.getRateLimitStatus().getRemaining());
    
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
                retweetLog.flush();
                if (i > 0)
                    logger.info("Discovered " + i + " retweets for tweet #" + tweet.getId());
                return reTweetReturn.getRateLimitStatus();
            } catch (TwitterException te) {
                if (te.exceededRateLimitation()) {
                    logger.error("GetRetweet API call limit was exeeded. Rescheduling task after " + te.getRateLimitStatus().getSecondsUntilReset() + " s");
                    crawlThreadpool.schedule(this, te.getRateLimitStatus().getSecondsUntilReset(), TimeUnit.SECONDS);
                    return te.getRateLimitStatus();
                }
                return null;
            }
        }

    }

    /**
     * Method constructs a set of unique user IDs from the user-followers file
     * 
     * @param userNamesFile
     *            file containing userId and number of user's followers
     */
    private void readUserFile(String userNamesFile) {
        String line = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(userNamesFile));
            line = br.readLine();
            while (line != null) {
                // extract UserId
                Long userId = Long.parseLong(line.substring(0, line.indexOf('\t')));
                userSet.add(userId);
                line = br.readLine();
            } 
        } catch (IOException e) {
            logger.error("Failed to read userFile: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void readCrawlLogFile(String crawlLogFile) {
        String line = null;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(crawlLogFile));
            line = br.readLine();
            int i = 0;
            while (line != null) {
                i++;
                // extract crawled UserId
                Long userId = Long.parseLong(line);
                crawledSet.add(userId);
                line = br.readLine();
            } 
            logger.info("Read " + i + " already crawled users from crawlLogFile");
        } catch (IOException e) {
            logger.error("Failed to read crawlLogFile: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Method returns timeout before the next Twitter API call (to distribute API calls uniformly)
     * 
     * @param limit
     *            status, retrieved from Twitter API call
     * @return timeout
     */
    private int getScheduleInterval(RateLimitStatus limit) {
        return limit.getSecondsUntilReset() / limit.getRemaining();
    }

    public static void main(String[] args) {
        if (args.length < 6) {
            logger.info("Usage: Crawler [userFollowerFile] [outputTweetFile] [outputRetweetFile] [favoritesFile] [crawlLog] [crawlerId] [crawlerTotalNum]");
            System.exit(-1);
        }
        try {
            Crawler tweetCrawler = new Crawler(args[0], args[1], args[2], args[3], args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]));
            tweetCrawler.crawlTweets();
        } catch (NumberFormatException e) {
            logger.error("Error while parsing program arguments:" + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("Error while writing output file:" + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("Error while executing crawl job:" + e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            logger.error("Error while executing crawl job:" + e.getMessage());
            e.printStackTrace();
        }
    }
}