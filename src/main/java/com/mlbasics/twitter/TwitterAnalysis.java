package com.mlbasics.twitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TwitterAnalysis implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = -4110328812533728836L;
	public int characterCount;
	public int wordsTweeted;
	public List<String> hashTag;


	public TwitterAnalysis(int wordsTweeted, int characterCount,  List<String> hash) {
		super();
		this.wordsTweeted = wordsTweeted;
		this.hashTag = hash;
		this.characterCount = characterCount;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TwitterAnalysis [characterCount=").append(characterCount).append(", wordsTweeted=")
		.append(wordsTweeted).append(", hashTag=").append(hashTag).append("]");
		return builder.toString();
	}


	public int getWordsTweeted() {
		return wordsTweeted;
	}

	public void setWordsTweeted(int wordsTweeted) {
		this.wordsTweeted = wordsTweeted;
	}

	public List<String> getHashTag() {
		return hashTag;
	}

	public void setHashTag(List<String> hashTag) {
		this.hashTag = hashTag;
	}

	public int getCount() {
		return characterCount;
	}

	public void setCount(int characterCount) {
		this.characterCount = characterCount;
	}

	public static void main(String[] args) {
		try {

			/* Turning off unwanted logs to view the stream */ 
			Logger.getLogger("org").setLevel(Level.OFF);
			Logger.getLogger("akka").setLevel(Level.OFF);

			/*
			 * Setting consumerKey, consumerSecret, accessToken and accessTokenSecret for
			 * streaming Data from Twitter API
			 */
			System.setProperty("twitter4j.oauth.consumerKey","a" );
			System.setProperty("twitter4j.oauth.consumerSecret", "a");
			System.setProperty("twitter4j.oauth.accessToken","a" );
			System.setProperty("twitter4j.oauth.accessTokenSecret","a" );

			/* Initializing the spark context */
			SparkConf sparkConf = new SparkConf()
					.setAppName("Twitter Stream Analysis")
					.setMaster("local[2]");
			/*
			 * Initializing the java streaming context and specifying the batch duration to
			 * 30 seconds so that the tweets are retrieved. 
			 */
			JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

			/*
			 * Instantiating the JavaReceiverInputDStream  , the abstract class for defining 
			 * any input stream that receives data over the network.			
			 */
			JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

			/*
			 * Code extracts the statuses from response of the twitterAPI excluding other tags such as Geo locations etc.
			 */
			JavaDStream<String> statuses = twitterStream.map(status -> status.getText());
			statuses.print();
			/*
			 * hashTagArray is an object of Twitter Analysis class which extracts 
			 * 1. All the words in a tweet.
			 * 2. Length of all the characters in a tweet
			 * 3. Hastags contained in a tweet
			 */

			/*
			 * Words are split using the String split function to return an array and the length of the 
			 * array is calculated.
			 * Number of character is counted by using the length function
			 * The hash tags are extracted using the startsWith function of the String class 
			 */
			JavaDStream<TwitterAnalysis> hashTagArray = statuses.map(s -> {
				List<String> hash = new ArrayList<String>();
				String[] words;
				words = s.split(" ");
				int size = words.length;
				for (String a : words) {
					if(a.startsWith("#")) {
						hash.add(a.substring(1, a.length()));
					}
				}
				return new TwitterAnalysis(size, s.length(), hash);
			});
			hashTagArray.print();


			/*
			 * The average of both words and character is calculated using the mean function
			 */			
			hashTagArray.foreachRDD(tags -> {
				if (tags.count() > 0) {
					double characterAverage = tags.mapToDouble(characters -> characters.getCount()).mean();
					System.out.println("Average characters in a tweet without window: " + characterAverage);
				}
			});

			hashTagArray.foreachRDD(tags -> {
				if (tags.count() > 0) {
					double wordAverage = tags.mapToDouble(words -> words.getWordsTweeted()).mean();
					System.out.println("Average Words in tweet without widow: " + wordAverage);
				}
			});


			/*
			 * Top 10 hashtag 
			 */
			JavaDStream<String> hashSet = hashTagArray.flatMap(a -> a.getHashTag().iterator());

			/*
			 * counts the occurance of a hash tag by mapping to a tuple and sorting based on the key
			 * which is the count in this case
			 */
			JavaPairDStream<Long, String> topHash = hashSet.countByValue().mapToPair(Tuple2::swap)
					.transformToPair(hcswap -> hcswap.sortByKey());
			//.mapToPair(Tuple2::swap);

			topHash.print(10);

			/*
			 *  Calculation of average word count, character count and top 10 hash tag based on window duration
			 */

			/*
			 * Prints the average characters in a tweet 
			 */
			hashTagArray.window(Durations.minutes(5), Durations.seconds(30))
			.foreachRDD(tags -> {
				if (tags.count() > 0) {
					double characterAverage = tags.mapToDouble(characters -> characters.getCount()).mean();
					System.out.println("Average characters in a tweet with window: " + characterAverage);
				}
			});

			/*
			 * Prints the average words in a tweet
			 */
			hashTagArray.window(Durations.minutes(5), Durations.seconds(30))
			.foreachRDD(tags -> {
				if (tags.count() > 0) {
					double wordAverage = tags.mapToDouble(words -> words.getWordsTweeted()).mean();
					System.out.println("Average Words in tweet with window: " + wordAverage);
				}
			});

			/*
			 * Prints the top 10 hash tag based on the window duration
			 */
			JavaPairDStream<Integer, String> hashPairbyWindow = hashSet.mapToPair(hash -> new Tuple2<String, Integer>(hash, 1))
					.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(300), Durations.seconds(30))
					.mapToPair(hash -> hash.swap())
					.transformToPair(s -> s.sortByKey());
			hashPairbyWindow.print(10);

			jssc.start();
			jssc.awaitTermination();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
