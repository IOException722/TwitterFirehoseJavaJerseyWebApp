package twitter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import common.Constant;
import common.AllObjects;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.UserMentionEntity;

@Path("start_firehose")
public class Firehose {
	private static Connection conn = null;
	private static PreparedStatement stmt = null;
	private static Client hosebirdClient=null;
	private static BlockingQueue<String> msgQueue;
	private static String allPhrases="";

	@GET
	@Produces("text/html")
	public Response getStartingPage()
	{
		String outputStartRunning = "<h1>Social Media Intelligence <h1>" +
				"<p>Firehose Service is started ... <br>Ping @ " + new Date().toString() + "</p<br>";
		String outputAlreadyRunning = "<h1>Social Media Intelligence <h1>" +
				"<p>Already a firehose Service is running ....! It can run one firehose service at a time! <br>Ping @ " + new Date().toString() + "</p<br>";
		makeJDBCConnection();
		String finalResponse =(getFirehoseStatus()==1 || getPhraseListenerStatus()==1)?outputAlreadyRunning:outputStartRunning;
		if(AllObjects.getClient()==null)
		{
			if(getFirehoseStatus()==0 && getPhraseListenerStatus()==0)
			{
			    startFirehose(Constant.phrasesDefault,1, 0);
			}
		}
		else
		{ 
		if(getFirehoseStatus()==1 || getPhraseListenerStatus()==1)
			return  Response.status(200).entity(finalResponse.toString()).build();
		}
		
		return  Response.status(200).entity(finalResponse.toString()).build();
	}
	
	public static Client getHoseBirdClientObject(List<String> phrases,int firehose_status, int phrase_listener_status)
	{
		allPhrases ="";
		List<String> trackTerms=new ArrayList<String>();
		for(int i=0;i<phrases.size();i++)
		{
			trackTerms.add(phrases.get(i).replaceAll("_", " "));
			allPhrases+=trackTerms.get(i);
			if(i<phrases.size()-1 )
				 allPhrases+=", ";
			System.out.println(trackTerms.get(i));
		}
		
		msgQueue = new LinkedBlockingQueue<String>(1000000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(trackTerms);
	    
		try 
		{
			Authentication hosebirdAuth = new OAuth1(Constant.consumerKey, Constant.consumerSecret, Constant.token, Constant.secret);	
			ClientBuilder builder = new ClientBuilder()
					  .name("Hosebird-Client-01")                              // optional: mainly for the logs
					  .hosts(hosebirdHosts)
					  .authentication(hosebirdAuth)
					  .endpoint(hosebirdEndpoint)
					  .processor(new StringDelimitedProcessor(msgQueue))
					  .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

			hosebirdClient = builder.build();
			hosebirdClient.connect();
			log("Authenticaiton successful hosebird "+ " all phrases "+ allPhrases + " ph_l "+phrase_listener_status +" fh_l "+ firehose_status );
			updateSystmeParameters(firehose_status, getCurrentTimeStamp(), null,phrase_listener_status,getCurrentTimeStamp(),allPhrases);
//			set firehose_status
		} catch (Exception e) 
		{
//			put exception in database
			updateSystmeParameters(0, getCurrentTimeStamp(), e.getMessage(), 0 , getCurrentTimeStamp(), allPhrases);
		}
		return hosebirdClient;
		
		
	}
	
	public static void startFirehose(List<String> phrases, int firehose_status, int phrase_listener_status)
	{
		hosebirdClient = null;
		allPhrases ="";
		for(int i=0;i<phrases.size();i++)
		{
			allPhrases+=phrases.get(i).replaceAll("_", " ");
			if(i<phrases.size()-1 )
				 allPhrases+=", ";
			System.out.println(phrases.get(i));
		}
		if(hosebirdClient!=null)
			hosebirdClient.stop();
		if(AllObjects.getClient()!=null)
			AllObjects.getClient().stop();
		hosebirdClient = getHoseBirdClientObject(phrases,firehose_status,phrase_listener_status);
		AllObjects.setClient(hosebirdClient);
		while (!hosebirdClient.isDone()) {
			try {
				String tweet = msgQueue.take();
				insertTweetData(tweet);
				Timestamp  current_timestamp = getCurrentTimeStamp();
				Timestamp  last_timestamp  = getLastTimeAlive();
				long diff = current_timestamp.getTime() - last_timestamp.getTime();
				if (diff/1000>60)
				{
					
					updateSystmeParameters(firehose_status, getCurrentTimeStamp(), null,phrase_listener_status,getCurrentTimeStamp(),allPhrases);
				}
				if (diff/1000>60)
				{
					int saved_firehose_status = getFirehoseStatus();
					int saved_phrase_listener_status = getPhraseListenerStatus();
					if(saved_firehose_status==0 && saved_phrase_listener_status==0)
					{
						hosebirdClient.stop();
						updateSystmeParameters(0, getCurrentTimeStamp(),null,0, getCurrentTimeStamp(),allPhrases);

					}
				}
			} catch (Exception e) {
				updateSystmeParameters(0, getCurrentTimeStamp(), "Message is: "+e.getMessage()+"\n Cause is: "+e.getCause(),0, getCurrentTimeStamp(),allPhrases);
			}
		}
	}

	public static Timestamp getLastTimeAlive() {
 
		Timestamp timestamp = null;
		try {
			String getQueryStatement = "SELECT * FROM system_parameters"; 
			if(conn==null)
				makeJDBCConnection();
			stmt = conn.prepareStatement(getQueryStatement);
			ResultSet rs = stmt.executeQuery();
			// Let's iterate through the java ResultSet
			while (rs.next()) {
				timestamp= rs.getTimestamp("firehose_last_alive");
			}
		} catch (
 
		SQLException e) {
			e.printStackTrace();
		}
		
		return timestamp;
	}
	public static void StopeFirehose()
	{
		try{
			updateSystmeParameters(0, getCurrentTimeStamp(),"Stopped by client manually", 0, getCurrentTimeStamp(), allPhrases);

			if(AllObjects.getClient()!=null)
			{
				AllObjects.getClient().stop();
				AllObjects.setClient(null);
			}
		}
		catch (Exception e) {
			hosebirdClient = null;
			AllObjects.setClient(null);
			updateSystmeParameters(0, getCurrentTimeStamp(),"Stopped by client manually", 0, getCurrentTimeStamp(), allPhrases);

		}
	}
	
	public static int getFirehoseStatus()
	{
		int firehose_status = -1;
		try {
			String getQueryStatement = "SELECT * FROM system_parameters"; 
			if(conn==null)
				makeJDBCConnection();
			stmt = conn.prepareStatement(getQueryStatement);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				firehose_status  = rs.getInt("firehose_status");
			}
		} catch (
 
		SQLException e) {
			e.printStackTrace();
		}
		return firehose_status;
	}
	
	public static int getPhraseListenerStatus()
	{
		int phrase_listener_status=-1;
		try {
			String getQueryStatement = "SELECT * FROM system_parameters"; 
			if(conn==null)
				makeJDBCConnection();
			stmt = conn.prepareStatement(getQueryStatement);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				phrase_listener_status= rs.getInt("phrase_listener_status");
			}
		} catch (
 
		SQLException e) {
			e.printStackTrace();
		}
		return phrase_listener_status;
	}
	
	private static void log(String msg)
	{
		System.out.println(msg);
	}
	
	private static void makeJDBCConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+Constant.dbName+"?useUnicode=true&characterEncoding=UTF-8",Constant.usernName,Constant.password);
			if (conn != null) {
				log("Connection Successful! Enjoy. Now it's time to push data");
			} else {
				log("Failed to make connection!");
			}
		} catch (SQLException e) {
			log("MySQL Connection Failed!");
			e.printStackTrace();
			return;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	private static void updateSystmeParameters(int firehose_status,Timestamp firehose_last_alive, String log, int phrase_listener_status,Timestamp phrase_listener_last_alive, String phrase_listener_log)
	{
		
		try {
			String insertQueryStatement = "UPDATE system_parameters SET firehose_status=?,firehose_last_alive=?,firehose_log=?,phrase_listener_status=?,phrase_listener_last_alive=?,phrase_listener_log=?";
			stmt = conn.prepareStatement(insertQueryStatement);
			stmt.setInt(1, firehose_status);
			stmt.setTimestamp(2, firehose_last_alive);
			stmt.setString(3, log);
			stmt.setInt(4, phrase_listener_status);
			stmt.setTimestamp(5, phrase_listener_last_alive);
			stmt.setString(6, phrase_listener_log);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void insertTweetData(String tweetdetail)
	{
		try{
			log("Before insertoing the tweet in database"+ tweetdetail);
			 Status status = null;
			 try {
			     status = TwitterObjectFactory.createStatus(tweetdetail);
			 } catch (TwitterException e) {
			     e.printStackTrace();
			 }
//			 log("name:"+status.getUser().getName()+" | id:"+status.getUser().getId()+" |getScreenName: "+status.getUser().getScreenName());
			 Long comment_medium_id  = status.getId();
			 String comment = status.getText();
			 String comment_user_id = status.getUser().getScreenName();
			 String created_at = status.getCreatedAt().toString();
			 String lang = status.getLang();
			 int favorite_count = status.getFavoriteCount();
			 int retweet_count = status.getRetweetCount();
			 Long in_reply_to_user_id = status.getInReplyToUserId();
			 Long in_reply_to_status_id = status.getInReplyToStatusId();
			 Double centerLatitude, minLatitude=90.000, maxLatitude=-90.000;
			 Double centerLongitude, minLongitude=180.000, maxLongitude=-180.000;
			 int polygonDim =-1;
			 Double lat=-1000.00, lng=-1000.00;
			 String city =null, country =null;		 
			 if(status.getPlace()!=null)
			 {
				 GeoLocation[][] box =status.getPlace().getBoundingBoxCoordinates();
				 polygonDim= box.length;				
				 for(int i=0;i<polygonDim;i++)
				 {
					 for(int j=0;j<box[i].length;j++)
					 {
						 if(box[i][j].getLongitude()<minLongitude)
							 minLongitude = box[i][j].getLongitude();
						 if(box[i][j].getLongitude()>maxLongitude)
							maxLongitude = box[i][j].getLongitude();
						 
						 if(box[i][j].getLatitude()<minLatitude)
							 minLatitude = box[i][j].getLatitude();
						 if(box[i][j].getLatitude() > maxLatitude)
							 maxLatitude = box[i][j].getLatitude();
					 }
				 }
				 if(polygonDim>0)
				 {
				 centerLatitude = (minLatitude+ maxLatitude)/2;
				 centerLongitude = (minLongitude+maxLongitude)/2;
				 }
				 else
				 {
					 centerLatitude = null;
					 centerLongitude=  null;
					 
				 }
				 lat = centerLatitude;
				 lng = centerLongitude;
				 country = status.getPlace().getCountry();
				 city = status.getPlace().getFullName();
			 }
			 
			 
			 UserMentionEntity[] mentionsArray = status.getUserMentionEntities();
			 String mentions ="";
			 for(int i=0;i<mentionsArray.length;i++)
			 {
				 mentions+=mentionsArray[i].getScreenName();
				 if(i!=mentionsArray.length-1)
					 mentions+=",";
			 }
			 
			 HashtagEntity[] hashTagArray = status.getHashtagEntities();
			 String hashtags ="";
			 for(int i=0;i<hashTagArray.length;i++)
			 {
				 hashtags+=hashTagArray[i].getText();
				 if(i!=hashTagArray.length-1)
					 hashtags+=",";
			 }
			 
			 int medium_id = 1;
			 int client_id = 1;
			 
			try {
				String insertQueryStatement = "INSERT  INTO  comments  VALUES  (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				stmt = conn.prepareStatement(insertQueryStatement);
				stmt.setString(1,null);
				stmt.setString(2, Long.toString(comment_medium_id ));
				stmt.setString(3, comment);
				stmt.setString(4, comment_user_id);
				stmt.setString(5, lang);
				stmt.setString(6, created_at);
				stmt.setInt(7, favorite_count);
				stmt.setInt(8,retweet_count );
				stmt.setString(9, Long.toString(in_reply_to_user_id));
				stmt.setString(10, Long.toString(in_reply_to_status_id));
				stmt.setString(11, Double.toString(lat));
				stmt.setString(12, Double.toString(lng));
				stmt.setString(13,  country);
				stmt.setString(14, city);
				stmt.setString(15, mentions);
				stmt.setString(16, hashtags);
				stmt.setInt(17, medium_id);
				stmt.setInt(18, client_id);
				stmt.setTimestamp(19,getCurrentTimeStamp());
				stmt.executeUpdate();
				log("After insertoing the tweet in database"+ tweetdetail);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}
	
	
}
