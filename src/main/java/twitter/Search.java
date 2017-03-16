package twitter;

import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public class Search {
	
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/search_phrases")
	public Response get(@QueryParam("phrase") List<String> phrases) {
		
		String allPhrases ="";
		for(int i=0;i<phrases.size();i++)
		{
			allPhrases+=phrases.get(i).replaceAll("_", " ");
			if(i<phrases.size()-1 )
				 allPhrases+=", ";
			System.out.println(phrases.get(i));
		}
		System.out.println(allPhrases);
		String outputStope = "<h1>Social Media Intelligence <h1> <br>" +
				"<p>Firehose Service is already running for another phrase<br>"+"</p><br>"+" <p>Please stop current firehose before starting another search!<br>"+"</p<br>";
		String outputStartRunning = "<h1>Social Media Intelligence <h1>" +
				"<p>Firehose Service is started and consuming data for phrases"+ allPhrases+ " <br>Ping @ " + new Date().toString() + "</p<br>";	  
		String finalResponse =(Firehose.getFirehoseStatus()==1 || Firehose.getPhraseListenerStatus()==1)?outputStope:outputStartRunning;
		
		if(Firehose.getFirehoseStatus()==1 || Firehose.getPhraseListenerStatus()==1)
			return Response.status(200).entity(finalResponse.toString()).build();
		if(Firehose.getFirehoseStatus()==0 && Firehose.getPhraseListenerStatus()==0)
			Firehose.startFirehose(phrases,0,1);
	
		return  Response.status(200).entity(finalResponse.toString()).build();
	} 
	
}
