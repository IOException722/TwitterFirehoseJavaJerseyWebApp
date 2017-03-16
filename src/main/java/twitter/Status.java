package twitter;

import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
@Path("status")
public class Status {
	
	@GET
	@Produces("text/html")
	public Response getFirehoseStatus()
	{
		
		String outputStoped = "<h1>Social Media Intelligence <h1>" +
				"<p>Firehose Service is not running ... <br>"+"</p<br>"+" <p>Firehose last alive was at:<br>"+Firehose.getLastTimeAlive()+"</p<br>";
		
		String outputRunning = "<h1>Social Media Intelligence <h1>" +
				"<p>Firehose Service is running ....! <br>Ping @ " + new Date().toString() + "</p<br>";
		String finalOutput =(Firehose.getFirehoseStatus()==1 || Firehose.getPhraseListenerStatus()==1)?outputRunning:outputStoped;
		return Response.status(200).entity(finalOutput.toString()).build();
	}
	
	

}
