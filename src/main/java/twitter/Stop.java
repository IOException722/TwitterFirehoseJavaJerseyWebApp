package twitter;

import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
@Path("stop")
public class Stop {

	@GET
	@Produces("text/html")
	public Response getFirehoseStatus()
	{
		
		String outputStoped = "<h1>Social Media Intelligence <h1>" +
				"<p>Firehose Service is already stopped ... <br>"+"</p<br>"+" <p>Firehose last alive was at:<br>"+Firehose.getLastTimeAlive()+"</p<br>";
		
		String outputStoping = "<h1>Social Media Intelligence <h1>" +
				"<p>Stoping firehose Service .....! <br>Ping @ " + new Date().toString() + "</p<br>";
		String finalOutput =(Firehose.getFirehoseStatus()==0 && Firehose.getPhraseListenerStatus()==0)?outputStoped:outputStoping;
		
		Firehose.StopeFirehose();
		return Response.status(200).entity(finalOutput.toString()).build();
			
	}

}
