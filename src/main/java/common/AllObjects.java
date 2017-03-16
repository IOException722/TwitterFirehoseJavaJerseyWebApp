package common;

import com.twitter.hbc.core.Client;

public class AllObjects {
	private static Client hosebirdClient;
	public static Client getClient()
	{
		return hosebirdClient;
	}
	public static void setClient(Client hosebirdClien)
	{
		hosebirdClient = hosebirdClien;
	}
}
