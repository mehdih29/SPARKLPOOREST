package com.arismore.poste.util;

/*import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;*/

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Singleton {

	Gson gson;
	//Client client;

	private Singleton() {
		this.gson = new GsonBuilder().create();
		//this.client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
	}

	public Gson getGson() {
		return gson;
	}

	/*public Client getClient() {
		return client;
	}*/

	/** Holder */
	private static class SingletonHolder {
		/** Instance unique non préinitialisée */
		private final static Singleton instance = new Singleton();
	}

	/** Point d'accès pour l'instance unique du singleton */
	public static Singleton getInstance() {
		return SingletonHolder.instance;
	}
}