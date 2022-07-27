package server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;


public class Server {
	
	public static void main(String[] args) {
		//IP Adresse und Port des Servers festlegen
		try {
			InetAddress inet = InetAddress.getByName("localhost");
			InetSocketAddress addr = new InetSocketAddress(inet, 8080);
			//HttpServer erzeugen
			HttpServer server = HttpServer.create(addr, 0);
			//Festlegen, welches Objekt die Client Anfragen bearbeitet
			server.createContext("/", new Handler());
			//Threadpool anlegen
			server.setExecutor(Executors.newCachedThreadPool());
			//Server starten
			server.start();
			System.out.println("Zum Beenden des Abschlussprojekt Server Eingabetaste drücken");
			System.in.read();
			server.stop(0);
			((ExecutorService)server.getExecutor()).shutdown();
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		

	}
}
