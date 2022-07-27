package server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import klassen.Arbeitszeit;
import klassen.ArbeitszeitList;
import klassen.Auftraggeber;
import klassen.AuftraggeberList;
import klassen.Meldung;
import klassen.Mitarbeiter;
import klassen.MitarbeiterList;
import klassen.Projekt;
import klassen.ProjektList;

public class Handler implements HttpHandler {

	static {

		DatenbankReturn dr = Datenbank.createTableMitarbeiter();
		if(dr.isRc()) {
			System.out.println(dr.getMeldung());
		}
		DatenbankReturn drAg = Datenbank.createTableAuftraggeber();
		if(drAg.isRc()) {
			System.out.println(drAg.getMeldung());
		}
		DatenbankReturn drPr = Datenbank.createTableProjekt();
		if(drPr.isRc()) {
			System.out.println(drPr.getMeldung());
		}
		DatenbankReturn drAz = Datenbank.createTableArbeitszeit();
		if(drAz.isRc()) {
			System.out.println(drAz.getMeldung());
		}
	}


	@Override
	public void handle(HttpExchange exchange) throws IOException {
		// HTTP Methode ermitteln 
		String method = exchange.getRequestMethod();
		// URI ermitteln
		URI uri = exchange.getRequestURI();
		// Path Angabe aus der URI ermitteln
		String path = uri.getPath();
		if(path.startsWith("/")) {
			path = path.substring(1);

		}
		System.out.println(path + " " + method);
		String[] paths = path.split("/");
		if(method.equalsIgnoreCase("GET")) {
			get(exchange, paths);
		}
		else if(method.equalsIgnoreCase("POST")) {
			post(exchange, paths);
		}
		else if(method.equalsIgnoreCase("DELETE")) {
			delete(exchange, paths);
		}
		else if(method.equalsIgnoreCase("PUT")) {
			put(exchange, paths);
		}
		else {
			setResponse(exchange, 400, new Meldung("Falsche HTTP Methode " + method).toXML());
		}

	}

	/*
	 *    /mitarbeiter
	 *    
	 *    /auftraggeber
	 *    
	 *    /projekt
	 *    
	 *    /arbeitszeit
	 */
	private void post(HttpExchange exchange, String[] paths) {
		int statusCode = 201;
		String response = "";
		if(paths.length == 2 && paths[0].equals("mitarbeiter")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Mitarbeiter in xml String deserialisieren
				Mitarbeiter ma = new Mitarbeiter(xmlLine);
				DatenbankReturn dr = Datenbank.insertMitarbeiter(ma);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("auftraggeber")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Auftraggeber in xml String deserialisieren
				Auftraggeber ag = new Auftraggeber(xmlLine);
				DatenbankReturn dr = Datenbank.insertAuftraggeber(ag);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("projekt")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Projekt in xml String deserialisieren
				Projekt pr = new Projekt(xmlLine);
				DatenbankReturn dr = Datenbank.insertProjekt(pr);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("arbeitszeit")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				System.out.println(xmlLine);
				//AZ in xml String deserialisieren
				Arbeitszeit az = new Arbeitszeit(xmlLine);
				DatenbankReturn dr = Datenbank.insertArbeitszeit(az);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else {
			statusCode = 400;
			response = new Meldung("Falsche URI").toXML();
		}
		setResponse(exchange, statusCode, response);

	}

	/*
	 *   /mitarbeiterlist
	 *   
	 *   /auftraggeberlist
	 *   
	 *   /projektlist
	 *   
	 *   /arbeitszeitlist
	 */
	private void get(HttpExchange exchange, String[] paths) {
		int statusCode = 200;
		String response = "";
		if(paths.length == 1 && paths[0].equals("mitarbeiterlist")) {
			DatenbankReturnData<MitarbeiterList> dr = Datenbank.leseMitarbeiter();
			if(dr.isRc()) {
				//MitarbeierList in die XML Darstellung verwandeln
				response = dr.getData().toXML();
			}
			else {
				// Fehler, daher Statuscode ändern und Exception Text mit einer Meldung in XML Darstellung verwandeln
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		else if(paths.length == 1 && paths[0].equals("auftraggeberlist")) {
			DatenbankReturnData<AuftraggeberList> dr = Datenbank.leseAuftraggeber();
			if(dr.isRc()) {
				//AuftraggeberList in die XML Darstellung verwandeln
				response = dr.getData().toXML();
			}
			else {
				// Fehler, daher Statuscode ändern und Exception Text mit einer Meldung in XML Darstellung verwandeln
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		else if(paths.length == 1 && paths[0].equals("projektlist")) {
			DatenbankReturnData<ProjektList> dr = Datenbank.leseProjekt();
			if(dr.isRc()) {
				//ProjektList in die XML Darstellung verwandeln
				response = dr.getData().toXML();
			}
			else {
				// Fehler, daher Statuscode ändern und Exception Text mit einer Meldung in XML Darstellung verwandeln
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		else if(paths.length == 1 && paths[0].equals("arbeitszeitlist")) {
			DatenbankReturnData<ArbeitszeitList> dr = Datenbank.leseArbeitszeit();
			if(dr.isRc()) {
				//ArbeitszeitList in die XML Darstellung verwandeln
				response = dr.getData().toXML();
			}
			else {
				// Fehler, daher Statuscode ändern und Exception Text mit einer Meldung in XML Darstellung verwandeln
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		else {
			statusCode = 400;
			response = new Meldung("Falsche URI").toXML();
		}
		setResponse(exchange, statusCode, response);

	}

	/*
	 *    /mitarbeiter/id
	 *    
	 *    /auftraggeber/id
	 *    
	 *    /projekt/id
	 *    
	 *    /arbeitszeit/id
	 */
	private void delete(HttpExchange exchange, String[] paths) {
		int statusCode = 204;
		String response = "";
		if(paths.length == 2 && paths[0].equals("mitarbeiter")) {

			DatenbankReturn dr = Datenbank.deleteMitarbeiter(Integer.parseInt(paths[1]));
			if(!dr.isRc()) {
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}

		}
		else if(paths.length == 2 && paths[0].equals("auftraggeber")) {

			DatenbankReturn dr = Datenbank.deleteAuftraggeber(Integer.parseInt(paths[1]));
			if(!dr.isRc()) {
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}

		}
		else if(paths.length == 2 && paths[0].equals("projekt")) {

			DatenbankReturn dr = Datenbank.deleteProjekt(Integer.parseInt(paths[1]));
			if(!dr.isRc()) {
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		else if(paths.length == 2 && paths[0].equals("arbeitszeit")) {

			DatenbankReturn dr = Datenbank.deleteArbeitszeit(Integer.parseInt(paths[1]));
			if(!dr.isRc()) {
				statusCode = 500;
				response = new Meldung(dr.getMeldung()).toXML();
			}
		}
		
		else {
			statusCode = 400;
			response = new Meldung("Falsche URI").toXML();
		}
		setResponse(exchange, statusCode, response);

	}

	private void put(HttpExchange exchange, String[] paths) {
		int statusCode = 201;
		String response = "";
		if(paths.length == 2 && paths[0].equals("mitarbeiter")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Mitarbeiter in xml String deserialisieren
				Mitarbeiter ma = new Mitarbeiter(xmlLine);
				DatenbankReturn dr = Datenbank.updateMitarbeiter(ma);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("auftraggeber")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Auftraggeber in xml String deserialisieren
				Auftraggeber ag = new Auftraggeber(xmlLine);
				DatenbankReturn dr = Datenbank.updateAuftraggeber(ag);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("projekt")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Auftraggeber in xml String deserialisieren
				Projekt pr = new Projekt(xmlLine);
				DatenbankReturn dr = Datenbank.updateProjekt(pr);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else if(paths.length == 2 && paths[0].equals("arbeitszeit")) {
			// Zugriff auf request body hersellen
			InputStream is = exchange.getRequestBody();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			try {
				//XML String aus request body lesen
				String xmlLine = br.readLine();
				//Auftraggeber in xml String deserialisieren
				Arbeitszeit az = new Arbeitszeit(xmlLine);
				DatenbankReturn dr = Datenbank.updateArbeitszeit(az);
				if(!dr.isRc()) {
					statusCode = 500;
					response = new Meldung(dr.getMeldung()).toXML();
				}
			} catch (IOException e) {
				statusCode = 500;
				response = new Meldung(e.toString()).toXML();

			}
		}
		else {
			statusCode = 400;
			response = new Meldung("Falsche URI").toXML();
		}
		setResponse(exchange, statusCode, response);

	}


	private void setResponse(HttpExchange exchange, int statusCode, String response) {
		System.out.println("\t " + statusCode + ", '" + response + "'");
		Headers responseHeaders = exchange.getResponseHeaders();
		responseHeaders.set("Content-Type", "text/plain");
		try {
			exchange.sendResponseHeaders(statusCode, statusCode != 204 ? response.length() : -1);
			OutputStream os = exchange.getResponseBody();
			if(statusCode != 204) {
				os.write(response.getBytes());
			}
			os.close();
		}
		catch (IOException e) { }	
	}



}
