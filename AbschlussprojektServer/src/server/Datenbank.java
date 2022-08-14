package server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;

import klassen.Arbeitszeit;
import klassen.ArbeitszeitList;
import klassen.Auftraggeber;
import klassen.AuftraggeberList;
import klassen.Mitarbeiter;
import klassen.MitarbeiterList;
import klassen.Projekt;
import klassen.ProjektList;


public class Datenbank {
	private static final String DBLOCATION = "C:\\Evi\\JavaWifi\\Abschlussprojekt\\Datenbank";
	private static final String CONNSTRING = "jdbc:derby:" + DBLOCATION + ";create=true";

	private static final String MITARBEITER = "Mitarbeiter";
	private static final String ID = "Id";
	private static final String NAME = "Name";
	private static final String ADRESSE = "Adresse";
	private static final String GEBURTSDAT = "Geburtsdat";
	private static final String SVNUMMER = "SVNummer";
	private static final String TELEFON = "Telefon";
	private static final String EMAIL = "Email";
	private static final String WOCHENARBEITSZEIT = "Wochenarbeitszeit";
	private static final String STUNDENSATZ = "Stundensatz";
	
	private static final String AUFTRAGGEBER = "Auftraggeber";
	private static final String AUFTRAGGEBERID = "AuftraggeberId";
	private static final String AUFTRAGGEBERNAME = "AuftraggeberName";
	private static final String AUFTRAGGEBERADRESSE = "AuftraggeberAdresse";
	private static final String AUFTRAGGEBERTELEFON = "AuftraggeberTelefon";
	private static final String AUFTRAGGEBEREMAIL = "AuftraggeberEmail";
	
	private static final String PROJEKT = "Projekt";
	private static final String PROJEKTID = "ProjektId";
	private static final String PROJEKTNAME = "ProjektName";
	private static final String PROJEKTADRESSE = "ProjektAdresse";
	private static final String PROJEKTTELEFON = "ProjektTelefon";
	private static final String PROJEKTKONTAKTPERSON = "ProjektKontaktperson";
	private static final String PROJEKTAUFTRAGGEBERID = "ProjektAuftraggeberId";
	private static final String ABGESCHLOSSEN = "Abgeschlossen";
	
	private static final String ARBEITSZEIT = "Arbeitszeit";
	private static final String ZEILENNUMMER = "Zeilennummer";
	private static final String ARBEITSZEITDATUM = "ArbeitszeitDatum";
	private static final String ARBEITSZEITMITARBEITERID = "ArbeitszeitMitarbeiterId";
	private static final String ARBEITSZEITPROJEKTID = "ArbeitszeitProjektId";
	private static final String ARBEITSZEITVON = "ArbeitszeitVon";
	private static final String ARBEITSZEITBIS = "ArbeitszeitBis";
	private static final String STUNDENGESAMT = "Stundengesamt";
	private static final String ARBEITSZEITSTUNDENSATZ = "ArbeitszeitStundensatz";
	private static final String FAKTURIERT = "Fakturiert";
	

	//MITARBEITER
	
	public static DatenbankReturn createTableMitarbeiter() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		DatenbankReturn dr = new DatenbankReturn();

		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//Statement Objekt erzeugen
			stmt = conn.createStatement();
			//Falls Mitarbeiter Tabelle schon vorhanden ist, dann fertig
			rs = conn.getMetaData().getTables(null, null, MITARBEITER.toUpperCase(), new String[] {"TABLE"});
			if(rs.next()) {
				//Methode beenden
				dr.setRc(true);
				return dr;
			}

			//Mitarbeiter Tabelle anlegen
			String ct = "CREATE TABLE " + MITARBEITER + " (" +
					ID + " INTEGER GENERATED ALWAYS AS IDENTITY, " +
					NAME + " VARCHAR(200), " +
					ADRESSE + " VARCHAR(200), " +
					GEBURTSDAT + " DATE, " +
					SVNUMMER + " VARCHAR(200), " +
					TELEFON + " VARCHAR(200), " +
					EMAIL + " VARCHAR(200), " +
					WOCHENARBEITSZEIT + " DOUBLE, " +
					STUNDENSATZ + " DOUBLE, " +
					"PRIMARY KEY(" + ID + ")" +
					")";
			stmt.executeUpdate(ct);
			dr.setRc(true);

		} catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}

		}
		return dr;
	}

	public static DatenbankReturnData <MitarbeiterList> leseMitarbeiter(){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<MitarbeiterList> dr = new DatenbankReturnData<>();
		MitarbeiterList ml = new MitarbeiterList();
		ArrayList<Mitarbeiter> mitarbeiterElemente = new ArrayList<>();
		ml.setMitarbeiterElemente(mitarbeiterElemente);
		dr.setData(ml);
		String select = "SELECT * FROM " + MITARBEITER;

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			//Mitarbeiter Datensätze einlesen, Mitarbeiter Objekte erzeugen und in ArrayList speichern
			rs = stmt.executeQuery();
			while(rs.next()) {
				mitarbeiterElemente.add(new Mitarbeiter(rs.getInt(ID), rs.getString(NAME), rs.getString(ADRESSE), rs.getDate(GEBURTSDAT).toLocalDate(), rs.getString(SVNUMMER),
						rs.getString(TELEFON), rs.getString(EMAIL), rs.getDouble(WOCHENARBEITSZEIT), rs.getDouble(STUNDENSATZ)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}


	public static DatenbankReturn insertMitarbeiter (Mitarbeiter mitarbeiter) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String insert = "INSERT INTO " + MITARBEITER + " (" + NAME + "," + ADRESSE + "," + GEBURTSDAT + "," + SVNUMMER + "," + TELEFON +
				"," + EMAIL + "," + WOCHENARBEITSZEIT + "," + STUNDENSATZ + ") VALUES(?,?,?,?,?,?,?,?)";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(insert);

			//Mitarbeiter Datensätze in die Mitarbeiter Tabelle einfügen

			stmt.setString(1, mitarbeiter.getName());
			stmt.setString(2, mitarbeiter.getAdresse());
			LocalDateTime dt = LocalDateTime.of(mitarbeiter.getGeburtsdat(), LocalTime.of(0, 0, 0, 0));
			java.sql.Date date = new java.sql.Date(dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
			stmt.setDate(3, date);
			stmt.setString(4, mitarbeiter.getSvNummer());
			stmt.setString(5, mitarbeiter.getTelefon());
			stmt.setString(6, mitarbeiter.getEmail());
			stmt.setDouble(7, mitarbeiter.getWochenarbeitszeit());
			stmt.setDouble(8, mitarbeiter.getStundensatz());

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}

	public static DatenbankReturn deleteMitarbeiter(int id) {

		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String delete = "DELETE FROM " + MITARBEITER + " WHERE " + ID + " = ?"; 
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt löschen
			stmt = conn.prepareStatement(delete);
			
			stmt.setInt(1, id);

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}

	public static DatenbankReturn updateMitarbeiter(Mitarbeiter mitarbeiter) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String update = "UPDATE " + MITARBEITER + " SET " +
				NAME + " = ?, " + 
				ADRESSE + " = ?, " + 
				GEBURTSDAT + " = ?, " + 
				SVNUMMER + " = ?, " + 
				TELEFON + " = ?, " + 
				EMAIL + " = ?, " +
				WOCHENARBEITSZEIT + " = ?, " +
				STUNDENSATZ + " = ? WHERE " +
				ID + " = ?";
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt erzeugen
			stmt = conn.prepareStatement(update);
			//Mitarbeiter Datensätze in die Tabelle einfügen

			stmt.setString(1, mitarbeiter.getName());
			stmt.setString(2, mitarbeiter.getAdresse());
			LocalDateTime dt = LocalDateTime.of(mitarbeiter.getGeburtsdat(), LocalTime.of(0, 0, 0, 0));
			java.sql.Date date = new java.sql.Date(dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
			stmt.setDate(3, date);
			stmt.setString(4, mitarbeiter.getSvNummer());
			stmt.setString(5, mitarbeiter.getTelefon());
			stmt.setString(6, mitarbeiter.getEmail());
			stmt.setDouble(7, mitarbeiter.getWochenarbeitszeit());
			stmt.setDouble(8, mitarbeiter.getStundensatz());
			stmt.setInt(9, mitarbeiter.getId());

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;
	}
	
	
	//AUFTRAGGEBER
	
	public static DatenbankReturn createTableAuftraggeber() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		DatenbankReturn dr = new DatenbankReturn();

		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//Statement Objekt erzeugen
			stmt = conn.createStatement();
			//Falls Auftraggeber Tabelle schon vorhanden ist, dann fertig
			rs = conn.getMetaData().getTables(null, null, AUFTRAGGEBER.toUpperCase(), new String[] {"TABLE"});
			if(rs.next()) {
				//Methode beenden
				System.out.println("Tabelle existiert");
				dr.setRc(true);
				return dr;
			}

			//Auftraggeber Tabelle anlegen
			String ct = "CREATE TABLE " + AUFTRAGGEBER + " (" +
					AUFTRAGGEBERID + " INTEGER GENERATED ALWAYS AS IDENTITY, " +
					AUFTRAGGEBERNAME + " VARCHAR(200), " +
					AUFTRAGGEBERADRESSE + " VARCHAR(200), " +
					AUFTRAGGEBERTELEFON + " VARCHAR(200), " +
					AUFTRAGGEBEREMAIL + " VARCHAR(200), " +
					"PRIMARY KEY(" + AUFTRAGGEBERID + ")" +
					")";
			stmt.executeUpdate(ct);
			dr.setRc(true);
			System.out.println("Auftraggeber Tabelle wurde angelegt");


		} catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}

		}
		return dr;
	}

	public static DatenbankReturnData <AuftraggeberList> leseAuftraggeber(){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<AuftraggeberList> dr = new DatenbankReturnData<>();
		AuftraggeberList al = new AuftraggeberList();
		ArrayList<Auftraggeber> auftraggeberElemente = new ArrayList<>();
		al.setAuftraggeberElemente(auftraggeberElemente);
		dr.setData(al);
		String select = "SELECT * FROM " + AUFTRAGGEBER;

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			//Auftraggeber Datensätze einlesen, Auftraggeber Objekte erzeugen und in ArrayList speichern
			rs = stmt.executeQuery();
			while(rs.next()) {
				auftraggeberElemente.add(new Auftraggeber(rs.getInt(AUFTRAGGEBERID), rs.getString(AUFTRAGGEBERNAME), rs.getString(AUFTRAGGEBERADRESSE), 
						rs.getString(AUFTRAGGEBERTELEFON), rs.getString(AUFTRAGGEBEREMAIL)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}


	public static DatenbankReturn insertAuftraggeber (Auftraggeber auftraggeber) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String insert = "INSERT INTO " + AUFTRAGGEBER + " (" + AUFTRAGGEBERNAME + "," + AUFTRAGGEBERADRESSE + "," + AUFTRAGGEBERTELEFON +
				"," + AUFTRAGGEBEREMAIL + ") VALUES(?,?,?,?)";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(insert);

			//Auftraggeer Datensätze in die Auftraggeber Tabelle einfügen

			stmt.setString(1, auftraggeber.getName());
			stmt.setString(2, auftraggeber.getAdresse());
			stmt.setString(3, auftraggeber.getTelefon());
			stmt.setString(4, auftraggeber.getEmail());

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn deleteAuftraggeber(int id) {

		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		
		String delete = "DELETE FROM " + AUFTRAGGEBER + " WHERE " + AUFTRAGGEBERID + " = ?"; 
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt löschen
			stmt = conn.prepareStatement(delete);
			
			stmt.setInt(1, id);

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn updateAuftraggeber(Auftraggeber auftraggeber) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String update = "UPDATE " + AUFTRAGGEBER + " SET " +
				AUFTRAGGEBERNAME + " = ?, " + 
				AUFTRAGGEBERADRESSE + " = ?, " + 
				AUFTRAGGEBERTELEFON + " = ?, " + 
				AUFTRAGGEBEREMAIL + " = ? WHERE " +
				AUFTRAGGEBERID + " = ?";
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt erzeugen
			stmt = conn.prepareStatement(update);
			//Mitarbeiter Datensätze in die Wein Tabelle einfügen

			stmt.setString(1, auftraggeber.getName());
			stmt.setString(2, auftraggeber.getAdresse());
			stmt.setString(3, auftraggeber.getTelefon());
			stmt.setString(4, auftraggeber.getEmail());
			stmt.setInt(5, auftraggeber.getId());

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;
	}
	


	//PROJEKT
	
	public static DatenbankReturn createTableProjekt() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		DatenbankReturn dr = new DatenbankReturn();

		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//Statement Objekt erzeugen
			stmt = conn.createStatement();
			//Falls Projekt Tabelle schon vorhanden ist, dann fertig
			rs = conn.getMetaData().getTables(null, null, PROJEKT.toUpperCase(), new String[] {"TABLE"});
			if(rs.next()) {
				//Methode beenden
				System.out.println("Projekt Tabelle existiert");
				dr.setRc(true);
				return dr;
			}

			//Projekt Tabelle anlegen
			String ct = "CREATE TABLE " + PROJEKT + " (" +
					PROJEKTID + " INTEGER GENERATED ALWAYS AS IDENTITY, " +
					PROJEKTNAME + " VARCHAR(200), " +
					PROJEKTADRESSE + " VARCHAR(200), " +
					PROJEKTTELEFON + " VARCHAR(200), " +
					PROJEKTKONTAKTPERSON + " VARCHAR(200), " +
					PROJEKTAUFTRAGGEBERID + " INTEGER, " +
					ABGESCHLOSSEN + " BOOLEAN, " +
					"PRIMARY KEY(" + PROJEKTID + "), " +
					"FOREIGN KEY(" + PROJEKTAUFTRAGGEBERID + ") REFERENCES " + AUFTRAGGEBER + " (" + AUFTRAGGEBERID + ")" +
					")";
			stmt.executeUpdate(ct);
			dr.setRc(true);
			System.out.println("Projekt Tabelle wurde angelegt");


		} catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}

		}
		return dr;
	}

	public static DatenbankReturnData <ProjektList> leseProjekt(){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<ProjektList> dr = new DatenbankReturnData<>();
		ProjektList pl = new ProjektList();
		ArrayList<Projekt> projekte = new ArrayList<>();
		pl.setProjekte(projekte);
		dr.setData(pl);
		String select = "SELECT * FROM " + PROJEKT + " INNER JOIN " + AUFTRAGGEBER + " ON " + 
		PROJEKT + "." + PROJEKTAUFTRAGGEBERID + "=" + AUFTRAGGEBER + "." + AUFTRAGGEBERID;

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			//Projekt Datensätze einlesen, Projekt Objekte erzeugen und in ArrayList speichern
			rs = stmt.executeQuery();
			while(rs.next()) {
				projekte.add(new Projekt(rs.getInt(PROJEKTID), new Auftraggeber(rs.getInt(AUFTRAGGEBERID), rs.getString(AUFTRAGGEBERNAME), rs.getString(AUFTRAGGEBERADRESSE), rs.getString(AUFTRAGGEBERTELEFON), rs.getString(AUFTRAGGEBEREMAIL)),  rs.getString(PROJEKTNAME), rs.getString(PROJEKTADRESSE), 
						rs.getString(PROJEKTTELEFON), rs.getString(PROJEKTKONTAKTPERSON), rs.getBoolean(ABGESCHLOSSEN)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}


	public static DatenbankReturn insertProjekt (Projekt projekt) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String insert = "INSERT INTO " + PROJEKT + " (" + 
		PROJEKTNAME + "," + 
		PROJEKTADRESSE + "," + 
		PROJEKTTELEFON + "," + 
		PROJEKTKONTAKTPERSON + "," + 
		PROJEKTAUFTRAGGEBERID + "," + 
		ABGESCHLOSSEN + ") VALUES(?,?,?,?,?,?)";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(insert);

			//Projekt Datensätze in die Projekt Tabelle einfügen

			stmt.setString(1, projekt.getName());
			stmt.setString(2, projekt.getAdresse());
			stmt.setString(3, projekt.getTelefon());
			stmt.setString(4, projekt.getKontaktperson());
			stmt.setInt(5, projekt.getAuftraggeber().getId());
			stmt.setBoolean(6, projekt.isAbgeschlossen());

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn deleteProjekt(int id) {

		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		
		String delete = "DELETE FROM " + PROJEKT + " WHERE " + PROJEKTID + " = ?"; 
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt löschen
			stmt = conn.prepareStatement(delete);
			
			stmt.setInt(1, id);

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn updateProjekt(Projekt projekt) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String update = "UPDATE " + PROJEKT + " SET " +
				PROJEKTNAME + " = ?, " + 
				PROJEKTADRESSE + " = ?, " + 
				PROJEKTTELEFON + " = ?, " + 
				PROJEKTKONTAKTPERSON + " = ?, " +
				PROJEKTAUFTRAGGEBERID + " = ?, " +
				ABGESCHLOSSEN + " = ? WHERE " +
				PROJEKTID + " = ?";
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt erzeugen
			stmt = conn.prepareStatement(update);
			//Projekt Datensätze in die Projekt Tabelle einfügen

			stmt.setString(1, projekt.getName());
			stmt.setString(2, projekt.getAdresse());
			stmt.setString(3, projekt.getTelefon());
			stmt.setString(4, projekt.getKontaktperson());
			stmt.setInt(5, projekt.getAuftraggeber().getId());
			stmt.setBoolean(6, projekt.isAbgeschlossen());
			stmt.setInt(7, projekt.getId());
			
			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;
	}
	
	
	
	//ARBEITSZEIT
	
	
	public static DatenbankReturn createTableArbeitszeit() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		DatenbankReturn dr = new DatenbankReturn();

		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//Statement Objekt erzeugen
			stmt = conn.createStatement();
			//Falls Arbeitszeit Tabelle schon vorhanden ist, dann fertig
			rs = conn.getMetaData().getTables(null, null, ARBEITSZEIT.toUpperCase(), new String[] {"TABLE"});
			if(rs.next()) {
				//Methode beenden
				System.out.println("Arbeitszeit Tabelle existiert");
				dr.setRc(true);
				return dr;
			}

			//Arbeitszeit Tabelle anlegen
			String ct = "CREATE TABLE " + ARBEITSZEIT + " (" +
					ZEILENNUMMER + " INTEGER GENERATED ALWAYS AS IDENTITY, " +
					ARBEITSZEITDATUM  + " DATE, " +
					ARBEITSZEITMITARBEITERID  + " INTEGER, " +
					ARBEITSZEITPROJEKTID  + " INTEGER, " +
					ARBEITSZEITVON  + " VARCHAR(200), " +
					ARBEITSZEITBIS  + " VARCHAR(200), " +
					STUNDENGESAMT  + " DOUBLE, " +
					ARBEITSZEITSTUNDENSATZ + " DOUBLE, " +
					FAKTURIERT + " BOOLEAN, " +
					"PRIMARY KEY(" + ZEILENNUMMER + "), " +
					"FOREIGN KEY(" + ARBEITSZEITMITARBEITERID + ") REFERENCES " + MITARBEITER + " (" + ID + "), " +
					"FOREIGN KEY(" + ARBEITSZEITPROJEKTID + ") REFERENCES " + PROJEKT + " (" + PROJEKTID + ")" +
					")";
			stmt.executeUpdate(ct);
			dr.setRc(true);
			System.out.println("Arbeitszeit Tabelle wurde angelegt");


		} catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}

		}
		return dr;
	}

	public static DatenbankReturnData <ArbeitszeitList> leseArbeitszeit(){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<ArbeitszeitList> dr = new DatenbankReturnData<>();
		ArbeitszeitList al = new ArbeitszeitList();
		ArrayList<Arbeitszeit> arbeitszeiten = new ArrayList<>();
		al.setArbeitszeiten(arbeitszeiten);
		dr.setData(al);
		String select = "SELECT * FROM " + " (((" + ARBEITSZEIT + " INNER JOIN " + MITARBEITER + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITMITARBEITERID + "=" + MITARBEITER + "." + ID + ")" +
		" INNER JOIN " + PROJEKT + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITPROJEKTID + "=" + PROJEKT + "." + PROJEKTID + ")" +
		" INNER JOIN " + AUFTRAGGEBER + " ON " + PROJEKT + "." + PROJEKTAUFTRAGGEBERID + "=" + 
		AUFTRAGGEBER + "." + AUFTRAGGEBERID + ")";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			//Arbeitszeit Datensätze einlesen, Arbeitszeit Objekte erzeugen und in ArrayList speichern
			rs = stmt.executeQuery();
			while(rs.next()) {
				arbeitszeiten.add(new Arbeitszeit(rs.getInt(ZEILENNUMMER), rs.getDate(ARBEITSZEITDATUM).toLocalDate(), 
						new Mitarbeiter(rs.getInt(ID), rs.getString(NAME), rs.getString(ADRESSE), rs.getDate(GEBURTSDAT).toLocalDate(), rs.getString(SVNUMMER),
								rs.getString(TELEFON), rs.getString(EMAIL), rs.getDouble(WOCHENARBEITSZEIT), rs.getDouble(STUNDENSATZ)), 
						new Projekt(rs.getInt(PROJEKTID), new Auftraggeber(rs.getInt(AUFTRAGGEBERID), rs.getString(AUFTRAGGEBERNAME), rs.getString(AUFTRAGGEBERADRESSE), rs.getString(AUFTRAGGEBERTELEFON), rs.getString(AUFTRAGGEBEREMAIL)),  rs.getString(PROJEKTNAME), rs.getString(PROJEKTADRESSE), 
						rs.getString(PROJEKTTELEFON), rs.getString(PROJEKTKONTAKTPERSON), rs.getBoolean(ABGESCHLOSSEN)),
						rs.getString(ARBEITSZEITVON), rs.getString(ARBEITSZEITBIS), rs.getDouble(STUNDENGESAMT), rs.getDouble(ARBEITSZEITSTUNDENSATZ), rs.getBoolean(FAKTURIERT)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}


	public static DatenbankReturn insertArbeitszeit (Arbeitszeit arbeitszeit) {
		
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String insert = "INSERT INTO " + ARBEITSZEIT + " (" + 
		ARBEITSZEITDATUM + "," + 
		ARBEITSZEITMITARBEITERID + "," + 
		ARBEITSZEITPROJEKTID + "," + 
		ARBEITSZEITVON + "," + 
		ARBEITSZEITBIS + "," + 
		STUNDENGESAMT + "," +
		ARBEITSZEITSTUNDENSATZ + "," + 
		FAKTURIERT + ") VALUES(?,?,?,?,?,?,?,?)";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(insert);

			//Arbeitszeit Datensätze in die Arbeitszeit Tabelle einfügen
			LocalDateTime dt = LocalDateTime.of(arbeitszeit.getDatum(), LocalTime.of(0, 0, 0, 0));
			java.sql.Date date = new java.sql.Date(dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
			stmt.setDate(1, date);	
			stmt.setInt(2, arbeitszeit.getMitarbeiter().getId());
			stmt.setInt(3, arbeitszeit.getProjekt().getId());
			stmt.setString(4, arbeitszeit.getVon());
			stmt.setString(5, arbeitszeit.getBis());
			stmt.setDouble(6, arbeitszeit.getStundengesamt());
			stmt.setDouble(7, arbeitszeit.getStundensatz());
			stmt.setBoolean(8, arbeitszeit.isFakturiert());
			

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} 
			catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn deleteArbeitszeit(int id) {

		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		
		String delete = "DELETE FROM " + ARBEITSZEIT + " WHERE " + ZEILENNUMMER + " = ?"; 
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt löschen
			stmt = conn.prepareStatement(delete);
			
			stmt.setInt(1, id);

			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	public static DatenbankReturn updateArbeitszeit(Arbeitszeit arbeitszeit) {
		DatenbankReturn dr = new DatenbankReturn();
		Connection conn = null;
		PreparedStatement stmt = null;
		String update = "UPDATE " + ARBEITSZEIT + " SET " +
				ARBEITSZEITDATUM + " = ?, " + 
				ARBEITSZEITMITARBEITERID + " = ?, " + 
				ARBEITSZEITPROJEKTID + " = ?, " + 
				ARBEITSZEITVON + " = ?, " +
				ARBEITSZEITBIS + " = ?, " +
				STUNDENGESAMT + " = ?, " +
				ARBEITSZEITSTUNDENSATZ + " = ?, " +
				FAKTURIERT + " = ? WHERE " +
				ZEILENNUMMER + " = ?";
		try {
			//Verbindung zum DB Manager hersellen
			conn = DriverManager.getConnection(CONNSTRING);
			//PreparedStatement Objekt erzeugen
			stmt = conn.prepareStatement(update);
			//Projekt Datensätze in die Projekt Tabelle einfügen
			
			LocalDateTime dt = LocalDateTime.of(arbeitszeit.getDatum(), LocalTime.of(0, 0, 0, 0));
			java.sql.Date date = new java.sql.Date(dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
			stmt.setDate(1, date);
			stmt.setInt(2, arbeitszeit.getMitarbeiter().getId());
			stmt.setInt(3, arbeitszeit.getProjekt().getId());
			stmt.setString(4, arbeitszeit.getVon());
			stmt.setString(5, arbeitszeit.getBis());
			stmt.setDouble(6, arbeitszeit.getStundensatz());
			stmt.setBoolean(7, arbeitszeit.isFakturiert());
			stmt.setInt(8, arbeitszeit.getZeilennummer());

			
			//SQL Kommando ausführen
			stmt.executeUpdate();
			dr.setRc(true);


		}
		catch(SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			}
			catch(SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;
	}
	
	public static DatenbankReturnData <ArbeitszeitList> leseMitarbeiterArbeitszeitInZeitraum(int mitarbeiterId, LocalDate datumVon, LocalDate datumBis){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<ArbeitszeitList> dr = new DatenbankReturnData<>();
		ArbeitszeitList al = new ArbeitszeitList();
		ArrayList<Arbeitszeit> arbeitszeiten = new ArrayList<>();
		al.setArbeitszeiten(arbeitszeiten);
		dr.setData(al);
		String select = "SELECT * FROM " + " (((" + ARBEITSZEIT + " INNER JOIN " + MITARBEITER + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITMITARBEITERID + "=" + MITARBEITER + "." + ID + ")" +
		" INNER JOIN " + PROJEKT + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITPROJEKTID + "=" + PROJEKT + "." + PROJEKTID + ")" +
		" INNER JOIN " + AUFTRAGGEBER + " ON " + PROJEKT + "." + PROJEKTAUFTRAGGEBERID + "=" + 
		AUFTRAGGEBER + "." + AUFTRAGGEBERID + ")" + " WHERE " + MITARBEITER + "." + ID + " =? AND " + ARBEITSZEIT + "." + ARBEITSZEITDATUM +
		" BETWEEN ? AND ? ";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			
			if(mitarbeiterId != 0 && datumVon != null && datumBis != null) {
				stmt.setInt(1, mitarbeiterId);
				LocalDateTime dtVon = LocalDateTime.of(datumVon, LocalTime.of(0, 0, 0, 0));
				java.sql.Date dateVon = new java.sql.Date(dtVon.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
				stmt.setDate(2, dateVon);
				LocalDateTime dtBis = LocalDateTime.of(datumBis, LocalTime.of(0, 0, 0, 0));
				java.sql.Date dateBis = new java.sql.Date(dtBis.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
				stmt.setDate(3, dateBis);
				
			}
			
			//Arbeitszeit Datensätze einlesen, Arbeitszeit Objekte erzeugen und in ArrayList speichern	
			rs = stmt.executeQuery();		
			while(rs.next()) {
				arbeitszeiten.add(new Arbeitszeit(rs.getInt(ZEILENNUMMER), rs.getDate(ARBEITSZEITDATUM).toLocalDate(), 
						new Mitarbeiter(rs.getInt(ID), rs.getString(NAME), rs.getString(ADRESSE), rs.getDate(GEBURTSDAT).toLocalDate(), rs.getString(SVNUMMER),
								rs.getString(TELEFON), rs.getString(EMAIL), rs.getDouble(WOCHENARBEITSZEIT), rs.getDouble(STUNDENSATZ)), 
						new Projekt(rs.getInt(PROJEKTID), new Auftraggeber(rs.getInt(AUFTRAGGEBERID), rs.getString(AUFTRAGGEBERNAME), rs.getString(AUFTRAGGEBERADRESSE), rs.getString(AUFTRAGGEBERTELEFON), rs.getString(AUFTRAGGEBEREMAIL)),  rs.getString(PROJEKTNAME), rs.getString(PROJEKTADRESSE), 
						rs.getString(PROJEKTTELEFON), rs.getString(PROJEKTKONTAKTPERSON), rs.getBoolean(ABGESCHLOSSEN)),
						rs.getString(ARBEITSZEITVON), rs.getString(ARBEITSZEITBIS), rs.getDouble(STUNDENGESAMT), rs.getDouble(ARBEITSZEITSTUNDENSATZ), rs.getBoolean(FAKTURIERT)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
				
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}
	
	
	public static DatenbankReturnData <ArbeitszeitList> leseProjektArbeitszeitInZeitraum(int projektId, LocalDate datumVon, LocalDate datumBis){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		DatenbankReturnData<ArbeitszeitList> dr = new DatenbankReturnData<>();
		ArbeitszeitList al = new ArbeitszeitList();
		ArrayList<Arbeitszeit> arbeitszeiten = new ArrayList<>();
		al.setArbeitszeiten(arbeitszeiten);
		dr.setData(al);
		String select = "SELECT * FROM " + " (((" + ARBEITSZEIT + " INNER JOIN " + MITARBEITER + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITMITARBEITERID + "=" + MITARBEITER + "." + ID + ")" +
		" INNER JOIN " + PROJEKT + " ON " + 
		ARBEITSZEIT + "." + ARBEITSZEITPROJEKTID + "=" + PROJEKT + "." + PROJEKTID + ")" +
		" INNER JOIN " + AUFTRAGGEBER + " ON " + PROJEKT + "." + PROJEKTAUFTRAGGEBERID + "=" + 
		AUFTRAGGEBER + "." + AUFTRAGGEBERID + ")" + " WHERE " + PROJEKT + "." + PROJEKTID + " =? AND " + ARBEITSZEIT + "." + ARBEITSZEITDATUM +
		" BETWEEN ? AND ? ";

		try {
			conn = DriverManager.getConnection(CONNSTRING);
			stmt = conn.prepareStatement(select);
			
			if(projektId != 0 && datumVon != null && datumBis != null) {
				stmt.setInt(1, projektId);
				LocalDateTime dtVon = LocalDateTime.of(datumVon, LocalTime.of(0, 0, 0, 0));
				java.sql.Date dateVon = new java.sql.Date(dtVon.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
				stmt.setDate(2, dateVon);
				LocalDateTime dtBis = LocalDateTime.of(datumBis, LocalTime.of(0, 0, 0, 0));
				java.sql.Date dateBis = new java.sql.Date(dtBis.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
				stmt.setDate(3, dateBis);
				
			}
			
			//Arbeitszeit Datensätze einlesen, Arbeitszeit Objekte erzeugen und in ArrayList speichern	
			rs = stmt.executeQuery();		
			while(rs.next()) {
				arbeitszeiten.add(new Arbeitszeit(rs.getInt(ZEILENNUMMER), rs.getDate(ARBEITSZEITDATUM).toLocalDate(), 
						new Mitarbeiter(rs.getInt(ID), rs.getString(NAME), rs.getString(ADRESSE), rs.getDate(GEBURTSDAT).toLocalDate(), rs.getString(SVNUMMER),
								rs.getString(TELEFON), rs.getString(EMAIL), rs.getDouble(WOCHENARBEITSZEIT), rs.getDouble(STUNDENSATZ)), 
						new Projekt(rs.getInt(PROJEKTID), new Auftraggeber(rs.getInt(AUFTRAGGEBERID), rs.getString(AUFTRAGGEBERNAME), rs.getString(AUFTRAGGEBERADRESSE), rs.getString(AUFTRAGGEBERTELEFON), rs.getString(AUFTRAGGEBEREMAIL)),  rs.getString(PROJEKTNAME), rs.getString(PROJEKTADRESSE), 
						rs.getString(PROJEKTTELEFON), rs.getString(PROJEKTKONTAKTPERSON), rs.getBoolean(ABGESCHLOSSEN)),
						rs.getString(ARBEITSZEITVON), rs.getString(ARBEITSZEITBIS), rs.getDouble(STUNDENGESAMT), rs.getDouble(ARBEITSZEITSTUNDENSATZ), rs.getBoolean(FAKTURIERT)));
			}
			rs.close();
			dr.setRc(true);
		} 
		catch (SQLException e) {
			dr.setMeldung(e.toString());
		}
		finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
				
			} catch (SQLException e) {
				dr.setMeldung(e.toString());
			}
		}
		return dr;

	}




}
