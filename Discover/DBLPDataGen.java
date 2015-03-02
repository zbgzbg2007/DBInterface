//import org.xml.sax.*;
//import org.xml.sax.helpers.XMLReaderFactory;
import java.io.*;
import java.sql.*;
import java.util.*;

public class DBLPDataGen {

	/**
	 * @param args
	 */
public static void main(String[] args) throws IOException{

		//XMLReader parser = XMLReaderFactory.createXMLReader();
	DBLPContentHandler ch = new DBLPContentHandler();
	//parser.setContentHandler(ch);
	String filename = "../../dblp_full/dblp.xml";
	if (args.length > 0) {
		filename = args[0];
	}	
	ch.startDocument();
	for (ch.scan = 0; ch.scan <= 1; ch.scan++) {
		ch.lineNumber = 0;
	    BufferedReader br = null;
	    String thisLine;
	    String qName;
	    HashMap<String, String> atts = new HashMap<String, String>();
	    
	    try {
	      br = new BufferedReader(new FileReader(filename));

	      // dis.available() returns 0 if the file does not have more lines.
	      while ((thisLine = br.readLine()) != null) {
	    	  ch.lineNumber ++;
	    	  if (ch.lineNumber % 1000 == 0) System.out.println(ch.lineNumber);
	      // this statement reads the line from the file and print it to
	        // the console.
	       // System.out.println(thisLine);
	        while (thisLine.length() > 0) {	        	
	        	if (thisLine.charAt(0) == '<' && (thisLine.charAt(1) == '?' || thisLine.charAt(1) == '!')) {
	        		int backPos = thisLine.indexOf(">");	        		
	        		thisLine = thisLine.substring(backPos + 1);
	        	} else if (thisLine.charAt(0) == '<' && thisLine.charAt(1) == '/') {
	        		int backPos = thisLine.indexOf(">");
	        		qName = thisLine.substring(2, backPos);
	        		thisLine = thisLine.substring(backPos + 1);
	        		ch.endElement(qName);
	        	} else if (thisLine.charAt(0) == '<') {	        		
	        		int backPos = thisLine.indexOf(">");
	        		String segment = thisLine.substring(1, backPos);
		        	thisLine = thisLine.substring(backPos + 1);             	
	             	
		        	int spacePos = segment.indexOf(" ");
		        	atts.clear();
		        	if (spacePos < 0) {
		        		qName = segment;
		        	} else {
		        		qName = segment.substring(0, spacePos);
		        		segment = segment.substring(spacePos + 1) + " ";
		        		String attQName = "";
		        		String attValue = "";
		        		int equalPos = 0;
		        		while (segment.length() > 0 && (equalPos = segment.indexOf("=")) > 0) {		        			
		        			spacePos = segment.indexOf(" ", equalPos + 1);
		        			
		        			attQName = segment.substring(0, equalPos).trim();		        			
		        			attValue = segment.substring(equalPos+1, spacePos).trim();
		        			segment = segment.substring(spacePos + 1);
		        			
		        			if (attValue.length() > 1 && attValue.charAt(0) == '\"') {
		        				atts.put(attQName, attValue.substring(1, attValue.length() - 1));
		        			} else {
		        				atts.put(attQName, attValue);
		        			}
		        		}
		        	}
		        	ch.startElement(qName, atts);
	        	} else {
	        		int frontPos = thisLine.indexOf("<");
	        		if (frontPos < 0) frontPos = 1;
	        		if (ch.characterData.length() > 0) {
	        			ch.characterData = ch.characterData + thisLine.substring(0, frontPos);
	        		} else {
	        			ch.characterData = thisLine.substring(0, frontPos);
	        		}
	        		thisLine = thisLine.substring(frontPos);
	        		
	        	}
	        }
	      }
	        // dispose all the resources after using them.
	        br.close();
	      
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		//parser.parse(file);
		
	}
}

}
class DBLPContentHandler {//implements ContentHandler{
//	private Locator locator;
	protected long lineNumber = 0;
	private Connection conn;
	private Statement stmt;

	private String internalKey = "";
	private String type = "";

	private ArrayList<String> authors = new ArrayList<String>();
//	private ArrayList<String> editors;
	private String title;
	private String booktitle;
	private String pages;
	private int year;
	//private String address;
	private String journal;
	private String volume;
	private String number;
	//private String month;
	private String isbn;
	private String url;
	private String ee;
//	private String cdrom;
	
	private String crossref;

	private ArrayList<String> cites = new ArrayList<String>();
	private String publisher;
	private String series;
	//private String note;
	//private String school;
	//private String chapter;
	
	private int publisherNum = 0;
	private int seriesNum = 0;
	private int personNum = 0;
	private int itemNum = 0;
	protected int scan = 0;
	
	private void reset() {
		internalKey = "";
		type = "";
		authors.clear();
		title = "";
		booktitle = "";
		pages = "";
		year = 0;
		journal = "";
		volume = "";
		number = "";
		isbn = "";
		url = "";
		ee = "";
//		cdrom = "";
		crossref = "";
		cites.clear();
		publisher = "";
		series = "";
	}
	protected String characterData;

	//public void setDocumentLocator(Locator _locator) {
	//	locator = _locator;		
	//}
	public void startDocument() {
		try {
			String server = "jdbc:mysql://localhost/dblp_full";
			Class.forName("com.mysql.jdbc.Driver").newInstance();		
			conn = DriverManager.getConnection(server, "root", "");
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
			
			stmt.executeUpdate("DELETE FROM Item");
			stmt.executeUpdate("DELETE FROM Person");
			stmt.executeUpdate("DELETE FROM Publisher");
			stmt.executeUpdate("DELETE FROM Series");
			stmt.executeUpdate("DELETE FROM RelationPersonItem"); 
			stmt.executeUpdate("DELETE FROM Cite");
		} catch (Exception e) {
			System.err.println(e+ "--" + lineNumber);
		}
		reset();
		/*authors = new ArrayList<String>();
		cite = new ArrayList<String>();
		publisherNum = 0;
		seriesNum = 0;*/

	};
	public void endDocument(){
		try {
			conn.close();
		} catch (Exception e) {
			System.err.println(e + "--" + lineNumber);
		}
	};

	/** The opening tag of an element. */
	public void startElement(String qName, HashMap<String, String> atts) {
		if (qName.equals("article") || qName.equals("inproceedings") || qName.equals("proceedings") 
				|| qName.equals("book") || qName.equals("incollection") || qName.equals("phdthesis") 
				|| qName.equals("masterthesis") || qName.equals("www")) {
			type = qName;
			for (String attQName: atts.keySet()) {
				if (attQName.equals("key"))
					internalKey = atts.get(attQName);
			}
			characterData = "";
		}
		if (qName.equals("author") || qName.equals("editor") || qName.equals("title")
				|| qName.equals("booktitle") || qName.equals("pages") || qName.equals("year") 
				|| qName.equals("journal") || qName.equals("volume") || qName.equals("number") 
				|| qName.equals("isbn") || qName.equals("url") || qName.equals("ee") || qName.equals("crossref") 
				|| qName.equals("cite") || qName.equals("publisher") || qName.equals("series")) {
			characterData = "";
		}
	}
	
	private String clean(String characterData) {
		if (characterData.indexOf("'") >= 0) { 
			characterData = characterData.replaceAll("'", "\\\\\'");
		}
		return characterData;
		
	}
	/** The closing tag of an element. */
	public void endElement(String qName) {
		String cleanCharacterData = clean(characterData);
		if (qName.equals("author") || qName.equals("editor")) {
			authors.add(cleanCharacterData);
		} else if (qName.equals("title")) {
			title = cleanCharacterData;
		} else if (qName.equals("booktitle")) {
			booktitle = cleanCharacterData;
		} else if (qName.equals("pages")) {
			pages = cleanCharacterData;
		} else if (qName.equals("year")) {
			year = Integer.valueOf(cleanCharacterData);			
		} else if (qName.equals("journal")) {
			journal = cleanCharacterData;			
		} else if (qName.equals("volume")) {
			volume = cleanCharacterData;			
		} else if (qName.equals("number")) {
			number = cleanCharacterData;		
		} else if (qName.equals("isbn")) {
			isbn = cleanCharacterData;
		} else if (qName.equals("url")) {
			url = cleanCharacterData;			
		} else if (qName.equals("ee")) {
			ee = cleanCharacterData;			
		} else if (qName.equals("crossref")) {
			crossref = cleanCharacterData;
		} else if (qName.equals("cite")) {
			if (characterData.length() > 3) {
				cites.add(cleanCharacterData);
			}
		} else if (qName.equals("publisher")) {
			publisher = cleanCharacterData;
		} else if (qName.equals("series")) {
			series = cleanCharacterData;
		}
		else if (qName.equals("article") || qName.equals("inproceedings") || qName.equals("proceedings") 
				|| qName.equals("book") || qName.equals("incollection") || qName.equals("phdthesis") 
				|| qName.equals("masterthesis") || qName.equals("www")) {
			//StringBuffer SQL = new StringBuffer();
			try {
			if (scan == 0) {
				//publisher
				int publisherId = 0;
				if (publisher != "") {
					ResultSet rs = stmt.executeQuery("SELECT PublisherId FROM Publisher WHERE Name = \'"+publisher+"\'");
					if (rs.next()) {						
						publisherId = rs.getInt(1);
					} else {
						publisherNum ++;
						publisherId = publisherNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Publisher VALUES (").append(publisherId).append(",\'").append(publisher).append("\')");
debug(SQL);	
						stmt.executeUpdate(SQL.substring(0));
					}
				}
				//series
				int seriesId = 0;
				if (series != "") {
					ResultSet rs = stmt.executeQuery("SELECT SeriesId FROM Series WHERE Title = \""+series+"\"");
					if (rs.next()) {						
						seriesId = rs.getInt(1);
					} else {
						seriesNum ++;
						seriesId = seriesNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Series VALUES (").append(seriesId).append(",\'").append(series).append("\')");
debug(SQL);	
						stmt.executeUpdate(SQL.substring(0));						
					}
				}
				//crossreferenced items
				int refItemId = 0;
				if (crossref != "" || type.equals("article") || type.equals("incollection") || type.equals("inproceedings")) {
					String refType = type;
					String refUrl = "";
					if (type.equals("article")) {
						refType = "journal";
						booktitle = journal;
					} else if (type.equals("incollection")) {
						refType = "book";
					} else if (type.equals("inproceedings")) {
						refType = "proceedings";
					}					
					ResultSet rs = null;
					if (url != "") {
						int dashPos = url.indexOf("#");
						if (dashPos > 0) refUrl = url.substring(0, dashPos);
					}
					if (crossref != "" || refUrl != "") {
						StringBuffer SQL = new StringBuffer();
						SQL.append("SELECT ItemId FROM Item WHERE ");
						if (crossref != "") SQL.append("InternalKey=\'").append(crossref).append("\'");
						if (crossref != "" && refUrl != "") SQL.append(" OR ");
						if (refUrl != "") SQL.append("Url=\'").append(refUrl).append("\'");
						rs = stmt.executeQuery(SQL.substring(0));
					}					
					//rs = stmt.executeQuery("SELECT ItemId FROM Item WHERE Url = \'"+refUrl+"\'");
					//if (itemNum == 3374)
						//System.out.println(SQL);
					if (rs != null && rs.next()) {
						refItemId = rs.getInt(1);
					} else {
						itemNum ++;
						refItemId = itemNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Item SET ItemId=").append(refItemId).append(",Type=\'").append(refType).append("\'");
						if (crossref != "") SQL.append(", InternalKey=\'").append(crossref).append("\'");
						if (booktitle != "") SQL.append(", Title=\'").append(booktitle).append("\'");
						if (refUrl != "") SQL.append(", Url=\'").append(refUrl).append("\'");
						if (publisherId != 0) SQL.append(", PublisherId=\'").append(publisherId).append("\'");
debug(SQL);	
						stmt.executeUpdate(SQL.substring(0));						
						
//						stmt.executeUpdate("INSERT INTO Item SET ItemId="+refItemId+",",crossref")
					}
				}
				//item
				int itemId = 0;
				if (true) {
					ResultSet rs = null;
					if (internalKey != "" || url != "") {
						StringBuffer SQL = new StringBuffer();
						SQL.append("SELECT ItemId FROM Item WHERE ");
						if (internalKey != "") SQL.append("InternalKey=\'").append(internalKey).append("\'");
						if (internalKey != "" && url != "") SQL.append(" OR ");
						if (url != "") SQL.append("Url=\'").append(url).append("\'");
						rs = stmt.executeQuery(SQL.substring(0));
					}
//					rs = stmt.executeQuery("SELECT ItemId FROM Item WHERE Url = \'"+url+"\'");
					if (rs != null && rs.next()) {
						itemId = rs.getInt(1);
						StringBuffer SQL = new StringBuffer();
						SQL.append("UPDATE Item SET ").append("Type=\'").append(type).append("\',Title=\'").append(title).append("\'");
						if (internalKey != "") SQL.append(", InternalKey=\'").append(internalKey).append("\'");
						if (pages != "") SQL.append(", Pages=\'").append(pages).append("\'");
						if (year != 0) SQL.append(",Year=").append(year);
						if (volume != "") SQL.append(", Volume=\'").append(volume).append("\'");
						if (number != "") SQL.append(",Number=\'").append(number).append("\'");
						if (url != "") SQL.append(",Url=\'").append(url).append("\'");
						if (ee != "") SQL.append(",EE=\'").append(ee).append("\'");
						if (isbn != "") SQL.append(",ISBN=\'").append(isbn).append("\'");
						if (refItemId != 0) SQL.append(",CrossRef=").append(refItemId);
						if (publisherId != 0) SQL.append(",PublisherId=").append(publisherId);
						if (seriesId != 0) SQL.append(",SeriesId=").append(seriesId);
						SQL.append(" WHERE ItemId = ").append(itemId);
debug(SQL);					
						stmt.executeUpdate(SQL.substring(0));
					} else {
						itemNum ++;
						itemId = itemNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Item SET ItemId=").append(itemId).append(",Type=\'").append(type).append("\',Title=\'").append(title).append("\'");
						if (internalKey != "") SQL.append(", InternalKey=\'").append(internalKey).append("\'");
						if (pages != "") SQL.append(", Pages=\'").append(pages).append("\'");
						if (year != 0) SQL.append(",Year=").append(year);
						if (volume != "") SQL.append(", Volume=\'").append(volume).append("\'");
						if (number != "") SQL.append(",Number=\'").append(number).append("\'");
						if (url != "") SQL.append(",Url=\'").append(url).append("\'");
						if (ee != "") SQL.append(",EE=\'").append(ee).append("\'");
						if (isbn != "") SQL.append(",ISBN=\'").append(isbn).append("\'");
						if (refItemId != 0) SQL.append(",CrossRef=").append(refItemId);
						if (publisherId != 0) SQL.append(",PublisherId=").append(publisherId);
						if (seriesId != 0) SQL.append(",SeriesId=").append(seriesId);						

debug(SQL);				
						stmt.executeUpdate(SQL.substring(0));
					//	SQL.append("INSERT INTO Item VALUES (").append(itemId).append(",\'").append(internalKey)
					//	.append("\',\'").append(type).append("\',\'").append(title).append("\',\'").append(pages)
					//	.append("\',\'").append(volume).append("\',").append(year).append(",\'").append(number)
					//	.append("\',\'").append(url).append("\',\'").append(ee).append("\',\'").append(cdrom)
					//	.append("\',\'").append(isbn).append("\',").append(refItemId).append(",").append(publisherId).append(",").append(seriesId).append(")");
						
					//	stmt.execute(SQL.substring(0));
						
		//				stmt.executeUpdate("INSERT INTO Item VALUES ("+itemId+",\'"+internalKey+"\',\'"+type+"\',\'"+title
		//						+"\',\'"+pages+"\',"+year+","+volume+","+number+",\'"+url+"\',\'"+ee+"\',\'"+cdrom
		//						+"\',\'"+isbn+"\',0,"+publisherId+","+seriesId+")");
						
					}	
				}
				//authors & relationPersonItem
				for (String author: authors) {
					ResultSet rs = stmt.executeQuery("SELECT PersonId FROM Person WHERE Name = \'"+author+"\'");
					int personId;
					if (rs.next()) {
						personId = rs.getInt(1);						
					} else {
						personNum ++;
						personId = personNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Person VALUES (").append(personId)
							.append(",\'").append(author).append("\')");
debug(SQL);	
						stmt.executeUpdate(SQL.substring(0));
					}
					StringBuffer SQL = new StringBuffer();
					SQL.append("INSERT INTO RelationPersonItem VALUES (").append(personId).append(",").append(itemId).append(")");
debug(SQL);	
					stmt.executeUpdate(SQL.substring(0));					
				}				

			}
			if (scan == 1 && cites.size() > 0) {
				int itemId = 0;
				ResultSet rs = null;
				if (internalKey != "" || url != "") {
					StringBuffer SQL = new StringBuffer();
					SQL.append("SELECT ItemId FROM Item WHERE ");
					if (internalKey != "") SQL.append("InternalKey=\'").append(internalKey).append("\'");
					if (internalKey != "" && url != "") SQL.append(" OR ");
					if (url != "") SQL.append("Url=\'").append(url).append("\'");
					rs = stmt.executeQuery(SQL.substring(0));
				}
//				rs = stmt.executeQuery("SELECT ItemId FROM Item WHERE Url = \'"+url+"\'");
				if (rs != null && rs.next()) {
					itemId = rs.getInt(1);
				}
				//cited items & cites
				for (String cite: cites) {
					int citeId = 0;
					rs = stmt.executeQuery("SELECT ItemId FROM Item WHERE InternalKey = \'"+cite+"\'");
					if (rs.next()) {
						citeId = rs.getInt(1);						
					} else {
						itemNum ++;
						citeId = itemNum;
						StringBuffer SQL = new StringBuffer();
						SQL.append("INSERT INTO Item SET ItemId=").append(citeId).append(",InternalKey=\'").append(cite).append("\'");
						stmt.executeUpdate(SQL.substring(0));						
					}
					StringBuffer SQL = new StringBuffer();
					SQL.append("INSERT INTO Cite VALUES (").append(itemId).append(",").append(citeId).append(")");
					stmt.executeUpdate(SQL.substring(0));
				}
			}
			} catch (SQLException e) {
				System.err.println(e + "--" + lineNumber);
			}
			reset();
		}
	}

	/** Character data. 
	public void characters(char[] ch, int start, int length) {
		characterData = characterData + getCharacters(ch, start, length);
	}*/

	/** The start of a namespace scope 
	public void startPrefixMapping(String prefix, String uri) { }
*/
	/** The end of a namespace scope */
	//public void endPrefixMapping(String prefix) { }
	/** Ignorable whitespace character data. */
	//public void ignorableWhitespace(char[] ch, int start, int length) { }

	/** Processing Instruction */
	//public void processingInstruction(String target, String data) { }

	/** A skipped entity. */
	//public void skippedEntity(String name) { }

	/**
	 * Internal method to format arrays of characters so the special whitespace
	 * characters will show.
	 */
//	public String getCharacters(char[] ch, int start, int length) {
//		String sb = "";
//		for (int i = start; i < start + length; i++) {
//			sb = sb + ch[i];
//		}
//		return sb;
//	}
private void debug(StringBuffer SQL) {
	//System.out.println(SQL);
}
}
