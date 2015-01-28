import java.sql.*;


public class Connecter {
	static Connection conn = null;// this should be global
	public static void main(String[] args) {

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception ex) {
			System.out.println(ex);
		}
		Statement stmt = null;
		ResultSet rs = null;
		String database = "sample";
		String user = "zhengb";
		String password = "1113";
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost/" + 
				database + "?user=" + 
				user + "&password=" + password);
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select * from products");
			while (rs.next()) {
				String name = rs.getString("prodId");
				String color = rs.getString("manufacturer");
				String ani = rs.getString("model");
				System.out.println(name+" "+color+" "+ani);

//				String table = rs.getString("tables");
//				System.out.println(table);
			}
	
		//	if (stmt.excute(query)) rs = stmt.getResultSet();

		} catch (SQLException ex) {
			System.out.println(ex);
	
		} 
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException rsex) {
					System.out.println(rsex);
				}
			}
			rs = null;
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException stex) {

					System.out.println(stex);
				}

			}
			stmt = null;
			try {
				conn.close();
			} catch (Exception ex) {
				System.out.println(ex);
			}
		}
	}
}
