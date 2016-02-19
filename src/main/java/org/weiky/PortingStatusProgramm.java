package org.weiky;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This programm is used to compare the porting status of a sap hana and an sql server.
 * @author weiky
 *
 */
public class PortingStatusProgramm {

	private static String userNameMSSql;
	private static String passwordMSSql;

	private static String urlsqlserver;
	private static String dbo = "dbo";
	private static String urlhana;
	private static String userNameHana;
	private static String passwordHana;
	public static ExecutorService executor;
	public static int runnercounter = 1;
	private static int maximalrowPerWorker = 50;

	public static void main(String[] args) throws Exception {


		try{
			if(args[0]=="help" ){
				printHelp();
			} else{

				executor = Executors.newFixedThreadPool(10);

				// Credentials
				userNameMSSql = args[0];
				passwordMSSql = args[1];

				userNameHana = args[2];
				passwordHana = args[3];

				urlsqlserver = "jdbc:sqlserver://"+args[4]+";";
				urlhana = "jdbc:sap://"+args[5];


				String outputFile = getCurrentDirectory();
				String outputEncoding = "UTF-8";


				Iterator<String> it;

				ArrayList<String> systemtables = setUpSystemTables();

				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				Connection connmssql = DriverManager.getConnection(urlsqlserver, userNameMSSql, passwordMSSql);
				System.out.println("MS SQL connected ");

				Connection connhana = DriverManager.getConnection(urlhana, userNameHana, passwordHana);
				System.out.println("SAP HANA connected ");

				ArrayList<String> schemaList = getAllSchemas(systemtables, connmssql);

				it = schemaList.iterator();
				showDiffDatabeses(it,outputFile,outputEncoding);
			}
		} catch(ArrayIndexOutOfBoundsException e ){
			printHelp();
		}
	}

	private static void printHelp() {
		System.out.println("input arguments order: usernameSqlServer PasswordSqlServer usernameHana passwordHana urlSqlServer urlHana");
		System.out.println("for example:\" java -jar porting_status_programm-1.0.jar sqluser pw hanauser pw localhost\\TESTDB 15.29.29.12:30015");

	}

	private static String getCurrentDirectory() throws Exception{
		return new java.io.File(".").getCanonicalPath();
	}

	/**
	 * This method is used to get a List with all Systemtables that are not intresting for the porting status.
	 * @return
	 */
	private static ArrayList<String> setUpSystemTables() {
		ArrayList<String> systemtables = new ArrayList<String>();
		systemtables.add("model");
		systemtables.add("msdb");
		systemtables.add("tempdb");
		systemtables.add("master");
		return systemtables;
	}

	/**
	 * This method is to get a list of all databases from the sql server
	 * @param systemtables
	 * @param connmssql
	 * @return
	 * @throws Exception
	 */
	private static ArrayList<String> getAllSchemas(ArrayList<String> systemtables, Connection connmssql)
			throws Exception {
		ArrayList<String> schemaList = new ArrayList<String>();
		DatabaseMetaData metaData = connmssql.getMetaData();
		ResultSet res = metaData.getCatalogs();
		ArrayList<String> schemalist = new ArrayList<String>();
		while (res.next()) {
			if (!systemtables.contains(res.getString("TABLE_CAT"))) {
				schemaList.add(res.getString("TABLE_CAT"));
			}
		}
		return schemaList;
	}


	public static void showDiffDatabeses(Iterator<String> it,String outputFile,String outputEncoding) throws Exception {

		Double sumHana = 0.0;
		Double sumMs = 0.0;

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), outputEncoding));

		while (it.hasNext()) {
			System.out.println("");
			bw.write("\n");
			String schema = it.next();
			Connection connmssql = null;
			Connection connhana = null;
			Statement sthana = null;
			Statement st = null;
			try {
				connmssql = DriverManager.getConnection(urlsqlserver, userNameMSSql, passwordMSSql);
				connhana = DriverManager.getConnection(urlhana, userNameHana, passwordHana);
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("Connection Problem");
			}
			try {
				sthana = connhana.createStatement();
				st = connmssql.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("Statement Problem");
			}

			DatabaseMetaData dbmetadata = connmssql.getMetaData();
			ResultSet rs = dbmetadata.getTables(schema, "dbo", "%", null);
			Double hana;
			Double ms;
			while (rs.next()) {

				String sqlhana = "select count(1) from " + schema + "." + rs.getString(3);
				String sqlmssql = "select count(1) from \"" + schema + "\".\"dbo\".\"" + rs.getString(3) + "\"";
				ResultSet countrshana = null;
				ResultSet countrsms = null;
				try {
					countrshana = sthana.executeQuery(sqlhana + ";");
					countrsms = st.executeQuery(sqlmssql + ";");

					countrshana.next();
					countrsms.next();

					hana = countrshana.getDouble(1);
					ms = countrsms.getDouble(1);
					sumHana = sumHana + hana;
					sumMs = sumMs + ms;
					bw.write(" hana: " + hana);
					System.out.print(" hana: " + hana);
					bw.write(" mssql: " + ms);
					System.out.print(" mssql: " + ms);
					System.out.print("  Diff: " + (ms - hana));
					bw.write("  Diff: " + (ms - hana));
					System.out.print("		prozent: " + ((hana / ms)) * 100);
					bw.write("		prozent: " + ((hana / ms)) * 100);
					System.out.print(" 		schema: " + schema + " table: " + rs.getString(3) + "\n");
					bw.write(" 		schema: " + schema + " table: " + rs.getString(3) + "\n");
				} catch (Exception e) {
					//e.printStackTrace();
				}

			}

		}

		System.out.println("summe Hana: " + sumHana);
		bw.write("summe Hana: " + sumHana);
		System.out.println("Summe MS: " + sumMs);
		bw.write("Summe MS: " + sumMs);
		System.out.println("Prozent: " + (sumHana / sumMs) * 100);
		bw.write("Prozent: " + (sumHana / sumMs) * 100);
	}

}
