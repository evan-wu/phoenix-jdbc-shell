package com.tazhi.phoenix.shell;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a phoenix shell but using jdbc connection.
 * 
 * @author evan.wu
 *
 */
public class PhoenixShellMain {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixShellMain.class);
    
    public static void main(String[] args) {
        Properties prop = new Properties();
        String phoenixUrl = null;
        try {
            InputStream in = new FileInputStream("config.properties");
            prop.load(in);
            String url = prop.getProperty("phoenix.url");
            if (url != null && url.trim().length() > 0)
                phoenixUrl = url;
            else {
                System.out.println("phoenix.url is not configured!");
                System.exit(1);
            }
            in.close();
        } catch (IOException e) {
            System.out.println("config.properties can not be read!");
            System.exit(1);
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;

        try {
            logger.info("Connection to " + phoenixUrl);
            connection = DriverManager.getConnection(phoenixUrl);
        } catch (SQLException e) {
            System.out.println("failed to get phoenix jdbc connection");
            logger.error("Failed to get phoenix jdbc connection", e);
            System.exit(1);
        }

        boolean interactive = false;
        
        if (args.length < 1) {
            interactive = true;
        }
        
        if (interactive) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                StringBuilder sb = new StringBuilder();
                System.out.print("> ");
                while (true) {
                    String input = br.readLine();
                    if ("quit".equals(input) || "exit".equals(input)) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            //
                        }
                        System.exit(0);
                    }
                    
                    sb.append(input);
                    
                    if (input.endsWith(";")) {
                        sb.deleteCharAt(sb.length() -1);
                        try {
                            statement = connection.createStatement();
                            processSql(sb.toString(), connection, rs);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        sb = new StringBuilder();
                        System.out.print("> ");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
        
            try {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < args.length; i++) {
                    sb.append(" ").append(args[i]);
                }
                if (sb.charAt(sb.length() - 1) == ';')
                    sb.deleteCharAt(sb.length() - 1);
                
                processSql(sb.toString(), connection, rs);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (Exception e) {
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (Exception e) {
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    private static void processSql(String sql, Connection connection, ResultSet rs) throws SQLException {
        logger.info("Executing sql " + sql);
        boolean isUpdate = false;
        if (sql.toLowerCase().indexOf("upsert") >= 0 || sql.toLowerCase().indexOf("delete") >=0 || sql.toLowerCase().indexOf("drop")>=0 || sql.toLowerCase().indexOf("create")>=0) {
            isUpdate = true;
        }

        long start = System.currentTimeMillis();
        Statement statement = connection.createStatement();
        if (!isUpdate) {
            rs = statement.executeQuery(sql);
            Object[] result = processResultSet(rs);
            System.out.println(result[0]);
            logger.info("\n\r" + (String)result[0]);
            System.out.println(result[1] + " rows executed in (" + (System.currentTimeMillis() - start) + " milliseconds)");
            logger.info(result[1] + " rows executed in (" + (System.currentTimeMillis() - start) + " milliseconds)");
        } else {
            int updateCount = statement.executeUpdate(sql);
            connection.commit();
            System.out.println(updateCount + " rows updated in (" + (System.currentTimeMillis() - start) + " milliseconds)");
            logger.info(updateCount + " rows updated in (" + (System.currentTimeMillis() - start) + " milliseconds)");
        }
    }
    
    private static Object[] processResultSet(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();

        ResultSetMetaData rsmd = rs.getMetaData();
        int totalCols = rsmd.getColumnCount();
        int[] colCounts = new int[totalCols];
        String[] colLabels = new String[totalCols];
        for (int i = 0; i < totalCols; i++) {
            colCounts[i] = rsmd.getColumnDisplaySize(i + 1);
            colLabels[i] = rsmd.getColumnLabel(i + 1);
            if (colLabels[i].length() > colCounts[i]) {
                colLabels[i] = colLabels[i].substring(0, colCounts[i]);
            }
            sb.append(String.format("| %" + colCounts[i] + "s ", colLabels[i]));
        }
        sb.append("|\n");

        String horizontalLine = getHorizontalLine(colCounts);
        int rows = 0;
        while (rs.next()) {
            rows ++;
            sb.append(horizontalLine);
            for (int i = 0; i < totalCols; i++) {
                sb.append(String.format("| %" + colCounts[i] + "s ", rs.getString(i + 1)));
            }
            sb.append("|\n");

        }

        return new Object[] {(getHorizontalLine(colCounts) + sb.toString()), rows};
    }

    private static String getHorizontalLine(int[] colCounts) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < colCounts.length; i++) {
            sb.append("+");
            for (int j = 0; j < colCounts[i] + 2; j++) {
                sb.append("-");
            }
        }
        sb.append("+\n");

        return sb.toString();
    }
}
