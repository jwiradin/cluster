package com.test;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Locale;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Sort {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    private static final String CREATE_TABLE = "create table data(id varchar(30), data CLOB)";
    private static final String INSERT = "insert into data (id, data) values(?,?)";

    private static final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    private static String path;

    public static void main(String[] args) {

        if (args.length == 0 || !args[0].startsWith("-p")) {
            System.out.println("Parameter: -p=log file path");
            return;
        }

        path = args[0].substring(3, args[0].length());

        try {

            System.out.println(dtFormatter.format(LocalDateTime.now()) + " Starting ----");
            Connection dbConnection = prepareDB();
            processFiles(dbConnection, path);
            dbConnection.close();
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " Completed ----");

        } catch (Exception ex) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " Error: " + ex.getMessage());
        }

    }

    private static void processFiles(Connection dbConnection, String path) {

        FilenameFilter init = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().contains("pega") && !name.toLowerCase().contains("-sorted");
            }
        };

        //get all files
        try {
            File dir = new File(path);

            File files[] = dir.listFiles(init);

            ResultSet rs;

            for (File file : files) {

                File output = new File(file.getAbsolutePath() + "-sorted");

                if (!output.exists()) {

                    System.out.println(dtFormatter.format(LocalDateTime.now()) + " Start processing file: " + file.getAbsolutePath());
                    prepareData(dbConnection, file);

                    rs = dbConnection.prepareStatement("Select data from data order by id").executeQuery();

                    BufferedWriter writer = new BufferedWriter(new FileWriter(output.getAbsolutePath()));

                    while(rs.next()){
                        writer.write(rs.getString("data"));
                    }
                    writer.close();

                } else{
                    System.out.println(dtFormatter.format(LocalDateTime.now()) + " File: "+ file.getAbsolutePath() + " has been previously sorted.");
                }
            }

        } catch (Exception ex) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " + ex.getMessage());
        }
    }

    private static void prepareData(Connection dbConnection, File file){

        StringBuilder sb = new StringBuilder();
        Pattern ex = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
        String tmp ="";
        String prvKey = "";
        Long seq = Long.parseLong("0");

        try {
            PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
            ps.executeUpdate();
            ps.close();

            ps = dbConnection.prepareStatement("insert into data (id,data) values(?,?)");

            Scanner sc = new Scanner(file);

            while(sc.hasNext()) {
                tmp = sc.nextLine();
                if (ex.matcher(tmp).find()) {
                    prvKey = tmp.substring(0, 23) + String.format("000000", seq);
                    sb.append(tmp + "\n");

                    while (sc.hasNext()) {
                        tmp = sc.nextLine();
                        if (ex.matcher(tmp).find()) {
                            ps.setString(1, prvKey);
                            ps.setString(2, sb.toString());
                            ps.executeUpdate();

                            seq++;
                            prvKey = tmp.substring(0, 23) + String.format("000000", seq);
                            sb.setLength(0);
                        }
                        sb.append(tmp + "\n");
                    }
                }
            }
            if(sb.length()>0) {
                ps.setString(1, prvKey);
                ps.setString(2, sb.toString());
                ps.executeUpdate();
            }
            ps.close();
        }
        catch (StringIndexOutOfBoundsException bex){
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " prvkey:"+ prvKey+" string:" + tmp + " - "  + bex.getMessage());
        }
        catch (SQLException exs){
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " +exs.getMessage());
        }
        catch(FileNotFoundException exf){
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " +exf.getMessage());
        }
    }

    private static Connection prepareDB(){
        Connection dbConnection = null;

        try {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " Starting in memory h2 server");
            Class.forName(DB_DRIVER);
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

            PreparedStatement createTable = dbConnection.prepareStatement(CREATE_TABLE);
            createTable.executeUpdate();
            createTable.close();

            return dbConnection;
        } catch (SQLException e) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " + e.getMessage());
        }

        return  null;
    }
}
