package com.test;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Locale;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cluster {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    private static final String CREATE_TABLE = "create table data(id varchar(30), ts varchar(30), ip varchar(20), state varchar(500),data CLOB)";
    private static final String INSERT = "insert into data (id, ts, ip, state, data) values(?,?,?,?,?)";

    private static final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    private static String path;
    private static Logger log;
    public static void main(String[] args) {
        org.apache.log4j.Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(new PatternLayout("%d %p (%t) [%c] - %m%n")));
        root.setLevel(Level.INFO);

        for (String a: args) {
            if(a.startsWith("-p")){
                path = a.substring(3, args[0].length());
            }else if(a.startsWith("-d")){
                root.setLevel(Level.DEBUG);
            }
        }
        if ("".equals(path)){
            System.out.println("argument -p=path to log folder is required");
            return;
        }
        log = Logger.getLogger(Cluster.class);

        try {

            log.info("Starting ----");
            Connection dbConnection = prepareDB();
            processFiles(dbConnection, path);
            dbConnection.close();
            log.info("Completed ----");

        } catch (Exception ex) {
            log.error(ex);
        }

    }

    private static void processFiles(Connection dbConnection, String path) {

        FilenameFilter init = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().contains("cluster") && !name.toLowerCase().contains("-sorted");
            }
        };

        //get all files
        try {
            File dir = new File(path);

            File files[] = dir.listFiles(init);

            ResultSet rs;
            File output = null;
            for (File file : files) {

                output = new File(file.getAbsolutePath() + "-sorted");

                if (!output.exists()) {
                    log.info("Start processing file: " + file.getAbsolutePath());
                    prepareData(dbConnection, file);


                } else{
                    log.info("File: "+ file.getAbsolutePath() + " has been previously sorted.");
                }
            }

            if(output != null) {
                rs = dbConnection.prepareStatement("Select ts, ip, data from data where state <> '' order by ts, id").executeQuery();
                BufferedWriter writer = new BufferedWriter(new FileWriter(output.getAbsolutePath()));
                while (rs.next()) {
                    //writer.write(rs.getString("ts") + "#" + rs.getString("ip") + "#" + rs.getString("data"));
                    writer.write(rs.getString("data"));
                }
                writer.close();
            }
/*
                    rs = dbConnection.prepareStatement("Select ip, state, data from data order by id").executeQuery();

                    BufferedWriter writer = new BufferedWriter(new FileWriter(output.getAbsolutePath()));

                    while(rs.next()){
                        writer.write(rs.getString("data"));
                    }
                    writer.close();

 */
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private static void prepareData(Connection dbConnection, File file){

        StringBuilder sb = new StringBuilder();
        Pattern ex = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
        String tmp ="";
        String prvKey = "";
        Long seq = Long.parseLong("0");
        String ts = "";
        String state = "";
        String ip = "";

        Pattern exip = Pattern.compile("\\[((?:[0-9]{1,3}\\.){3}[0-9]{1,3})\\]:");
        try {
            /*
            PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
            ps.executeUpdate();
            ps.close();
            */
            PreparedStatement ps = dbConnection.prepareStatement(INSERT);

            Scanner sc = new Scanner(file);
            Matcher m;

            while(sc.hasNext()) {
                tmp = sc.nextLine();
                //extract ip address
                if ("".equals(ip)){
                    m = exip.matcher(tmp);
                    if(m.find()){
                        ip = m.group(1);
                    }
                }

                tmp = sc.nextLine();
                m = ex.matcher(tmp);

                if (m.find()) {
                    ts = m.group(0);
                    prvKey = String.format("000000", seq);
                    sb.append(tmp + "\n");
                    state = parseLine(tmp);


                    while (sc.hasNext()) {
                        tmp = sc.nextLine();
                        if ("".equals(ip)){
                            m = exip.matcher(tmp);
                            if(m.find()){
                                ip = m.group(1);
                            }
                        }
                        m = ex.matcher(tmp);
                        if (m.find()) {
                            ps.setString(1, prvKey);
                            ps.setString(2, ts);
                            ps.setString(3, ip);
                            ps.setString(4, state);
                            ps.setString(5, sb.toString());
                            ps.executeUpdate();

                            seq++;
                            prvKey = String.format("000000", seq);
                            ts = m.group(0);
                            sb.setLength(0);
                            state = parseLine(tmp);
                        }
                        sb.append(tmp + "\n");
                    }
                }
            }
            if(sb.length()>0) {
                ps.setString(1, prvKey);
                ps.setString(2, ts);
                ps.setString(3, ip);
                ps.setString(4, state);
                ps.setString(5, sb.toString());
                ps.executeUpdate();
            }
            ps.close();
        }
        catch (StringIndexOutOfBoundsException e){
            log.error(e);
        }
        catch (SQLException e){
            log.error(e);
        }
        catch(FileNotFoundException e){
            log.error(e);
        }
    }

    private static String parseLine(String line){

        int i = line.toLowerCase().indexOf("removing member");

        if (i > 0){
            return line.substring(i);
        }

        i = line.toLowerCase().indexOf("initialized new");
        if (i > 0){
            return line.substring(i);
        }

        i = line.toLowerCase().indexOf("cluster.clusterservice");
        if (i > 0){
            return line.substring(i);
        }

        i = line.toLowerCase().indexOf("connection[id=");
        if (i > 0){
            return line.substring(i);
        }

        return "";
    }


    private static Connection prepareDB(){
        Connection dbConnection = null;

        try {
            log.debug("Starting in memory h2 server");
            Class.forName(DB_DRIVER);
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

            PreparedStatement createTable = dbConnection.prepareStatement(CREATE_TABLE);
            createTable.executeUpdate();
            createTable.close();

            return dbConnection;
        } catch (SQLException e) {
            log.error(e);
        } catch (ClassNotFoundException e) {
            log.error(e);
        }

        return  null;
    }
}
