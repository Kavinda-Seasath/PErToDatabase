package com.directfn.tools;

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws SQLException {

        String      filepath;
        String      line;
        String      insertQry = "INSERT INTO $tablename VALUES ('$date','$values');";
        String      createTbl = "CREATE TABLE $tablename (" +
                "date DATE,raw_ID int PRIMARY KEY NOT NULL,exchange varchar(255),symbol varchar(255),isEndOfDay varchar(255)," +
                "periodStart varchar(255),periodStop varchar(255),twaSpread varchar(255),rtwaSpread varchar(255)," +
                "twaBestBid varchar(255),twaBestAsk varchar(255),tvwaQuotation varchar(255),twBestAskVolume varchar(255)," +
                "twBestAskValue varchar(255),twnoBestAsk varchar(255),twVolumeAskSide varchar(255),twValueAskSide varchar(255)," +
                "twnoAskSide varchar(255),twBestBidVolume varchar(255),twBestBidValue varchar(255),twnoBestBid varchar(255)," +
                "twVolumeBidSide varchar(255),twValueBidSide varchar(255),twnoBidSide varchar(255),twMidPointVolume varchar(255)," +
                "twMidPointValue varchar(255),twnoMidPoint varchar(255),twtVolumeMidPoint varchar(255),twtValueMidPoint varchar(255)," +
                "twtoMidPoint varchar(255),totalOrderCoverage varchar(255),bidOrderCoverage varchar(255),askOrderCoverage varchar(255)," +
                "beVWAPBidSide varchar(255),beVWAPAskSide varchar(255),twTotalVolumeBidSide varchar(255),twTotalVolumeAskSide varchar(255)," +
                "twbaIndicatorBest varchar(255),twbaIndicatorAll varchar(255),twcIndicatorBidSide varchar(255)," +
                "twcIndicatorAskSide varchar(255),localRoundTripClasses varchar(255),localRoundTripCosts varchar(255)," +
                "localRoundTripCoverages varchar(255),referenceRoundTripClasses varchar(255),referenceRoundTripCosts varchar(255)," +
                "referenceRoundTripCoverages  varchar(255) );";

        Connection con;
        Statement stmt;


        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream("config" + File.separator + "dbconfig.properties");
            prop.load(in);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        String connectionurl = prop.getProperty("jdbcurl");
        con = DriverManager.getConnection(connectionurl);




        Scanner scanner = new Scanner(System.in);
        System.out.println("Add file path: (ex: C:\\file path )");
        filepath = scanner.nextLine();


        File folder = new File(filepath);

        //reading content on filepath
        FilenameFilter txtFileFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name) {
                String lowercaseName = name.toLowerCase();
                if(lowercaseName.endsWith(".txt")) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        File[] files = folder.listFiles(txtFileFilter);

        System.out.println("Number of files found : "+files.length);

        for (File file:files){

            String tblName = file.getName().substring(0,24).replace("-","_");
            String date = file.getName().substring(11,15) +"-"+ file.getName().substring(8,10)+"-"+file.getName().substring(5,7);
            System.out.println(date);

            ResultSet rs = con.getMetaData().getTables(null,null,tblName,null);

            if (rs.next()){
                System.out.println(rs.getString("TABLE_NAME")+" Already Exist");
            }else {

                stmt = con.createStatement();
                String query = createTbl.replace("$tablename",tblName);
                System.out.println(query);
                stmt.executeUpdate(query);



                try {

                    BufferedReader br = new BufferedReader(new FileReader(file));

                    br.readLine();

                    while ((line = br.readLine()) != null){

                        //System.out.println(line);
                        //String[] frame = line.split("\\|");
                        //System.out.println(frame.length);
                        String values = line.replace("| ","','");
                        String insQry = insertQry.replace("$tablename",tblName).replace("$values",values).replace("$date",date);
                        //System.out.println(insQry);
                        stmt.executeUpdate(insQry);


                    }

                } catch (FileNotFoundException e) {

                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                stmt.close();
            }


        }



    }
}
