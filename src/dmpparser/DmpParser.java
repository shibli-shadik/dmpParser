package dmpparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DmpParser
{
    private static String SRC_FOLDER = null;
    private static String DEST_FOLDER = null;
    private static String DB_CONN_URL = null;
    private static String DB_USER = null;
    private static String DB_PASS = null;
    private static String API_URL = null;
    
    public static void main(String[] args)
    {
        readConfigFile();
        readFiles();
    }
    
    private static void readConfigFile()
    {
        BufferedReader reader = null;
        String[] splitLine;
        
        try
        {
            String absPath = new File(DmpParser.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            
            String line = null;
            //reader = new BufferedReader(new FileReader(absPath.substring(0, absPath.length() - "dmpapp.jar".length())+ "/config.txt"));
            reader = new BufferedReader(new FileReader("F:\\projects\\dmp\\config.txt"));
            
            while((line = reader.readLine()) != null)
            {
                splitLine = line.split("#");
                
                if("SRC_FOLDER".equals(splitLine[0]))
                {
                    SRC_FOLDER = splitLine[1];
                }
                else if("DEST_FOLDER".equals(splitLine[0]))
                {
                    DEST_FOLDER = splitLine[1];
                }
                else if("DB_CONN_URL".equals(splitLine[0]))
                {
                    DB_CONN_URL = splitLine[1];
                }
                else if("DB_USER".equals(splitLine[0]))
                {
                    DB_USER = splitLine[1];
                }
                else if("DB_PASS".equals(splitLine[0]))
                {
                    DB_PASS = splitLine[1];
                }
                else if("API_URL".equals(splitLine[0]))
                {
                    API_URL = splitLine[1];
                }
            }
            
            reader.close();
        }
        catch (Exception ex)
        {
            saveErrorLog("Read Config File", ex.toString());
            System.out.println(ex.toString());
        }
        finally
        {
            try
            {
                reader.close();
            }
            catch (IOException ex)
            {
                saveErrorLog("Read Config File", ex.toString());
                System.out.println(ex.toString());
            }
        }
    }
    
    private static void readFiles()
    {
        final File folder = new File(SRC_FOLDER);
        List<String> result = new ArrayList<>();
        
        search(".*\\.txt", folder, result);
        
        if(result.size() > 0)
        {
            for (String s : result)
            {
                //New code
                //loadDataNew(SRC_FOLDER + "\\" + s);
                //moveFile(SRC_FOLDER + "\\" + s, DEST_FOLDER + "\\" + s);
                System.out.println(s);
                preStaging(SRC_FOLDER + "\\" + s); //1
                staging(); //2
            }
        }
        else
        {
            saveErrorLog("Read Files", "There is no file to read");
            System.out.println("There is no file to read");
        }
    }
    
    private static void preStaging(String fileName)
    {
        BufferedReader reader = null;
        String line = null;
        String[] splitLine;
        int lineNo = 0;
        String posId = null;
        String terminalId = null;
        String tranType = null;
        
        deletePreStagingInfo();
        
        try
        {
            reader = new BufferedReader(new FileReader(fileName));
            
            while((line = reader.readLine()) != null)
            {
                splitLine = line.split("\\|");
                
                lineNo = Integer.parseInt(splitLine[1]);
                posId = splitLine[3];
                tranType = splitLine[0].substring(splitLine[0].length() - 1, splitLine[0].length());
                
                if(lineNo == 1)
                {
                    terminalId = splitLine[4];
                }
                else if(lineNo == 2)
                {
                    terminalId = splitLine[6];
                }
                
                //System.out.println(tranType +" "+lineNo+" "+posId+" "+terminalId + " "+line);
                savePreStagingInfo(posId, terminalId, tranType, lineNo, line);
            }
            
        }
        catch (FileNotFoundException  ex)
        {
            saveErrorLog("Pre Staging", ex.toString());
            System.out.println(ex.toString());
        }
        catch (IOException ex)
        {
            saveErrorLog("Pre Staging", ex.toString());
            System.out.println(ex.toString());
        }
        finally
        {
            try
            {
                reader.close();
            }
            catch (IOException ex)
            {
                saveErrorLog("Pre Staging", ex.toString());
                System.out.println(ex.toString());
            }
        }
    }
    
    private static void staging()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS);
            Statement stmt = conn.createStatement();
            
            //Get data
            ResultSet rs = stmt.executeQuery("SELECT * FROM `pre_staging` group by terminal_id, pos_id, tran_type, line_no");
            
            int lineNo = 0;
            int size = 0;
            if(rs != null)
            {
                rs.last();
                size = rs.getRow();
            }
            
            String[][] strInitArr = new String[size][2];
            
            rs = stmt.executeQuery("SELECT * FROM `pre_staging` group by terminal_id, pos_id, tran_type, line_no");
            int i = 0;
            
            while(rs.next())
            {
                //System.out.println(rs.getString(1) + "  " + rs.getString(2)+ "  " + rs.getString(3)+ "  " + rs.getInt(4)+ "  " + rs.getString(5));
                strInitArr[i][0] = rs.getString(4);
                strInitArr[i][1] = rs.getString(5);
                i++;
                
            }
            
            displayArray(strInitArr);
            //System.out.println(rs.getString(1) + "  " + rs.getString(2)+ "  " + rs.getString(3)+ "  " + rs.getInt(4)+ "  " + rs.getString(5));
            
            conn.close();
            
            //Remove unnecessary rows
            for (int j = 0; j < strInitArr.length;)
            {
                if((j+1) < strInitArr.length && "2".equals(strInitArr[j+1][0]))
                {
                    System.out.println(strInitArr[j][0] + " " + strInitArr[j][1]);
                    System.out.println(strInitArr[j+1][0] + " " + strInitArr[j+1][1]);
                    j+=2;
                }
                else
                {
                    j++;
                }
            }
            
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Staging", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    public static void deletePreStagingInfo()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS);
            Statement stmt = conn.createStatement();
            
            String delSql = "delete from pre_staging";
            stmt.executeUpdate(delSql);
            
            conn.close();
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Delete Pre Staging Info", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    public static void savePreStagingInfo(String posId, String terminalId, String tranType, int lineNo, String data)
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                String query = "insert into pre_staging (pos_id, terminal_id, tran_type, line_no, data)"
                        + " values (?, ?, ?, ?, ?)";
                
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, posId);
                preparedStmt.setString(2, terminalId);
                preparedStmt.setString(3, tranType);
                preparedStmt.setInt(4, lineNo);
                preparedStmt.setString(5, data);
                
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Save Pre Staging Info", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    public static void search(final String pattern, final File folder, List<String> result)
    {
        for (final File f : folder.listFiles())
        {
            if (f.isDirectory())
            {
                search(pattern, f, result);
            }
            
            if (f.isFile())
            {
                if (f.getName().matches(pattern))
                {
                    result.add(f.getName());
                }
            }
        }
    }
    
    private static void moveFile(String src, String dest)
    {
        Path result = null;
        
        try
        {
            new File(dest).delete();
            result =  Files.move(Paths.get(src), Paths.get(dest));
        }
        catch (IOException e)
        {
            System.out.println("Exception while moving file: " + e.getMessage());
        }
        
        if(result != null)
        {
            System.out.println(src + " moved to " + dest);
        }
        else
        {
            System.out.println(src + " movement failed.");
        }
    }
    
    public static void saveErrorLog(String errorFor, String message)
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            
            //Insert data
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                String query = " insert into error_log (error_for, message, created_at)"
                        + " values (?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, errorFor);
                preparedStmt.setString(2, message);
                
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                preparedStmt.setTimestamp(3, timestamp);
                
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            System.out.println(ex.toString());
        }
    }
    
    private static void displayArray(String[][] strArr)
    {
        System.out.println("-------------------------------------");
        System.out.println("Id\t\tData");
        for (int i = 0; i < strArr.length; i++)
        {
            String[] itemRecord = strArr[i];
            System.out.println(itemRecord[0] + "\t\t" + itemRecord[1]);
        }
        System.out.println("-------------------------------------");
    }
}
