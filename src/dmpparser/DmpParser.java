package dmpparser;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
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
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class DmpParser
{
    private static String SRC_FOLDER = null;
    private static String DEST_FOLDER = null;
    private static String ERR_FOLDER = null;
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
                else if("ERR_FOLDER".equals(splitLine[0]))
                {
                    ERR_FOLDER = splitLine[1];
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
            boolean flag = false;
            for (String s : result)
            {
                System.out.println(s);
                flag = checkFile(SRC_FOLDER + "\\" + s);
                
                if(flag == true)
                {
                    preStaging(SRC_FOLDER + "\\" + s); //1
                    staging(); //2
                    moveFile(SRC_FOLDER + "\\" + s, DEST_FOLDER + "\\" + s);//3
                }
                else //Move file error folder
                {
                    moveFile(SRC_FOLDER + "\\" + s, ERR_FOLDER + "\\" + s);
                }
            }
        }
        else
        {
            saveErrorLog("Read Files", "There is no file to read");
            System.out.println("There is no file to read");
        }
    }
    
    private static boolean checkFile(String fileName)
    {
        boolean flag = true;
        BufferedReader reader = null;
        String line = null;
        String[] splitLine;
        
        try
        {
            reader = new BufferedReader(new FileReader(fileName));
            
            while((line = reader.readLine()) != null)
            {
                splitLine = line.split("\\|");
                
                if("1".equals(splitLine[1]))
                {
                    if(splitLine.length != 6)
                    {
                        flag = false;
                        break;
                    }
                }
                if("2".equals(splitLine[1]))
                {
                    if(splitLine.length != 8)
                    {
                        flag = false;
                        break;
                    }
                }
            }
        }
        catch (FileNotFoundException  ex)
        {
            saveErrorLog("Check File", ex.toString());
            System.out.println(ex.toString());
        }
        catch (IOException ex)
        {
            saveErrorLog("Check File", ex.toString());
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
                saveErrorLog("Check File", ex.toString());
                System.out.println(ex.toString());
            }
        }
        
        return flag;
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
            String date = "";
            String amount = "";
            String posId = "";
            String terminalId = "";
            String caseId = "";
            String tranType = "";
            String statusCode = "";
            String rrnNo = "";
            
            for (int j = 0; j < strInitArr.length;)
            {
                if((j+1) < strInitArr.length && "2".equals(strInitArr[j+1][0]))
                {
                    System.out.println(strInitArr[j][0] + " " + strInitArr[j][1]);
                    System.out.println(strInitArr[j+1][0] + " " + strInitArr[j+1][1]);
                    
                    //Process data to save staging table
                    String[] splitLine1 = strInitArr[j][1].split("\\|");
                    String[] splitLine2 = strInitArr[j+1][1].split("\\|");
                    
                    date = splitLine1[0].substring(0, splitLine1[0].length() - 3);
                    amount = splitLine1[2];
                    posId = splitLine1[3];
                    terminalId = splitLine1[4];
                    caseId = splitLine1[5];
                    tranType = splitLine1[0].substring(splitLine1[0].length() - 1, splitLine1[0].length());
                    statusCode = splitLine2[5];
                    rrnNo = splitLine2[4];
                    
                    saveStagingInfo(posId, terminalId, date, tranType, amount, caseId, statusCode, rrnNo);
                    
                    j+=2;
                }
                else
                {
                    j++;
                }
            }
            
            processStaging();
            
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
        catch (IOException ex)
        {
            saveErrorLog("Move File", ex.toString());
            System.out.println(ex.toString());
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
    
    private static void saveStagingInfo(String posId, String terminalId, String createdAt, String tranType, 
            String amount, String caseId, String status, String rrnNo)
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            //Insert data
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                //Insert data
                String query = " insert into staging (pos_id, terminal_id, created_at, tran_type, amount, case_id, status, rrn_no)"
                        + " values (?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, posId);
                preparedStmt.setString(2, terminalId);
                preparedStmt.setString(3, createdAt);
                preparedStmt.setString(4, tranType);
                preparedStmt.setString(5, amount);
                preparedStmt.setString(6, caseId);
                preparedStmt.setString(7, status);
                preparedStmt.setString(8, rrnNo);
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Save Staging Info", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    private static void processStaging()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS);
            Statement stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM `staging` where status = '00'");
            
            while(rs.next())
            {
                System.out.println(rs.getString(4) + "  " + rs.getString(5) + "  " + rs.getString(6));
                parseData(rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(8));
            }
            
            String delSql = "delete from staging";
            stmt.executeUpdate(delSql);
            
            conn.close();
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Save Staging Info", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    private static void parseData(String tranType, String pAmount, String pCaseId, String rrnNo)
    {
        //System.out.println("amount: "+pAmount + " caseId: "+pCaseId);
        
        pAmount = pAmount.replaceFirst("^0+(?!$)", "");
        String wholeAmt = pAmount.substring(0, pAmount.length() - 2);
        String fracAmt = pAmount.substring(pAmount.length()-2, pAmount.length());
        String amount = wholeAmt + "." + fracAmt;
        //System.out.println(amount);
        
        int v1 = Integer.parseInt(pCaseId.substring(0, 3));
        //System.out.println(v1);
        
        int v2 = Integer.parseInt(pCaseId.substring(3 + v1, 3 + v1 + 3));
        //System.out.println(v2);
        
        String s1 = pCaseId.substring(3 + v1, pCaseId.length());
        //System.out.println(s1);
        
        String caseId = s1.substring(3, v2 + 3);
        caseId = caseId.substring(2, caseId.length());
        //System.out.println(caseId);
        
        if("F".equals(tranType))
        {
            updateFineInfo(amount, caseId, rrnNo);
        }
        else if("R".equals(tranType))
        {
            updateFineInfoReversal(amount, caseId, rrnNo);
        }
        
        //payFine(amount, caseId);
    }
    
    private static void updateFineInfo(String pAmount, String pCaseId, String rrnNo)
    {
        URL url;
        HttpURLConnection connection = null;
        StringBuilder response = null;
        
        String reqStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?> "
                + "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                + "xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" "
                + "xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                + "<soap:Body>"
                + "<PayFineByCard xmlns=\"http://dmpsoap.com\">"
                + "<key>*7495#</key>"
                + "<channel>8</channel>"
                + "<case_id>" + pCaseId + "</case_id>"
                + "<amount>" + pAmount + "</amount>"
                + "</PayFineByCard>"
                + "</soap:Body>"
                + "</soap:Envelope>";
        
        try
        {
            //Create connection
            url = new URL(API_URL);
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
            
            connection.setRequestProperty("Content-Length", "" +
                    Integer.toString(reqStr.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");
            
            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            
            //Send request
            DataOutputStream wr = new DataOutputStream (
                    connection.getOutputStream ());
            wr.writeBytes (reqStr);
            wr.flush ();
            wr.close ();
            
            //Get Response
            InputStream is = null;
            
            try
            {
                is = connection.getInputStream();
            }
            catch(IOException exception)
            {
                //if something wrong instead of the output, read the error
                is = connection.getErrorStream();
            }
            
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            
            response = new StringBuilder();
            
            while((line = rd.readLine()) != null)
            {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            connection.disconnect();
        }
        catch (IOException ex)
        {
            saveErrorLog("Update Fine Info", ex.toString());
            System.out.println(ex.toString());
        }
        finally
        {
            if(connection != null)
            {
                connection.disconnect();
            }
        }
        
        if(response != null)
        {
            System.out.println("Case Id: " + pCaseId + " request is sent");
            saveTransactionLog(pCaseId, pAmount, response.toString(), rrnNo);
        }
        
    }
    
    private static void updateFineInfoReversal(String pAmount, String pCaseId, String rrnNo)
    {
        System.out.println(pAmount + " " + pCaseId);
    }
    
    private static void saveTransactionLog(String pCaseId, String pAmount, String pRequest, String rrnNo)
    {
        String status = "";
        
        try
        {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(pRequest));
            
            Document doc = dBuilder.parse(is);
            doc.getDocumentElement().normalize();
            
            NodeList nList = doc.getElementsByTagName("PayFineByCardResponse");
            
            Node nNode = nList.item(0);
            
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                
                Element eElement = (Element) nNode;
                
                status = eElement.getElementsByTagName("PayFineByCardResult").item(0).getTextContent();
            }
        }
        catch (IOException | ParserConfigurationException | DOMException | SAXException ex)
        {
            saveErrorLog("XML parsing in Save Transaction Log", ex.toString());
            System.out.println(ex.toString());
        }
        
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                String query = "insert into transactions (case_id, amount, status, created_at, rrn_no)"
                        + " values (?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, pCaseId);
                preparedStmt.setString(2, pAmount);
                preparedStmt.setString(3, status);
                
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                preparedStmt.setTimestamp(4, timestamp);
                
                preparedStmt.setString(5, rrnNo);
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Save Transaction Log", ex.toString());
            System.out.println(ex.toString());
        }
    }
}
