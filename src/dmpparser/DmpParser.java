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

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DmpParser
{
    private static String SRC_FOLDER = null;
    private static String DEST_FOLDER = null;
    private static String ERR_FOLDER = null;
    private static String DB_CONN_URL = null;
    private static String DB_USER = null;
    private static String DB_PASS = null;
    private static String PAYMENT_API_URL = null;
    private static String REVERSAL_API_URL = null;
    
    public static void main(String[] args)
    {
        //parseJson();
        readConfigFile();
        //readFiles();
        
        //Read from file_register table
        readNewFiles();
    }
    
    private static void parseJson()
    {
        //https://www.javatpoint.com/java-json-example
        String s = "{\"name\":\"sonoo\",\"salary\":600000.0,\"age\":27}";
        Object obj = JSONValue.parse(s);
        JSONObject jsonObject = (JSONObject) obj;
        
        String name = (String) jsonObject.get("name");
        double salary = (Double) jsonObject.get("salary");
        long age = (Long) jsonObject.get("age");
        System.out.println(name+" "+salary+" "+age);
    }
    
    private static void readConfigFile()
    {
        BufferedReader reader = null;
        String[] splitLine;
        
        try
        {
            String absPath = new File(DmpParser.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            
            String line = null;
            //reader = new BufferedReader(new FileReader(absPath.substring(0, absPath.length() - "dmpParser.jar".length())+ "/config.txt"));
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
                else if("PAYMENT_API_URL".equals(splitLine[0]))
                {
                    PAYMENT_API_URL = splitLine[1];
                }
                else if("REVERSAL_API_URL".equals(splitLine[0]))
                {
                    REVERSAL_API_URL = splitLine[1];
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
                //System.out.println(s);
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
    
    private static void readNewFiles()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS);
            Statement stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM `file_register` WHERE status = 0");
            
            boolean flag = false;
            String fileName = "";
            String registerId = "";
            int totalLine = 0;
            
            while(rs.next())
            {
                //System.out.println(rs.getString(1) + "  " + rs.getString(2));
                registerId = rs.getString(1);
                fileName = rs.getString(2);
                //flag = checkFile(SRC_FOLDER + "/" + fileName);
                flag = checkFile(SRC_FOLDER + "\\" + fileName);
                
                if(flag == true)
                {
                    //totalLine = preStaging(SRC_FOLDER + "/" + fileName); //1
                    totalLine = preStaging(SRC_FOLDER + "\\" + fileName); //1
                    staging(); //2
                    //moveFile(SRC_FOLDER + "/" + fileName, DEST_FOLDER + "/" + fileName);//3
                    moveFile(SRC_FOLDER + "\\" + fileName, DEST_FOLDER + "\\" + fileName);//3
                    updateRegister(FileStatus.PROCESSED.ordinal(), totalLine, Long.parseLong(registerId));//4
                }
                else //Move file error folder
                {
                    //moveFile(SRC_FOLDER + "/" + fileName, ERR_FOLDER + "/" + fileName);
                    moveFile(SRC_FOLDER + "\\" + fileName, ERR_FOLDER + "\\" + fileName);
                    updateRegister(FileStatus.REJECTED.ordinal(), totalLine, Long.parseLong(registerId));
                }
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Read New Files", ex.toString());
            System.out.println(ex.toString());
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
                else if("2".equals(splitLine[1]))
                {
                    if(splitLine.length < 7)
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
    
    private static int preStaging(String fileName)
    {
        BufferedReader reader = null;
        String line = null;
        String[] splitLine;
        int lineNo = 0;
        String posId = null;
        String terminalId = null;
        String tranType = null;
        int totalLine = 0;
        
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
                totalLine++;
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
        
        return totalLine;
    }
    
    private static void staging()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS);
            Statement stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM `pre_staging` group by terminal_id, pos_id, tran_type, line_no");
            
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
            
            //displayArray(strInitArr);
            //System.out.println(rs.getString(1) + "  " + rs.getString(2)+ "  " + rs.getString(3)+ "  " + rs.getInt(4)+ "  " + rs.getString(5));
            
            conn.close();
            
            //Remove unnecessary rows
            String paymentDate = "";
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
                    //System.out.println(strInitArr[j][0] + " " + strInitArr[j][1]);
                    //System.out.println(strInitArr[j+1][0] + " " + strInitArr[j+1][1]);
                    
                    //Process data to save staging table
                    String[] splitLine1 = strInitArr[j][1].split("\\|");
                    String[] splitLine2 = strInitArr[j+1][1].split("\\|");
                    
                    paymentDate = splitLine1[0].substring(0, splitLine1[0].length() - 3);
                    amount = splitLine1[2];
                    posId = splitLine1[3];
                    terminalId = splitLine1[4];
                    caseId = splitLine1[5];
                    tranType = splitLine1[0].substring(splitLine1[0].length() - 1, splitLine1[0].length());
                    statusCode = splitLine2[5];
                    rrnNo = splitLine2[4];
                    
                    saveStagingInfo(posId, terminalId, paymentDate, tranType, amount, caseId, statusCode, rrnNo);
                    
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
    
    private static void saveStagingInfo(String posId, String terminalId, String paymentDate, String tranType,
            String amount, String caseId, String status, String rrnNo)
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            //Insert data
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                //Insert data
                String query = " insert into staging (pos_id, terminal_id, payment_date, tran_type, amount, case_id, status, rrn_no)"
                        + " values (?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, posId);
                preparedStmt.setString(2, terminalId);
                preparedStmt.setString(3, paymentDate);
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
                //System.out.println(rs.getString(4) + "  " + rs.getString(5) + "  " + rs.getString(6));
                parseData(rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(8), rs.getString(2), rs.getString(3));
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
    
    private static void parseData(String pTranType, String pAmount, String pCaseId, String pRrnNo, String pTerminalId, String pPaymentDate)
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
        
        String[] strPaymentDate = pPaymentDate.split(" ");
        String[] strDate = strPaymentDate[0].split("/");
        String paymentDate = strDate[2] + "-" + strDate[0] + "-" + strDate[1] + " " + strPaymentDate[1] + " " + strPaymentDate[2];
        //String paymentDate = strDate[2] + "-" + strDate[0] + "-" + strDate[1] + " " + strPaymentDate[1];
        
        
        if("F".equals(pTranType))
        {
            updateFineInfo(amount, caseId, pRrnNo, pTerminalId, paymentDate);
        }
        else if("R".equals(pTranType))
        {
            updateFineInfoReversal(amount, caseId, pRrnNo, pTerminalId, paymentDate);
        }
    }
    
    private static void updateFineInfo(String pAmount, String pCaseId, String rrnNo, String terminalId, String paymentDate)
    {
        URL url;
        HttpURLConnection connection = null;
        StringBuilder response = null;
        
        String reqStr =
                "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                + "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                + "<soap:Body>"
                + "<DoCardPayment xmlns=\"http://tempuri.org/\">"
                + "<CaseId>" + pCaseId + "</CaseId>"
                + "<Amount>" + pAmount + "</Amount>"
                + "<PaymentDate>" + paymentDate + "</PaymentDate>"
                + "<TerminalId>" + terminalId + "</TerminalId>"
                + "<username>ucash_card</username>"
                + "<channelKey>A!23$56-8#z</channelKey>"
                + "</DoCardPayment>"
                + "</soap:Body>"
                + "</soap:Envelope>";
        
        try
        {
            //Create connection
            url = new URL(PAYMENT_API_URL);
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
            
            connection.setRequestProperty("Content-Length", "" +
                    Integer.toString(reqStr.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");
            
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            
            //Send request
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
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
        
        try
        {
            if(response != null && !response.toString().contains("Fault"))
            {
                System.out.println("Case Id: " + pCaseId + " request is sent");
                saveTransactionLog(pCaseId, pAmount, response.toString(), rrnNo, terminalId, "F");
            }
            else
            {
                response = new StringBuilder();
                response.append("<soap:Body>");
                response.append("<DoCardPaymentResponse xmlns=\"http://tempuri.org/\">");
                response.append("<DoCardPaymentResult>");
                response.append("[{\"MessageCode\":\"9010\",\"MessageResponse\":\"Error in API communication\"}]");
                response.append("</DoCardPaymentResult>");
                response.append("</DoCardPaymentResponse>");
                response.append("</soap:Body>");
                
                saveTransactionLog(pCaseId, pAmount, response.toString(), rrnNo, terminalId, "F");
            }
        }
        catch (Exception ex)
        {
            saveErrorLog("Update Fine Info", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    private static void updateFineInfoReversal(String pAmount, String pCaseId, String rrnNo, String terminalId, String reversalTime)
    {
        URL url;
        HttpURLConnection connection = null;
        StringBuilder response = null;
        
        String reqStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                + "<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                + "<soap:Body>"
                + "<DoCardPaymentReversal xmlns=\"http://tempuri.org/\">"
                + "<CaseId>" + pCaseId + "</CaseId>"
                + "<Amount>" + pAmount + "</Amount>"
                + "<RRN>" + rrnNo + "</RRN>"
                + "<ReversalTime>" + reversalTime + "</ReversalTime>"
                + "<TerminalId>" + terminalId + "</TerminalId>"
                + "<username>ucash_card</username>"
                + "<channelKey>A!23$56-8#z</channelKey>"
                + "</DoCardPaymentReversal>"
                + "</soap:Body>"
                + "</soap:Envelope>";
        
        try
        {
            //Create connection
            url = new URL(REVERSAL_API_URL);
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
            
            connection.setRequestProperty("Content-Length", "" +
                    Integer.toString(reqStr.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");
            
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            
            //Send request
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
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
            saveErrorLog("Update Fine Info Reversal", ex.toString());
            System.out.println(ex.toString());
        }
        finally
        {
            if(connection != null)
            {
                connection.disconnect();
            }
        }
        
        try
        {
            if(response != null && !response.toString().contains("Fault"))
                
            {
                System.out.println("Case Id: " + pCaseId + " request is sent");
                saveTransactionLog(pCaseId, pAmount, response.toString(), rrnNo, terminalId, "R");
            }
            else
            {
                response = new StringBuilder();
                response.append("<soap:Body>");
                response.append("<DoCardPaymentReversalResponse xmlns=\"http://tempuri.org/\">");
                response.append("<DoCardPaymentReversalResult>");
                response.append("[{\"MessageCode\":\"9010\",\"MessageResponse\":\"Error in API communication\"}]");
                response.append("</DoCardPaymentReversalResult>");
                response.append("</DoCardPaymentReversalResponse>");
                response.append("</soap:Body>");
                
                saveTransactionLog(pCaseId, pAmount, response.toString(), rrnNo, terminalId, "R");
            }
        }
        catch (Exception ex)
        {
            saveErrorLog("Update Fine Info Reversal", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    private static void saveTransactionLog(String pCaseId, String pAmount, String pRequest, String rrnNo, String terminalId, String tranType)
    {
        String response = "";
        String messageCode = "";
        String messageResponse = "";
        Object obj = null;
        JSONObject jsonObject = null;
        
        try
        {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(pRequest));
            
            Document doc = dBuilder.parse(is);
            doc.getDocumentElement().normalize();
            
            if("F".equals(tranType))
            {
                NodeList nList = doc.getElementsByTagName("DoCardPaymentResponse");
                
                Node nNode = nList.item(0);
                
                if (nNode.getNodeType() == Node.ELEMENT_NODE)
                {
                    Element eElement = (Element) nNode;
                    
                    response = eElement.getElementsByTagName("DoCardPaymentResult").item(0).getTextContent();
                    
                    obj = JSONValue.parse(response.substring(1, response.length() - 1));
                    jsonObject = (JSONObject) obj;
                    
                    messageCode = (String) jsonObject.get("MessageCode");
                    messageResponse = (String) jsonObject.get("MessageResponse");
                }
            }
            else if("R".equals(tranType))
            {
                NodeList nList = doc.getElementsByTagName("DoCardPaymentReversalResponse");
                
                Node nNode = nList.item(0);
                
                if (nNode.getNodeType() == Node.ELEMENT_NODE)
                {
                    Element eElement = (Element) nNode;
                    
                    response = eElement.getElementsByTagName("DoCardPaymentReversalResult").item(0).getTextContent();
                    
                    obj = JSONValue.parse(response.substring(1, response.length() - 1));
                    jsonObject = (JSONObject) obj;
                    
                    messageCode = (String) jsonObject.get("MessageCode");
                    messageResponse = (String) jsonObject.get("MessageResponse");
                }
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
                String query = "insert into transactions (case_id, amount, created_at, rrn_no, terminal_Id, message_code, message_response, tran_type)"
                        + " values (?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setString(1, pCaseId);
                preparedStmt.setString(2, pAmount);
                
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                preparedStmt.setTimestamp(3, timestamp);
                
                preparedStmt.setString(4, rrnNo);
                preparedStmt.setString(5, terminalId);
                preparedStmt.setString(6, messageCode);
                preparedStmt.setString(7, messageResponse);
                preparedStmt.setString(8, tranType);
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Save Transaction Log", ex.toString());
            System.out.println(ex.toString());
        }
    }
    
    private static void updateRegister(int status, int totalLine, long registerId)
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            try (Connection conn = DriverManager.getConnection(DB_CONN_URL, DB_USER, DB_PASS))
            {
                String query = "update file_register set STATUS = ?, total_line = ?, updated_at = ? where id = ?";
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                preparedStmt.setInt(1, status);
                preparedStmt.setInt(2, totalLine);
                
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                preparedStmt.setTimestamp(3, timestamp);
                
                preparedStmt.setLong(4, registerId);
                
                preparedStmt.execute();
            }
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            saveErrorLog("Update Register", ex.toString());
            System.out.println(ex.toString());
        }
    }
}
