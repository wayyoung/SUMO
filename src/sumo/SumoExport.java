package sumo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sumologic.client.Credentials;
import com.sumologic.client.SumoLogicClient;
import com.sumologic.client.model.LogMessage;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetSearchJobStatusResponse;
import com.sun.org.apache.xml.internal.serialize.OutputFormat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class SumoExport {
    String accessId = "sueijjowpajxEu";
    String accessKey = "ZMVwrxLJgw53YbFfewNlphmq3DSdxi9s8j7djTZgUkmm9fyj5OjyZTuPNcRGTR48";
    String query="_sourceCategory=dev1/device/unknown/* AND fw_version = 5.1.0* AND (proxyDisconnnected or proxyConnected or CTRL-EVENT or _sourceCategory=*/ConnectivityService or _sourceCategory=*/RTW or _sourceCategory=*/ifdu or _sourceCategory=*/init or suspend entry or NETWORK_DISCONNECTION_EVENT or RTW_DBG)";
    String url="https://api.us2.sumologic.com/";

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getStartHour() {
        return startHour;
    }

    public void setStartHour(String startHour) {
        this.startHour = startHour;
        try {
            this.startTime=yMd.parse(startHour);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String getEndHour() {
        return endHour;

    }

    public void setEndHour(String endHour) {
        this.endHour = endHour;
        try {
            this.endTime=yMd.parse(endHour);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getExportPath() {
        return exportPath;
    }

    public void setExportPath(String exportPath) {
        this.exportPath = exportPath;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    String startHour;
    String endHour;
    int pageSize=100000;
    String exportPath="K:\\SUMO\\DEV1_510\\exports2";
    boolean compressed=true;
    boolean ignoreWhenExisted=true;
    SimpleDateFormat yMd=new SimpleDateFormat("yyyyMMdd_HH");
    SimpleDateFormat isosdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    java.util.Date startTime;
    java.util.Date endTime;

    public Date getBegin() {
        return begin;
    }

    public void setBegin(Date begin) {
        this.begin = begin;
    }

    public Date getFinish() {
        return finish;
    }

    public void setFinish(Date finish) {
        this.finish = finish;
    }

    java.util.Date begin;
    java.util.Date finish;
// "* | count _sourceHost",  // This query will return all messages
//         "2013-03-10T13:10:00",    // between this start time and
//         "2013-03-10T13:11:00",    // this end time, specified in ISO 8601 format
    public void exportByReceipttime(Date startTime,Date endTime)throws Exception{
        SumoLogicClient sumoClient=null;
        String searchJobId=null;
        ObjectMapper objectMapper=new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        OutputStreamWriter gzipOutputWriter =null;
        boolean firstExported=false;
        try{
            query= query+" |  where _receipttime>=parseDate(\""+yMd.format(startTime)+"\", \"yyyyMMdd_HH\",\"UTC\" ) and _receipttime < parseDate(\""+yMd.format(endTime)+"\", \"yyyyMMdd_HH\",\"UTC\")";
            Credentials credential = new Credentials(accessId, accessKey);
            sumoClient = new SumoLogicClient(credential);
            sumoClient.setURL(url);
            searchJobId = sumoClient.createSearchJob( query,isosdf.format(begin),isosdf.format(finish), "UTC");
            GetSearchJobStatusResponse getSearchJobStatusResponse = null;
            int messageCount = 0;
            System.out.println("S-E: "+isosdf.format(begin)+"-"+isosdf.format(finish));
            System.out.println("query: "+query);
            while (getSearchJobStatusResponse == null ||
                    (!getSearchJobStatusResponse.getState().equals("DONE GATHERING RESULTS") &&
                            !getSearchJobStatusResponse.getState().equals("CANCELLED"))) {

                // Sleep for a little bit, so we don't hammer
                // the Sumo Logic service.
                Thread.sleep(5000);

                // Get the latest search job status.
                getSearchJobStatusResponse = sumoClient.getSearchJobStatus(searchJobId);

                // Extract the message and record counts for
                // using them later down the road.
                messageCount = getSearchJobStatusResponse.getMessageCount();


                // Tell the user what's going on. Class
                // GetSearchJobStatusResponse has a nice toString()
                // implementation that will show the status and
                // the message and record counts.
                if(messageCount>0) {
                    System.out.printf(
                            "Search job ID: '%s', %s\n",
                            searchJobId,
                            getSearchJobStatusResponse);
                }
            }
            int messageToExport=messageCount;
//create file
            String fileName=yMd.format(endTime)+"#"+messageCount+"json.gz";
            File f=new File(this.exportPath+"/"+fileName);
            if(ignoreWhenExisted&&f.exists()){
                System.out.println("Already Expoerted!!");
                return;
            }
            System.out.println("Exporting to: "+f.getCanonicalPath());
            gzipOutputWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)));
            gzipOutputWriter.write("[");
            while(messageToExport>0){

                int ct=Math.min(pageSize,messageToExport);
                GetMessagesForSearchJobResponse getMessagesForSearchJobResponse =
                        sumoClient.getMessagesForSearchJob(searchJobId, messageCount-messageToExport, ct);

                try {
                    List<LogMessage> messages = getMessagesForSearchJobResponse.getMessages();
                    for (LogMessage message : messages) {
                        Map<String, String> fields = message.getMap();
                        List<String> fieldNames = new ArrayList<String>(message.getFieldNames());
                        Collections.sort(fieldNames);

                        String json = objectMapper.writeValueAsString(fields);
                        if(firstExported) {
                            gzipOutputWriter.write(",\n" + json);
                        }else{
                            gzipOutputWriter.write("\n" + json);
                        }
                    }
                    System.out.println("done!!\n");

//                    gzipOutputWriter.write(v.toString());
                }catch(Exception ex){
                    ex.printStackTrace();
                }finally{
                    try {
                        gzipOutputWriter.write("\n]");
                        gzipOutputWriter.flush();
                        gzipOutputWriter.close();
                    }catch(Exception ex){}
                    messageToExport=messageToExport>ct?(messageToExport-ct):0;
                }



            }

//close file
        } catch (Throwable t) {

            // Yikes. We has an error.
            t.printStackTrace();

        } finally {

            try {
                sumoClient.cancelSearchJob(searchJobId);

            } catch (Throwable t) {
                System.out.printf("Error cancelling search job: '%s'", t.getMessage());
                //t.printStackTrace();
            }
        }
    }

    public void exportByHour()throws Exception{
        java.util.Date starth=yMd.parse(getStartHour());
        Calendar endc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        endc.setTime(starth);
        while(true) {
            endc.add(Calendar.HOUR_OF_DAY, 1);
            Date endh = endc.getTime();
            if (endh.compareTo(endTime) <= 0) {
                System.out.println("Hourly Export: "+yMd.format(starth)+"~"+yMd.format(endh));
                exportByReceipttime(starth, endh);
            } else {
                break;
            }
            starth=endh;
        }
    }

    public String searchLastExportTime(String path){
        TreeSet<String> ts=new TreeSet<String>();

        for (File file : (new File(path).listFiles())) {
            if(file.getName().contains("_")){
                ts.add(file.getName().substring(0,11));
            }
        }

        if(ts.size()>0)return ts.last();
        else return null;
    }

    public static void main(String[] args)throws Exception{
        String st="20180430_00";
        String et="20180508_00";
        SumoExport sme=new SumoExport();

        Calendar now=Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Date finish=now.getTime();
        now.add(Calendar.DAY_OF_MONTH,-15);
        Date begin=now.getTime();

        sme.setBegin(begin);
        sme.setFinish(finish);

        sme.setStartHour(st);
        sme.setEndHour(et);

        sme.setExportPath("K:/SUMO/QA1_510/exports");
        sme.exportByHour();


    }
}
