package sumo.wifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sumologic.client.Credentials;
import com.sumologic.client.SumoLogicClient;
import com.sumologic.client.model.LogMessage;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetRecordsForSearchJobResponse;
import com.sumologic.client.searchjob.model.GetSearchJobStatusResponse;
import com.sumologic.client.searchjob.model.SearchJobRecord;

import java.text.SimpleDateFormat;
import java.util.*;

public class QueryAgent {
    String accessId = "sueijjowpajxEu";
    String accessKey = "ZMVwrxLJgw53YbFfewNlphmq3DSdxi9s8j7djTZgUkmm9fyj5OjyZTuPNcRGTR48";
    String query="_sourceCategory=dev1/device/unknown/* AND fw_version = 5.1.0* AND (proxyDisconnnected or proxyConnected or CTRL-EVENT or _sourceCategory=*/ConnectivityService or _sourceCategory=*/RTW or _sourceCategory=*/ifdu or _sourceCategory=*/init or suspend entry or NETWORK_DISCONNECTION_EVENT or RTW_DBG)";
    String url="https://api.us2.sumologic.com/";

    SimpleDateFormat isosdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    String start;
    String end;
    SumoLogicClient sumoClient;

    String searchJobId = null;
    ObjectMapper objectMapper = new ObjectMapper();


    String environment="prod";


    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }


    public void setStart(String start){
        this.start=start;

    }
    public void setEnd(String end){
        this.end=end;
    }

    public String getStart(){
        return this.start;

    }
    public String getEnd(){
        return this.end;
    }

    public void init()throws Exception{
        Credentials credential = new Credentials(accessId, accessKey);
        sumoClient = new SumoLogicClient(credential);
        sumoClient.setURL(url);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public synchronized TreeMap<String, Date> queryDisconnectionCountOverFirmwareVersionBySourcename(String sourcename)throws Exception{
        TreeMap result = new TreeMap<String, Date>();
        String searchJobId = null;
        try {
            String query = "_sourceName="+sourcename+" and (_sourceCategory="+environment+"/device/*/restsdk and \"wifi disconnected\") | count by fw_version | sort by fw_version asc";
            searchJobId = sumoClient.createSearchJob(query, start, end, "UTC");
            GetSearchJobStatusResponse getSearchJobStatusResponse = null;
            int messageCount = 0;
            System.out.println("S-E: " + start + "-" + end);
            System.out.println("query: " + query);
            while (getSearchJobStatusResponse == null ||
                    (!getSearchJobStatusResponse.getState().equals("DONE GATHERING RESULTS") &&
                            !getSearchJobStatusResponse.getState().equals("CANCELLED"))) {

                // Sleep for a little bit, so we don't hammer
                // the Sumo Logic service.
                Thread.sleep(1000);

                // Get the latest search job status.
                getSearchJobStatusResponse = sumoClient.getSearchJobStatus(searchJobId);

                // Extract the message and record counts for
                // using them later down the road.
                messageCount = getSearchJobStatusResponse.getMessageCount();


                // Tell the user what's going on. Class
                // GetSearchJobStatusResponse has a nice toString()
                // implementation that will show the status and
                // the message and record counts.
                if (messageCount > 0) {
                    System.out.printf(
                            "Search job ID: '%s', %s\n",
                            searchJobId,
                            getSearchJobStatusResponse);
                }
            }

            //start to export the result
            int messageToExport=messageCount;
            GetMessagesForSearchJobResponse getMessagesForSearchJobResponse = sumoClient.getMessagesForSearchJob(searchJobId, 0, messageCount);
            GetRecordsForSearchJobResponse records=sumoClient.getRecordsForSearchJob(searchJobId, 0, messageCount);

            try {

                List<SearchJobRecord> messages = records.getRecords();
                for (SearchJobRecord message : messages) {
                    Map<String, String> fields = message.getMap();
                    List<String> fieldNames = new ArrayList<String>(message.getFieldNames());
                    Collections.sort(fieldNames);

                    String json = objectMapper.writeValueAsString(fields);
                    System.out.println(json);
                }
                System.out.println("done!!\n");

            } catch (Exception ex) {
                ex.printStackTrace();
            }


        } catch (Throwable t) {

            // Yikes. We has an error.
            t.printStackTrace();

        } finally {

            try {
                sumoClient.cancelSearchJob(searchJobId);

            } catch (Throwable t) {
                //System.out.printf("Error cancelling search job: '%s'", t.getMessage());
                //t.printStackTrace();
            }
        }



        return result;
    }

    public synchronized TreeMap<String, Date> queryFirwareVersionBeginBySourcename(String sourcename) throws Exception {
        TreeMap result = new TreeMap<String, Date>();
        String searchJobId = null;
        try {
            String query = "_sourceName=" + sourcename + " \"suspend entry\" | first(_messagetime) group by fw_version | formatDate(_first,\"yyyyMMdd'T'HH:mm:ss'Z'\",\"UTC\") as start | sort by fw_version asc";
            searchJobId = sumoClient.createSearchJob(query, start, end, "UTC");
            GetSearchJobStatusResponse getSearchJobStatusResponse = null;
            int messageCount = 0;
            System.out.println("S-E: " + start + "-" + end);
            System.out.println("query: " + query);
            while (getSearchJobStatusResponse == null ||
                    (!getSearchJobStatusResponse.getState().equals("DONE GATHERING RESULTS") &&
                            !getSearchJobStatusResponse.getState().equals("CANCELLED"))) {

                // Sleep for a little bit, so we don't hammer
                // the Sumo Logic service.
                Thread.sleep(1000);

                // Get the latest search job status.
                getSearchJobStatusResponse = sumoClient.getSearchJobStatus(searchJobId);

                // Extract the message and record counts for
                // using them later down the road.
                messageCount = getSearchJobStatusResponse.getMessageCount();


                // Tell the user what's going on. Class
                // GetSearchJobStatusResponse has a nice toString()
                // implementation that will show the status and
                // the message and record counts.
                if (messageCount > 0) {
                    System.out.printf(
                            "Search job ID: '%s', %s\n",
                            searchJobId,
                            getSearchJobStatusResponse);
                }
            }

            //start to export the result
            int messageToExport=messageCount;
            GetMessagesForSearchJobResponse getMessagesForSearchJobResponse = sumoClient.getMessagesForSearchJob(searchJobId, 0, messageCount);
            GetRecordsForSearchJobResponse records=sumoClient.getRecordsForSearchJob(searchJobId, 0, messageCount);

            try {

                List<SearchJobRecord> messages = records.getRecords();
                for (SearchJobRecord message : messages) {
                    Map<String, String> fields = message.getMap();
                    List<String> fieldNames = new ArrayList<String>(message.getFieldNames());
                    Collections.sort(fieldNames);

                    String json = objectMapper.writeValueAsString(fields);
                    System.out.println(json);
                }
                System.out.println("done!!\n");

            } catch (Exception ex) {
                ex.printStackTrace();
            }


        } catch (Throwable t) {

            // Yikes. We has an error.
            t.printStackTrace();

        } finally {

            try {
                sumoClient.cancelSearchJob(searchJobId);

            } catch (Throwable t) {
                //System.out.printf("Error cancelling search job: '%s'", t.getMessage());
                //t.printStackTrace();
            }
        }


        return result;
    }



    public static void main(String[] args)throws Exception{
        QueryAgent qa=new QueryAgent();
        qa.init();
        qa.setStart("2018-04-03T00:00:00Z");
        qa.setEnd("2018-05-30T00:00:00Z");
        qa.queryFirwareVersionBeginBySourcename("00:14:ee:0c:6c:df");
        qa.queryDisconnectionCountOverFirmwareVersionBySourcename("00:14:ee:0c:6c:df");


    }

}
