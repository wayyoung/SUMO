package sumo.analysis.servlet;
import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
import org.bson.Document;
import sumo.analysis.SumoMongo;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;

import static com.mongodb.client.model.Aggregates.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Updates.*;
import static com.mongodb.client.model.Projections.*;



@WebServlet(urlPatterns={"/queryProxyConnection"},name="servlet")
public class QueryProxyConnection extends HttpServlet {
    SumoMongo sgo=new SumoMongo();
    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        sgo.init();

    }

    class OutCount{
        boolean firstOut=false;
        int connectionIndex=0;
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String s = req.getParameter("sourcename");
        Calendar now=Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        java.util.Date edate=now.getTime();
        now.add(Calendar.DATE,-7);
        java.util.Date sdate=now.getTime();
        resp.setContentType("application/json");
        resp.setHeader("Access-Control-Allow-Origin", "*");

        resp.getOutputStream().print("[");
        final OutCount outCount=new OutCount();
        if(s!=null){

            sgo.getColletionBySourcename(s).aggregate(Arrays.asList(
                    match(and(exists("connectionIndex"),gte("mtime",sdate),lt("mtime",edate))),
                    sort(orderBy(ascending("mtime"),descending("messageid"))),
                    project(fields(include("mtime"),include("connectionIndex")))
            )).forEach(new Block<Document>() {
                @Override
                public void apply(final Document doc) {
                    try {
                        doc.append("dt",sdf.format(doc.getDate("mtime")));
                        if(outCount.connectionIndex==0){
//                            doc.append("connected",0);
//                            if(!outCount.firstOut){
//                                resp.getOutputStream().print("\n"+doc.toJson());
//                                outCount.firstOut=true;
//                            }else{
//                                resp.getOutputStream().print(",\n"+doc.toJson());
//                            }
                            doc.append("connected",1);
                            outCount.connectionIndex=doc.getInteger("connectionIndex");
                        }else{
                            if(outCount.connectionIndex!=doc.getInteger("connectionIndex")){
                                System.out.println("ERROR!!!\n");
                            }



                            doc.append("connected",0);
                            outCount.connectionIndex=0;
                        }
                        if(!outCount.firstOut){
                            resp.getOutputStream().print("\n"+doc.toJson());
                            outCount.firstOut=true;
                        }else{
                            resp.getOutputStream().print(",\n"+doc.toJson());
                        }


                    }catch(Exception ex){
                        ex.printStackTrace();
                    }
                }
            } );



        }
        resp.getOutputStream().print("\n]");
        resp.getOutputStream().flush();



        //req.getRequestDispatcher("hello.jsp").forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        doGet(req,resp);
    }
}
