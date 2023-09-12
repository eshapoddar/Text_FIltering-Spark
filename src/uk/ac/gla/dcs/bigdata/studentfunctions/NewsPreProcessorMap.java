package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

//public class NewsPreProcessorMap implements MapFunction<NewsArticle, ProcessedArticle>, Serializable {
public class NewsPreProcessorMap implements FlatMapFunction<NewsArticle,ProcessedArticle>, Serializable {
    private static TextPreProcessor preProcessor;

    public NewsPreProcessorMap() {
        preProcessor = new TextPreProcessor();
    }
    
    @Override
    public Iterator<ProcessedArticle> call(NewsArticle article) throws Exception {
        ArrayList<String> processedTokens = new ArrayList<String>();
        
        List<ProcessedArticle> pa  = new ArrayList<ProcessedArticle>();
        List<ProcessedArticle> pa_empty  = new ArrayList<ProcessedArticle>(0);
        int counter = 0;
        String s = "";
        if (article == null) return pa_empty.iterator();
        
        if (article.getTitle() == null ) return pa_empty.iterator();
        
	        for (ContentItem content : article.getContents()) {
	        	if (content == null) continue;
	        	
	        	if (content.getContent() != null && content.getSubtype() != null){
	        		if (content.getSubtype().equals("paragraph")){
		        		counter = counter + 1;
		        		
//		        		check if counter equals 1, then add title
		        		
		        		if (counter < 5) {
		        			s= s + " " +content.getContent();
		                    //List<String> tokens = preProcessor.process(content.getContent());
		                    //processedTokens.addAll(tokens);
		                }
		        		else break;
	        		}
	        	}
	        }
	     
	     if (counter == 0) return pa_empty.iterator();
	     ArrayList<String> tokens = (ArrayList<String>) preProcessor.process(s);
	     
	     //System.out.println(counter+ "  "+ tokens.size());
	     pa.add(new ProcessedArticle(article.getId(),article.getTitle(),tokens));
	     return pa.iterator();
    }
}
