package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoredArticles;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class DPHScoreMap implements MapFunction<ProcessedArticle, ScoredArticles> {
    private static final long serialVersionUID = 1L;
    private final List<String> queryTerms;
    
    public DPHScoreMap(Broadcast<List<String>> queryTerms) {
        this.queryTerms = queryTerms.value();
    }
    
    @Override
    public ScoredArticles call(ProcessedArticle processedArticle) throws Exception {
        List<String> documentTerms = processedArticle.getContents();
        
        int titleLength;
        
        if(processedArticle.getTitle() != null) {
        	titleLength=processedArticle.getTitle().length();
        }
        else {
        	titleLength=0;
        }
        // Calculate TermFrequency (count of the term in the document)
        short termCount = 0;
        for (int i = 0; i < queryTerms.size(); i++) {
            String term = queryTerms.get(i);
            
            for (int j = 0; j < documentTerms.size(); j++) {
                if (documentTerms.get(j).equals(term)) {
                    termCount++;
                }
            }
        }

        // The length of the document (in terms)
        int documentLength = documentTerms.size()+titleLength;
        
        return new ScoredArticles(processedArticle.getId(),processedArticle.getContents(),processedArticle.getTitle(),titleLength,termCount,documentLength);
    }
}
