package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleList;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoredArticles;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ArticleListMap implements MapFunction<ScoredArticles, ArticleList>, Serializable {

    private long documentCount;
    private double averageDocumentLength;
    private int sumTermFrequency;
    
    public ArticleListMap(long documentCount, double averageDocumentLength, int sumTermFrequency) {
        this.documentCount = documentCount;
        this.averageDocumentLength = averageDocumentLength;
        this.sumTermFrequency = sumTermFrequency;
    }

    @Override
    public ArticleList call(ScoredArticles newsArticle) throws Exception {
    	short termFrequencyInCurrentDocument= newsArticle.getTermFrequencyList();
    	int currentDocumentLength=newsArticle.getDocumentLength();
    	double dphScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, sumTermFrequency, currentDocumentLength, averageDocumentLength, documentCount);
        return new ArticleList(newsArticle.getId(),newsArticle.getTitle(), dphScore);
    }

    

	
}
