package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleList;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoredArticles;
import org.apache.spark.api.java.function.FilterFunction;
import static org.apache.spark.sql.functions.desc;


public class DPHCalculate {

    public static Dataset<ArticleList> calculateDPHScore(SparkSession spark, List<String> queryTerms, Dataset<ProcessedArticle> processedArticles) {

   
        Broadcast<List<String>> queryTermsBroadcast = spark.sparkContext().broadcast(queryTerms, scala.reflect.ClassTag$.MODULE$.apply(List.class));

        //total of the TermFrequency, the document length and the average document length are calculated here:
        DPHScoreMap dphScoreMap = new DPHScoreMap(queryTermsBroadcast);
        Dataset<ScoredArticles> scoredArticlesDataset = processedArticles.map(dphScoreMap,Encoders.bean(ScoredArticles.class));
        

        scoredArticlesDataset.show();
        ScoredArticles reducerResult = scoredArticlesDataset.reduce(new DPHScoreReducer());
               
		int sumTermFrequency = reducerResult.getTermFrequencyList();
        int sumDocumentLength = reducerResult.getDocumentLength();
        long sumDocumentCount = reducerResult.getDocumentCount();
        Double averageDocumentLength = (double) sumDocumentLength / (double) sumDocumentCount;
//        System.out.println(sumTermFrequency);
//        System.out.println(averageDocumentLength);
//        System.out.println(sumDocumentCount);
        ArticleListMap newsArticleListMap = new ArticleListMap(sumDocumentCount, averageDocumentLength, sumTermFrequency);
        scoredArticlesDataset = scoredArticlesDataset.filter((FilterFunction<ScoredArticles>) scoredArticle -> scoredArticle.getTermFrequencyList() > 0);
        Dataset<ArticleList> scoredList= scoredArticlesDataset.map(newsArticleListMap,Encoders.bean(ArticleList.class));
        
        // the result will be mapped through:
        return scoredList.orderBy(desc("score"));
    }

}