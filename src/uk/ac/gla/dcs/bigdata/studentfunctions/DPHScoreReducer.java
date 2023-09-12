package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHScoreMap;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoredArticles;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class DPHScoreReducer implements ReduceFunction<ScoredArticles> {
	private static final long serialVersionUID = 1L;

	public ScoredArticles call(ScoredArticles d1, ScoredArticles d2) throws Exception {
		short sumTermFrequency = (short) (d1.getTermFrequencyList() + d2.getTermFrequencyList());
		int sumDocumentLength = d1.getDocumentLength() + d2.getDocumentLength();
		long sumDocumentCount = d1.getDocumentCount() + d2.getDocumentCount();
		
		return new ScoredArticles("", null, "", 0, sumTermFrequency, sumDocumentLength, sumDocumentCount);
	}
}