package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHCalculate;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHScoreMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.MapRR;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsPreProcessorMap;
//import uk.ac.gla.dcs.bigdata.studentfunctions.RankedResultMap;
//import uk.ac.gla.dcs.bigdata.studentfunctions.TextualDistanceReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleList;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import static org.apache.spark.sql.functions.*;
/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
//		String sparkMasterDef = System.getenv("spark.local");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
	}
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		long startTime = System.currentTimeMillis();//time counter
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
//		System.out.println("count is "+newsjson.count());
		Dataset<Row> topTenNewsjson = newsjson.limit(50911);
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = topTenNewsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		
		
		// Apply NewsPreProcessorMap function to newsArticles dataset
	    Dataset<ProcessedArticle> processedArticles = news.flatMap(new NewsPreProcessorMap(), Encoders.bean(ProcessedArticle.class));   
	    System.out.println("Count 1 : " + news.count());
	    System.out.println("Count 2 : " + processedArticles.count());
	    
		List<DocumentRanking> docRankList = new ArrayList<>();
		// For each query find the ranked list
		for (Query query:queries.collectAsList()) {
			//Calculate DPHScore for each document
			Dataset<ArticleList> newsAsLists = DPHCalculate.calculateDPHScore(spark,query.getQueryTerms(),processedArticles);
			newsAsLists.show();
			
			//Taking the top 20 documents from a descending sorted dataset
			Dataset<ArticleList> nList =  newsAsLists.limit(20);
			
			//Converting dataset to list
			List<ArticleList> topDocs= new ArrayList<ArticleList>();
			topDocs = nList
					  .select(col("id"), col("score"), col("title"))
					  .as(Encoders.bean(ArticleList.class))
					  .collectAsList();
			
			List<ArticleList> finalList= new ArrayList<ArticleList>();
			//Check for similarity and create final article list with non similar articles
			for(int i = 0; i < 20; i++) {        //i 1to 20					
				for(int j = 0; j<=i; j++) {      //j 0 to i						
					if (topDocs.get(i).getTitle()!=null && topDocs.get(j).getTitle()!=null && i!=j) {
						if(i==0)
						{
							finalList.add(new ArticleList(topDocs.get(i).getId(),topDocs.get(i).getTitle(),topDocs.get(i).getScore()));
						}
						double titleDistance = TextDistanceCalculator.similarity(topDocs.get(i).getTitle(), topDocs.get(j).getTitle());
						if(titleDistance>=0.5) {
							finalList.add(new ArticleList(topDocs.get(j).getId(),topDocs.get(j).getTitle(),topDocs.get(j).getScore()));
						}
						
					}
					else {
						continue;
					}
				}
			}
			
			//Ensure all values in list are distinct
			List<ArticleList> distinctArticles = finalList.stream()
                    .distinct()
                    .collect(Collectors.toList());
			//Get top ten values from the list
			List<ArticleList> result = distinctArticles.subList(0, Math.min(finalList.size(), 10));
			//Add result for query to Document Ranking List
			docRankList.add(new DocumentRanking(query,result));
		}
//
		//log the computation
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		System.out.println("total_time:"+timeElapsed+"(millisecond)"+"\n"+"docRankList:"+"\n"+docRankList);

		return docRankList; 
	  //----------------------------------------------------------------
	    //return null;
	}
	
	
}