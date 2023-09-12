package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.ScoredArticles;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleList;

 /**
	 * 
	 */
/*MAP RANKED RESULT */

public class MapRR implements ReduceFunction<ArticleList> {
	
	public List<ArticleList> call(Dataset<ArticleList> scored_list) throws Exception {  //id,score,title

		List<ArticleList> topDocs = (List<ArticleList>) scored_list ;
		topDocs=topDocs.subList(0, Math.min(topDocs.size(), 20));
		List<ArticleList> finalList = new ArrayList<>();
		Double titleDistance = 0.0;
		
//		scored_list.withColumnRenamed("id", "primary_id").join(scored_list.withColumnRenamed("id", "secondary_id"), on=["primary_id<secondary_id"], how="outer");
		
		for(int i = 0; i < 20; i++) {        //i 1to 20					
					for(int j = 0; j<=i; j++) {      //j 0 to i						
						if (!topDocs.get(i).getTitle().isEmpty() && !topDocs.get(j).getTitle().isEmpty()) {
							if(i==0)
							{
								finalList.add(topDocs.get(i));
							}
							titleDistance = TextDistanceCalculator.similarity(topDocs.get(i).getTitle(), topDocs.get(j).getTitle());
							if(titleDistance<=0.5) {
								finalList.add(topDocs.get(j));
							}
							
						}
						else {
							continue;
						}
					}
				}
		
		finalList = finalList.subList(0, Math.min(finalList.size(), 20));
		System.out.println(finalList);
		return  finalList;
	}


	@Override
	public ArticleList call(ArticleList v1, ArticleList v2) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	

}