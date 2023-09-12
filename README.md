# BigData


**Custom Functions 
**
**NewsPreProcessorMap:** The code defines a class NewsPreProcessorMap that implements the MapFunction interface. The purpose of this class is to preprocess news articles to extract important text tokens.
The call() method takes a NewsArticle object and returns a ProcessedArticle object. It first initialises a TextPreProcessor object. Then, it loops through each ContentItem in the NewsArticle object and extracts the text content. It then uses the TextPreProcessor object to preprocess the content and extract tokens. The resulting tokens are added to an ArrayList called processedTokens. Finally, a new ProcessedArticle object is returned, which includes the NewsArticle ID, title, and the processed tokens as a list.
The class is serializable, which means that it can be easily distributed across a Spark cluster. 

**DPHCalculate:** The class includes a static method called calculateDPHScore that accepts as inputs a SparkSession object, a list of query words, and a dataset of processed articles and returns a dataset of ArticleList objects.

Using the Broadcast object, the calculateDPHScore method first broadcasts the list of query words to all worker nodes. It then uses the DPHScoreMap object to calculate the DPH score for each article and applies the map method to transfer the processed articles dataset to the dataset of ScoredArticles. Each processed article is given the DPHScoreMap function, and the map method uses the Encoders.bean method to encode the output as a ScoredArticles object.

**DPHScoreMap:** This class MapFunction interface by the call method, which accepts a ProcessedArticle object as input and outputs a ScoredArticles object. 
We use this class to accept the processed articles, and then compute some values, like titleLength,termCount and documentLength, which are returned with the ScoredArticle object in order to compute the DPH score for  documents in the queries. First we check if the title field is not empty, and if that’s the case, we get the title length for that, otherwise it’ll be 0, then we calculate the term frequency by traversing through the document terms and adding the count for every relevant term, lastly we calculate the document length by simply using the size().
Now, we return the new ScoredArticles object with article id, content, title, title length, term count and document length.

**DPHScoreReducer:**  This class implements the ReduceFunction which accepts a ScoredArticle class object. In order to compute the total term frequency, total document length and total document count for two ScoredArticles objects,we use the call function by passing those two ScoredArticles objects and simply add those respective values of each object. Then we return a new ScoredArticles object which contains total term frequency, total document length and total document count for the two parameters.

**ArticleListMap:**  This class  implements MapFunction which is accepting ScoredArticles, ArticleList and Serializable class objects. It has private instances documentCount, averageDocumentLength, sumTermFrequency. When called using the ScoredArticles object, it returns the respective id, title and dph score.

**ArticleList:** This class implements Serializable, and has 3 instances- id, title and dph. It is a structure and just assigns corresponding values to the calling variable based on the constructor type called. The dataset returned is used in final rankings and filtering top 10 documents as it contains the DPH score.

**ProcessedArticle:**  Just like ArticleList, ProcessedArticle is a class implementing Serializable as well, and has three instances - string id, title and an arraylist containing processed content of the article body. Based on the constructor type called, it assigns values to the calling object. It was used to calculate DPH score using the processed terms.

**ScoredArticles:** Like the above two classes, this class as well contains the structure of ScoredArticles object type. It has the following instances - string id, arraylist processedcontent, string title, termfrequencylist, documentLength and document count.
Depending on the type of constructor calling, it assigns values to the calling ScoredArticles type object. We used this along with ProcessedArticles in order to calculate DPH score and similarity as well.

