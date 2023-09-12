package uk.ac.gla.dcs.bigdata.studentstructures;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProcessedArticle implements Serializable {

	private static final long serialVersionUID = 7860293794078412243L;
	
	String id; // unique article identifier
	String title; // article title
	ArrayList<String> processedcontents; // the contents of the article body
	
	public ProcessedArticle() {}
	
	public ProcessedArticle(String id, String title,ArrayList<String> processedcontents) {
		super();
		this.id = id;
		this.title = title;
		this.processedcontents = processedcontents;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public ArrayList<String> getContents() {
		return processedcontents;
	}

	public void setContents(ArrayList<String> processedcontents) {
		this.processedcontents = processedcontents;
	}

	
	
}
