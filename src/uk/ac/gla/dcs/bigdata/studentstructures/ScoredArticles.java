package uk.ac.gla.dcs.bigdata.studentstructures;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

public class ScoredArticles implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id; // unique article identifier
    private ArrayList<String> processedcontent; // origin processedcontent
    private String title;
    private int titleLength; //record the title position in the processedcontent
    private short termFrequencyList; // (count) of the term in the document
    private int documentLength; // The length of the document (in processedcontent)
    private Long documentCount; // count of document

    public ScoredArticles(String id, ArrayList<String> processedcontent, String title, int titleLength, short termFrequencyList, int documentLength) {
        this.id = id;
        this.processedcontent = processedcontent;
        this.title=title;
        this.titleLength = titleLength;
        this.termFrequencyList = termFrequencyList;
        this.documentLength = documentLength;
        this.documentCount = 1L;
    }

    public ScoredArticles(String id, ArrayList<String> processedcontent, String title, int titleLength, short termFrequencyList, int documentLength, Long documentCount) {
        this.id = id;
        this.processedcontent = processedcontent;
        this.title=title;
        this.titleLength = titleLength;
        this.termFrequencyList = termFrequencyList;
        this.documentLength = documentLength;
        this.documentCount = documentCount;
    }

    public ScoredArticles() {
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
    public void setId(String id) {
        this.id = id;
    }

    public List<String> getprocessedcontent() {
        return processedcontent;
    }

    public void setprocessedcontent(ArrayList<String> processedcontent) {
        this.processedcontent = processedcontent;
    }

    public Long getDocumentCount() {
        return documentCount;
    }

    public void setDocumentCount(Long documentCount) {
        this.documentCount = documentCount;
    }

    public short getTermFrequencyList() {
        return termFrequencyList;
    }

    public void setTermFrequencyList(short termFrequencyList) {
        this.termFrequencyList = termFrequencyList;
    }

    public int getDocumentLength() {
        return documentLength;
    }

    public void setDocumentLength(int documentLength) {
        this.documentLength = documentLength;
    }

    public int getTitleLength() {
        return titleLength;
    }

    public void setTitleLength(int titleLength) {
        this.titleLength = titleLength;
    }
}