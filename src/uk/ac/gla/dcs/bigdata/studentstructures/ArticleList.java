package uk.ac.gla.dcs.bigdata.studentstructures;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ArticleList implements Serializable {

	//private static final long serialVersionUID = 7860293794078412243L;
	
	String id; // unique article identifier
	String title;
	double dph;
	
	public ArticleList() {}
	
	public ArticleList(String id,String title,double dph) {
		super();
		this.id = id;
		this.title=title;
		this.dph=dph;
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

	public double getScore() {
		return dph;
	}

	public void setScore(double dph) {
		this.dph = dph;
	}
	
	 @Override
	 public boolean equals(Object o) {
	        if (this == o) return true;
	        if (!(o instanceof ArticleList)) return false;
	        ArticleList article = (ArticleList) o;
	        return getId() == article.getId();
	    }

	 @Override
	 public int hashCode() {
	        return Objects.hash(getId());
	    }
}
