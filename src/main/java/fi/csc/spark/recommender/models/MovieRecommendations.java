package fi.csc.spark.recommender.models;

import java.util.List;

import scala.Serializable;

public class MovieRecommendations implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer userId;
	private List<String> recommendations;
	
	
	public MovieRecommendations(Integer userId, List<String> recommendations) {
		this.userId = userId;
		this.recommendations = recommendations;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public List<String> getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(List<String> recommendations) {
		this.recommendations = recommendations;
	}

}
