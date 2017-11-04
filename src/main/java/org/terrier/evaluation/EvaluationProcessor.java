package org.terrier.evaluation;
/** Interface for evaluating a set of document scores within a feature learning environment.
  * Usage:
  * <pre>
  * FeatureLoader floader = ?;
  * EvaluationProcessor eproc = new DefaultEvaluationProcessorFactory().getEvaluationProcessor();
  * eproc.setRelevanceInformation("/path/to/qrels");
  * //OR
  * eproc.setRelevanceInformation(floader.getQuerynos(), floader.getDocnos(), floader.getRelevance();
  * double[][] docscores = new double[floader.getQuerynos().length][floader.getDocnos()];
  * //populate docscores
  * eproc.process(floader.getQuerynos(), floader.getDocnos(), docscores, 1000);
  * System.out.println("MAP=" + eproc.getMeasureMean(measure);
  * </pre>
  * @author Craig Macdonald
  */
public interface EvaluationProcessor {

	
	public void setRelevanceInformation(String relevanceFile) throws Exception;
	public void setRelevanceInformation(String[] queryid, String[][] docno, byte[][] relevanceLabels);
	
	public void process(String[] queryid, String[][] docno,
			double[][] documentScores, int maxRank) throws Exception;
	public float getMeasureMean(String measure);
	public float[] getMeasure(String measure);
}