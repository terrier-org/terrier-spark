package org.terrier.evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import gnu.trove.TFloatArrayList;
import gnu.trove.TObjectIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.lemurproject.ireval.IREval;
import org.lemurproject.ireval.RetrievalEvaluator;
import org.lemurproject.ireval.RetrievalEvaluator.Document;
import org.lemurproject.ireval.RetrievalEvaluator.Judgment;
import org.lemurproject.ireval.SetRetrievalEvaluator;
import org.terrier.utility.ApplicationSetup;

/** EvaluationProcessor based on ireval.jar which comes from Indri. In particular, this
  * evaluator can function wholly within memory, without writing results out to disk
  * @author Craig Macdonald
  */
public class InMemoryEvaluationProcessor //implements EvaluationProcessor
{
	final static Logger logger = Logger.getLogger(InMemoryEvaluationProcessor.class);
	protected TreeMap< String, ArrayList<Judgment> > allJudgments = null;
	protected Map<String, RetrievalEvaluator> allQueryEvaluators = null;
	protected SetRetrievalEvaluator setEvaluator = null;
	protected java.util.Comparator<Document> trecEvalDocumentComparator = new TrecEvalDocumentComparator();
	protected final TObjectIntHashMap<String> query2index = new TObjectIntHashMap<String>();
	
	public InMemoryEvaluationProcessor() throws Exception
	{
		this(ApplicationSetup.getProperty("opt.qrels", null));
	}
	
	public InMemoryEvaluationProcessor(String qrelsFile) throws Exception
	{
		if (qrelsFile != null)
		{
			allJudgments = IREval.loadJudgments( qrelsFile );
			buildRetrievalEvaluators();
		}
	}
	
	public InMemoryEvaluationProcessor(TreeMap<String, ArrayList<Judgment>> _allJudgments)
	{
		allJudgments = _allJudgments;
		if (allJudgments != null)
			buildRetrievalEvaluators();
	}
	
	protected void buildRetrievalEvaluators()
	{
		allQueryEvaluators = new HashMap<String, RetrievalEvaluator>(allJudgments.size());
		final boolean regen_query2index = query2index.size() == 0;
		int i=0;
		for(Map.Entry<String, ArrayList<Judgment>> rels : allJudgments.entrySet())
		{
			allQueryEvaluators.put(rels.getKey(), new RetrievalEvaluator(rels.getKey(), rels.getValue()));
			if (regen_query2index)
				query2index.put(rels.getKey(), i++);
		}
	}
	
	protected Map<String, RetrievalEvaluator> getAllQueryEvaluators() {
		return allQueryEvaluators;
	}

	//@Override
	public void setRelevanceInformation(String qrelsFile)
			throws Exception 
	{
		query2index.clear();
		allJudgments = IREval.loadJudgments( qrelsFile );
		buildRetrievalEvaluators();
	}

	//@Override
	public void setRelevanceInformation(
		String[] queryid, String[][] docno,
		byte[][] relevanceLabels) 
	{
		final TreeMap< String, ArrayList<Judgment> > judgments = new TreeMap< String, ArrayList<Judgment> >();
		final int numQueries = queryid.length;
		query2index.clear();
		for(int i=0;i<numQueries;i++)
		{
			final ArrayList<Judgment> recentJudgments = new ArrayList<Judgment>();
			final int numDocByQuery = docno[i].length;
			for(int j=0;j<numDocByQuery;j++)
			{
				recentJudgments.add(new Judgment(docno[i][j], (int)relevanceLabels[i][j]));
			}
			judgments.put(queryid[i], recentJudgments);
			query2index.put(queryid[i], i);
			//System.err.println(queryid[i] + "->" + i);
		}
		allJudgments = judgments;
		buildRetrievalEvaluators();
	}
	
	
	//@Override
	public void process(String[] queryid, String[][] docno,
			double[][] documentScores, int maxRank) throws Exception 
	{
		if (allJudgments == null)
		{
			throw new IllegalStateException("No qrels file found (property opt.qrels), and no relevance information provided");
		}
		assert query2index.size() > 0;
		
		TreeMap< String, ArrayList<Document> > allRankings = new TreeMap< String, ArrayList<Document> >();
		for(int j=0;j<queryid.length;j++)
		{
			ArrayList<Document> recentRanking = new ArrayList<Document>();
			allRankings.put( queryid[j], recentRanking);
			for(int i=0;i<documentScores[j].length;i++)
			{
				if (docno[j][i] == null)
					break;
				recentRanking.add(new Document( docno[j][i], i+1, documentScores[j][i]));
			}
		}
		 // Map query numbers into Integer to get proper sorting.
        TreeMap< String, RetrievalEvaluator > evaluators = new TreeMap<String, RetrievalEvaluator>(new java.util.Comparator<String>() {
                public int compare(String a, String b) {
                	return query2index.get(a) - query2index.get(b);
                }
        });
        
        for( String query : allRankings.keySet() ) {
        	 final RetrievalEvaluator evaluator = allQueryEvaluators.get(query);
             if (evaluator == null)
             {
             	if (logger.isDebugEnabled())
             		logger.debug("No judgements for query " + query + ", skipping");
             	continue;
             }
             List<Document> ranking = allRankings.get( query );

            /* resort ranking on score, renumber ranks */
            java.util.Collections.sort(ranking, new TrecEvalDocumentComparator());
            int i = 1;
            for (Document d : ranking) {
                d.rank = i++;
            }
            if (maxRank > 0 && ranking.size() > maxRank)
            	ranking = ranking.subList(0, maxRank -1);

            //logger.info("Evaluating query " + query);
            evaluator.evaluate(ranking);
            evaluators.put( query, evaluator );
        }
        if (logger.isDebugEnabled())
        	logger.debug("Evaluated " + evaluators.size() + " queries");
        setEvaluator = new SetRetrievalEvaluator( evaluators.values() );	
        if(logger.isDebugEnabled())
        {
        	logger.debug(IREval.singleEvaluation(setEvaluator, true));
        }
	}

	//@Override
	public float[] getMeasure(String measure) {		
		final TFloatArrayList rtr = new TFloatArrayList();
		if (measure.equals("map"))
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.averagePrecision());
		else if (measure.equals("ndcg"))
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.normalizedDiscountedCumulativeGain());
		else if (measure.equals("recip_rank") || measure.equals("mrr") )
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.reciprocalRank());
		else if (measure.equals("r-Prec"))
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.rPrecision());
		else if (measure.equals("bpref"))
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.binaryPreference());	
//		else if (measure.equals("err"))
//			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
//				rtr.add((float) re.expectedReciprocalRank());
		else if (measure.startsWith("P_"))
			for(RetrievalEvaluator re : setEvaluator.getEvaluators())
				rtr.add((float) re.precision(Integer.parseInt(measure.replaceFirst("P_", "")))); 
		else throw new IllegalArgumentException("Measure "+measure+" is not supported");
		return rtr.toNativeArray();
	}

	//@Override
	public float getMeasureMean(String measure) {
		if (measure.equals("map"))
			return (float)setEvaluator.meanAveragePrecision();
		if (measure.equals("ndcg"))
			return (float)setEvaluator.meanNormalizedDiscountedCumulativeGain();
		if (measure.equals("recip_rank") || measure.equals("mrr") )
			return (float)setEvaluator.meanReciprocalRank();
		if (measure.equals("r-Prec"))
			return (float)setEvaluator.meanRPrecision();
		if (measure.equals("bpref"))
			return (float)setEvaluator.meanBinaryPreference();
//		if (measure.equals("err"))
//			return (float)setEvaluator.meanExpectedReciprocalRank();
		if (measure.startsWith("P_"))
			return (float)setEvaluator.meanPrecision(Integer.parseInt(measure.replaceFirst("P_", "")));
		throw new IllegalArgumentException("Measure "+measure+" is not supported");
	}
	
//	protected java.util.Comparator<Document> getTrecEvalDocumentComparator()
//	{
//		return new TrecEvalDocumentComparator();
//	}

	/** Sorter: sort by score descending, then by docno descending */
	protected static final class TrecEvalDocumentComparator implements
			java.util.Comparator<Document> {
		public int compare(final Document a, final Document b)
		{
		    int cmp = Double.compare(b.score, a.score);
		    if (cmp != 0)
		    	return cmp;
//			if (a.score < b.score) return 1;
//			if (b.score < a.score) return -1;
//		    //if (a.score > b.score) return -1;
		    return (b.documentNumber.compareTo(a.documentNumber));
		}
	}

	public static class TestComparator
	{
		//proper ranking is : a,c,b
		
		final Document a = new Document("doc1", 3, 2.0d);
		final Document b = new Document("doc2", 3, 1.0d);
		final Document c = new Document("doc3", 3, 1.0d);
		
		final Document dP = new Document("doc1", 3, -0d);
		final Document dN = new Document("doc1", 3, 0d);
		
		
		final TrecEvalDocumentComparator cmp = new TrecEvalDocumentComparator();
		
		protected void checksort(Document d1, Document d2, Document d3) {
			List<Document> list = Arrays.asList(d1, d2, d3);
			Collections.sort(list, cmp);
			assertEquals("doc1", list.get(0).documentNumber);
			assertEquals("doc3", list.get(1).documentNumber);
			assertEquals("doc2", list.get(2).documentNumber);			
		}
		
		
				
		@Test public void doSort() {
			checksort(a,b,c);
			checksort(a,c,b);			
			checksort(b,a,c);
			checksort(b,c,a);
			checksort(c,b,a);
			checksort(c,a,b);		
		}

		@Test public void doManual() {
			
			
			//equals
			assertEquals(0, cmp.compare(a, a));
			assertEquals(0, cmp.compare(b, b));
			assertEquals(0, cmp.compare(c, c));
			
			//assertEquals(0, cmp.compare(dP, dN));
			
			
			//comparisons
			assertEquals(-1, cmp.compare(a, b));
			assertEquals(1, cmp.compare(b, a));
			
			assertEquals(1, cmp.compare(b, c));
			assertEquals(-1, cmp.compare(c, b));
			
			assertEquals(-1, cmp.compare(a, c));
			assertEquals(1, cmp.compare(c, a));
			
			assertTrue( cmp.compare(a,c) < 0 && cmp.compare(c,b) < 0);
			assertTrue( cmp.compare(a,b) < 0 );
			
			assertTrue( cmp.compare(b,c) > 0 && cmp.compare(c,a) > 0);
			assertTrue( cmp.compare(b,a) > 0 );
		}
	}
}