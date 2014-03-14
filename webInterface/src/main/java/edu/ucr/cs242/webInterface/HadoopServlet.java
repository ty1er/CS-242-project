package edu.ucr.cs242.webInterface;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.ucr.cs242.runQuery.SearchLuceneIndex;

/**
 * Servlet implementation class HadoopServlet
 */
public class HadoopServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HadoopServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String searchSentence = request.getParameter("query");
        
        String hadoopDir = "/Users/stevenjacobs/cs242project/HADOOPINDEX";
        String tweetDir = "/Users/stevenjacobs/cs242project/TWEETTEXTINDEX";
        SearchLuceneIndex searcher = new SearchLuceneIndex(hadoopDir, tweetDir, searchSentence);
        Map<Double,String> sortedMap = searcher.sortedMap;
		
		Set set = sortedMap.entrySet();
		Iterator i = set.iterator();
		
    	PrintWriter wr = response.getWriter();
        wr.println("<h1>Hello from Lucene</h1>");
        
		while (i.hasNext()) {
			Map.Entry me = (Map.Entry) i.next();
			String tid = me.getValue().toString();
			String TweetText = searcher.getTweetText(tweetDir, tid);
			wr.println("<p>" + me.getKey().toString() + " "
					+ me.getValue().toString() + " " + TweetText + "</p>");

		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
