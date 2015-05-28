


import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class PageRanker {
	public static final String CHARSET = "SJIS";
	public static final int PAGE_TUPLE_LEN = 13;
	public static final int PAGELINK_TUPLE_LEN = 4;
	public static final String PAGES_SQL_FILE = "jawiki-20150512-page.sql.gz";
	public static final String PAGELINKS_SQL_FILE = "jawiki-20150512-pagelinks.sql.gz"; 
	public static final String PAGES_TEXT_FILE = "pages.txt";
	public static final String PAGELINKS_TEXT_FILE = "pagelinks.txt";
	public static final String PAGERANKS_TEXT_FILE = "pageranks.txt";
	 
	public static final float D = 0.85f;					//DAMPING FACTOR
	public static final float TOLERANCE = 0.0000001f;

	public static HashMap<String, Integer> idByTitleMap = new HashMap<String, Integer> ();
	public static HashMap<Integer, String> titleByIdMap = new HashMap<Integer, String> ();
	public static HashMap<Integer, Integer> numOutLinks = new HashMap<Integer, Integer>();
	public static List<Integer> danglingPages = new ArrayList<Integer>();
	public static HashMap<Integer, List<Integer>> inLinks;
	public static int totalPages = 0;

	
	public static void main(String args[]){
		long startTime = System.currentTimeMillis();
		
		try {
			System.out.println("WRITING PAGES_TEXT_FILE");
			File pagesTextFile = new File(PAGES_TEXT_FILE);
			File pagelinksTextFile = new File(PAGELINKS_TEXT_FILE);
			File pageRanksTextFile = new File(PAGERANKS_TEXT_FILE);

			int numLines = 0, mil=1;
	//		int lim = 5;

			if(!pagesTextFile.exists() || pagesTextFile.length() == 0){
				System.out.println("Writing " + PAGES_TEXT_FILE + "; length was " + pagesTextFile.length());

				PrintWriter writer = new PrintWriter (new OutputStreamWriter (
						new BufferedOutputStream(new FileOutputStream(pagesTextFile)), CHARSET));
				SQLParser parser = new SQLParser (new BufferedReader (new InputStreamReader(
						new GZIPInputStream(new FileInputStream(new File(PAGES_SQL_FILE))), "UTF-8")), "page");
				
				while (true){
					List<List<Object>> table = parser.readTuples();
					if(table == null)
						break;
	/*				if(numLines > lim*100000)
						break;
	*/	
					numLines += table.size();
					if(numLines > mil*100000){
						System.out.println("Writing: " + mil*100000 + " entries");
						mil++;
					}


					for(int i=0; i < table.size(); i++){
						List<Object> tuple = table.get(i);

						for(int j=0; j<tuple.size(); j++){
							writer.print(tuple.get(j) + " ");
						}
						writer.println();
					}
				}

				writer.close();
			}

	//		lim = 5;

			if(!pagelinksTextFile.exists() || pagelinksTextFile.length() == 0){
				System.out.println("WRITING PAGELINKS_TEXT_FILE");
				numLines = 0; mil=1;

				SQLParser parser = new SQLParser(new BufferedReader (new InputStreamReader(
						new GZIPInputStream(new FileInputStream(new File(PAGELINKS_SQL_FILE))), "UTF-8")), "pagelinks");
				
				PrintWriter writer = new PrintWriter (new OutputStreamWriter (
						new BufferedOutputStream(new FileOutputStream(pagelinksTextFile)), CHARSET));

				while (true){
					List<List<Object>> table = parser.readTuples();
					if(table == null)
						break;
	/*				if(numLines > lim*100000)
						break;
	*/	 
					numLines += table.size();
					if(numLines > mil*100000){
						System.out.println("Writing: " + mil*100000 + " entries");
						mil++;
					}


					for(int i=0; i < table.size(); i++){
						List<Object> tuple = table.get(i);

						for(int j=0; j<tuple.size(); j++){
							writer.print(tuple.get(j) + " ");
						}
						writer.println();
					}
				}

				writer.close();
			}

			idByTitleMap = SQLParser.prepareIdByTitleMap(pagesTextFile);
			
			System.out.println("idByTitleMap prepared: " + idByTitleMap.size() + " entries");
			
			for(String title : idByTitleMap.keySet()){
				int id = idByTitleMap.get(title);
				titleByIdMap.put(id, title);
			}

			numOutLinks = initializeNumOutLinks ();
			inLinks = SQLParser.prepareInLinks(pagelinksTextFile, 
					idByTitleMap);

			danglingPages = SQLParser.prepareDanglingPages(titleByIdMap);

			System.out.println("numOutLinks = " + numOutLinks.size());
			 
			System.out.println("First 10 dangling pages:");
			for(int i=0; i<(danglingPages.size()<10?danglingPages.size():10); i++){
				System.out.println("id: " + danglingPages.get(i));
			}

			System.out.println("Creating new web object: ");
			System.out.println("totalPages: " + totalPages);
			System.out.println("inLinks size: " + inLinks.size());
			System.out.println("numOutLinks size: " + numOutLinks.size());
			System.out.println("danglingPages size: " + danglingPages.size());
			Web g = new Web(totalPages, inLinks, numOutLinks, danglingPages);

			HashMap<Integer, Float> pageRanks = pageRanks(g, D, TOLERANCE);
			
			System.out.println("Calculation Over, Will print pageRanks now");
		
			Set<Entry<Integer, Float>> set = pageRanks.entrySet();
			List<Entry<Integer, Float>> list = new ArrayList<Entry<Integer, Float>>(set);
			Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>(){
				@Override
				public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2){
					return (o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			PrintWriter writer = new PrintWriter (new OutputStreamWriter (
					new BufferedOutputStream(new FileOutputStream(pageRanksTextFile)), CHARSET));
		
			for(Map.Entry<Integer, Float> entry : list){
				float rank = entry.getValue();
				int id = entry.getKey();
				String title = titleByIdMap.get(id);
				int namespace = Integer.parseInt(title.substring(0, 1));
				title = title.substring(2, title.length());
				writer.format("%d %s %.15f %.7f\n", id, title, rank, Math.log10(rank));
			}
			
			writer.close();
						
			long endTime = System.currentTimeMillis();
		
			System.out.format("Start time: %d, End time: %d\n", startTime, endTime);
			System.out.println("Time taken: " + (endTime - startTime)/1000f + " secs");
			System.out.println("DONE");	
		} 
		catch (IOException e){
			System.out.println(e.getMessage());
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		} 
	}

	static HashMap<Integer, Integer> initializeNumOutLinks (){
		int count=0, mil=1;
		HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();

		for(int id : titleByIdMap.keySet()){
			count++;
			if(count > mil*100000){
				System.out.println("initialzing numOutLinks: " + mil*100000 + " entries");
				mil++;
			}

			result.put(id, 0);
		}

		return result;
	}

	static HashMap<Integer, Float> pageRanks(Web g, float d, float tolerance){
		int n = g.getSize();
		HashMap<Integer, Float> p = new HashMap<Integer, Float> ();

		for(int id : titleByIdMap.keySet()){
			p.put(id, 1f/n);
		}

		int iteration = 1;
		float change = 2f;

		while(change > tolerance){
			System.out.println("Iteration: " + iteration);
			HashMap<Integer, Float> newP = step(g, p, d);

			float sum = 0f;
			for(int i : p.keySet()){
				sum += Math.abs(p.get(i) - newP.get(i));
			}

			change = sum;

			System.out.format("Change: %.15f\n", change);
			p = newP;
			iteration++;
		}

		return p;
	}

	static HashMap<Integer, Float> step (Web g, HashMap<Integer, Float> p, float d){
		int n = g.getSize();
		HashMap<Integer, Float> v = new HashMap<Integer, Float>();

		for(int id : titleByIdMap.keySet()){
			v.put(id, 0f);
		}

		float innerProduct = 0f;
		for(int j : g.danglingPages){
			innerProduct += p.get(j);
		}

		for(int id : titleByIdMap.keySet()){
			List<Integer> list = g.getInLinks().get(id);

			float temp = 0f;

			if(list != null){
				for(int k : list){
					if(!g.getNumOutLinks().containsKey(k)){
						continue;
					}

					int numOfOutLinks = g.getNumOutLinks().get(k);
					temp += p.get(k)/numOfOutLinks;
				}
			}

			temp *= d;

			temp += d*innerProduct/n;

			temp += (1-d)/n;

			v.put(id, temp);
		}

		float sum = 0f;

		for(int i : v.keySet()){
			sum += v.get(i);
		}

		for(int i : v.keySet()){
			float oldVal = v.get(i);
			v.put(i, oldVal/sum);
		}

		return v;
	}
}