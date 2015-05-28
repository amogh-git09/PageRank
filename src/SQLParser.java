


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class SQLParser {
	String prefix;
	String suffix; 
	
	BufferedReader in;
	
	public SQLParser(BufferedReader in, String table){
		this.in = in;
		prefix = "INSERT INTO `" + table + "` VALUES ";
		suffix = ";";
	}
	
	public List<List<Object>> readTuples() throws IOException{
		while (true){
			String line = "";
			
			try {
				if((line = in.readLine()) == null)
					System.out.println("OVER");
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(line == null){
				return null;
			}
			else if (line.equals("") || line.startsWith("--"))
					continue;
			else if(!(line.startsWith(prefix) && line.endsWith(suffix))){
				continue;
			}
			
			//pass only (...,...,...) to parseTuples, excluding prefix and suffix
			//i.e. "INSERT INTO table_name VALUES" and ";"
			return parseTuples(line.substring(prefix.length(), line.length() - 1));
		}
	}
	
	static int k=0;
	
	static List<List<Object>> parseTuples (String line) throws IllegalArgumentException{
		List<List<Object>> ret = new ArrayList<List<Object>>();
		
		//implementation of state machine
		int state = 0;
		List<Object> tuple = new ArrayList<Object> ();
		int startIndex = -1;
		
		for(int i=0; i < line.length(); i++){
			char ch = line.charAt(i);
		//	System.out.print(ch);
		//	System.out.print("     Enter State = " + state + "; ");
			
			switch (state){
			//outside tuple, expecting '('
			case 0: 
				if (ch == '(')
					state = 1;
				else
					error("state 0");
				break;
				
			//Start of a new tuple	
			case 1: 
				if(ch >= '0' && ch <= '9' || ch == '-' || ch == '.')	//number
					state = 2;	
				else if (ch == '\'')									//string
					state = 3;
				else if (ch == 'N')										//NULL										
					state = 5;
				else if (ch == ')'){									//end of current tuple
					ret.add(tuple);
					tuple = new ArrayList<Object>();
					state = 8;
				} else 
					error("state 1");
				
				startIndex = i;
				if (state == 3)
					startIndex++;					//next item is string: skipping single quote
				break;
				
			//number	
			case 2:
				if (ch >= '0' && ch <= '9' || ch == '-' || ch == '.');
				else if (ch == ',' || ch == ')'){
					String s = line.substring(startIndex, i);
					startIndex = -1;
					
					if(s.indexOf(".") == -1)						//Integer, no decimal point
						tuple.add(new Integer(s));
					else											//floating point number
						tuple.add(new Double(s));					
					
					if (ch == ','){									//another item follows
						state = 7;
					}
					else if (ch == ')') {							//end of current tuple
						ret.add(tuple);
						tuple = new ArrayList<Object>();
						state = 8;
					}
				} else{
					System.out.println ("Throwing Exception");
					error("state 2");
				}
				break;
				
			//string
			case 3:
				if (ch == '\'') {									//string completed
					String s = line.substring(startIndex, i);
					startIndex = -1;
					
					if (s.indexOf('\\') != -1){						//this is to unescape any escaped characters
						s = s.replaceAll("\\\\", "");
					}
					
					tuple.add(s);
					state = 6;
				} else if (ch == '\\')								//escape character
					state = 4;
				break;
				
			case 4:		
				if (ch =='\'' || ch == '\"' || ch == '\\')			//supports only single quote,					
					state = 3;										//double quote and backslash
				else
					error("state 4");
				break;
				
			//unquoted symbol i.e NULL
			case 5:
				if (ch >= 'A' && ch <= 'Z');
				else if (ch == ',' || ch == ')') {
					if (line.substring(startIndex, i).equals("NULL"))
						tuple.add(null);
					else
						error("state 5");
						
					startIndex = -1;
					if (ch == ',')
						state = 7;
					else if (ch == ')'){
						ret.add(tuple);
						tuple = new ArrayList<Object>();
						state = 8;
					}
				} else
					error("state 5 else");
				break;
				
			//inside the tuple, expecting comma or ')'	
			case 6:
				if (ch == ',')
					state = 7;
				else if (ch == ')'){
					ret.add(tuple);
					tuple = new ArrayList<Object>();
					state = 8;
				} else
					error("state 6");
				break;
				
			//inside the tuple, expecting new item
			case 7:
				if (ch >= '0' && ch <= '9' || ch == '-' || ch == '.')
					state = 2;
				else if (ch == '\'') 
					state = 3;
				else if (ch == 'N')
					state = 5;
				else 
					error("state 7");
				
				startIndex = i;
				if(state == 3)					//next item is string: skipping single quote
					startIndex++;		
				
				break;
				
			//Outside tuple, expecting comma or end of line
			case 8:
				if (ch == ',')
					state = 9;
				else
					error("state 8");
				
				break;
				
			//Outside the tuple, waiting for next tuple, expecting '('
			case 9:
				if (ch == '(')
					state = 1;
				else
					error("state 9");
				
				break;
				
			default: 
				error("state default");
			}
			
	//	System.out.println("; Exit State = " + state + "; ");
		}
		
		//the state must be 8, that's when a tuple ends
		if (state != 8)
			error("state NOT 8");
		
		return ret;
	}
	
	static void error(String msg) throws IllegalArgumentException{
		System.out.println("ERROR: " + msg);
		throw new IllegalArgumentException();
	}
	
	public static HashMap<String, Integer> prepareIdByTitleMap (File file){
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		System.out.println("Preparing idByTitleMap");
		int count = 0, mil=1;
		
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(file), PageRanker.CHARSET));
			String line;
			int nullCount = 0;
			
			while((line = reader.readLine()) != null){
				count++;
				if(count > mil*100000){
					System.out.println("Reading: " + mil*100000 + " entries");
					mil++;
				}
				
				String[] items = line.split(" ");
				
				if(items.length != PageRanker.PAGE_TUPLE_LEN){
					for(int i=0; i<items.length; i++){
						System.out.format("%s ", items[i]);
					}System.out.println();
					System.out.printf("SQLParser.readFile(): tuple length not appropriate (%d), "
							+ "skipping a tuple\n", items.length);
					continue;
				}
				
				int id = Integer.parseInt(items[0]);
				int namespace = Integer.parseInt(items[1]);
				String title = items[2];
								
				//namespace should be 0
				if(namespace != 0){
					continue;
				}
		
				title = namespace + ":" + title;
				
				result.put(title, id);
				PageRanker.totalPages++;
			}
			
			reader.close();
			System.out.format("idByTitleCount prepared, nullCount: %d, totalPages: %d\n", nullCount, PageRanker.totalPages);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	//Prepares the inLinks and numOutLinks 
	public static HashMap<Integer, List<Integer>> prepareInLinks(File linksFile, 
			HashMap<String, Integer> idByTitleMap) {
		System.out.println("Preparing inLinks");
		HashMap<Integer, List<Integer>> result = new HashMap<Integer, List<Integer>>();
		
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(linksFile), PageRanker.CHARSET));

			String line;
			int count = 0, mil=1, skip=0;
			
			while((line = reader.readLine()) != null){
				count++;
				if(count > mil*100000){
					System.out.println("Preparing: " + mil*100000 + " entries");
					mil++;
				}
				String[] items = line.split(" ");
				if(items.length != PageRanker.PAGELINK_TUPLE_LEN){
					System.out.printf("SqlReader.inLinks(): items length not appropriate (%d),"
							+ " skipping tuple\n", items.length);
					continue;
				}
				
				int srcId = Integer.parseInt(items[0]);
				int destNameSpace = Integer.parseInt(items[1]);
				String destTitle = items[2];
				int srcNamespace = Integer.parseInt(items[3]);
				
				if(destNameSpace != 0){
					continue;
				}
					
				destTitle = destNameSpace + ":" + destTitle;
				
				if(!idByTitleMap.containsKey(destTitle)){
			//		System.out.println("destTitle not found, skipping: " + destTitle + "    ; srcId: " + srcId);
					continue;
				}
												
				int destId = PageRanker.idByTitleMap.get(destTitle);
				
				if(srcId == destId){
					continue;
				}
				
				List<Integer> newList;
				
				if(result.containsKey(destId)){
					newList = result.get(destId);
				} else {
					newList = new ArrayList<Integer> ();
				}
				
				if(newList.contains(srcId)){
					continue;
				}
				
				newList.add(srcId);
				
				if(PageRanker.numOutLinks.get(srcId) == null){
					//System.out.println("SourceId not found: " + srcId + " ; skipping");
					skip++;
					continue;
				}
				
				result.put(destId, newList);

				int oldVal = PageRanker.numOutLinks.get(srcId);
				PageRanker.numOutLinks.put(srcId, oldVal+1);
		//		System.out.println("Adding to inLinks: " + destId + ", " + srcId);
			}
			
			System.out.println("Total skipped = " + skip);
			reader.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	

		return result;
	}
	
	public static ArrayList<Integer> prepareDanglingPages (HashMap<Integer, String> titleByIdMap){
		System.out.println("Preparing Dangling Pages");
		ArrayList<Integer> result = new ArrayList<Integer> ();
		
		int count=0, mil=1;
		
		for(int id : titleByIdMap.keySet()){
			count++;
			if(count > mil*100000){
				System.out.println("Preparing Dangling Pages: " + mil*100000 + " entries");
				mil++;
			}
			
			if(PageRanker.numOutLinks.get(id) == 0)
				result.add(id);
		}
		
		System.out.println("Dangling Pages prepared");
		return result;
	}
}
