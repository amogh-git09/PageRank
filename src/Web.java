


import java.util.List;
import java.util.Map;

public class Web {
	int size;
	Map<Integer, List<Integer>> inLinks;
	Map<Integer, Integer> numOutLinks;
	List<Integer> danglingPages;
	
	public int getSize() {
		return size;
	}

	public Map<Integer, List<Integer>> getInLinks() {
		return inLinks;
	}

	public Map<Integer, Integer> getNumOutLinks() {
		return numOutLinks;
	}

	public List<Integer> getDanglingPages() {
		return danglingPages;
	}

	public Web (int size, 	Map<Integer, List<Integer>> inLinks, Map<Integer, Integer> numOutLinks, 
			List<Integer> danglingPages){
		this.size = size;
		this.inLinks = inLinks;
		this.numOutLinks = numOutLinks;
		this.danglingPages = danglingPages;
	}
}
