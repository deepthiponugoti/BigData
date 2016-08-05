package retrieve;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.NullPointerException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class retrieval {
	public static void main(String[] args) throws FileNotFoundException, IOException, NullPointerException
	{
		String inp,terms;
		String xa=new String();
		Configuration config = new Configuration();

		config.set("fs.default.name","hdfs://127.0.0.1:9000/");

		FileSystem dfs = FileSystem.get(config);
		int c=1;
		Map<String, Integer> count = new HashMap<String, Integer>();
	      Scanner in = new Scanner(System.in);
	      System.out.print("Please enter the terms you want to search : ");
	      terms = in.nextLine(); 
	      StringTokenizer token = new StringTokenizer(terms);
	      while(token.hasMoreTokens())
	      {
	    	xa=token.nextToken();
	    	char first=xa.charAt(0);
	    	String fname=first+"-WORDS-r-00000";
	    	Path src = new Path(args[0]+fname);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(src)));
			inp=br.readLine();
	    	  while(inp!=null)
	  		  {
	  			StringTokenizer itr = new StringTokenizer(inp);
	  			String xy=itr.nextToken();
	  			if(xa.equals(xy))
	  			{
	  				while(itr.hasMoreTokens())
	  				{
	  					StringTokenizer subtoken=new StringTokenizer(itr.nextToken(),":");
	  					String name=subtoken.nextToken();
	  					Integer x = count.get(name);
	  					if (x == null) 
	  					{
	  						x = 0;
	  					}
	  					count.put(name, x+Integer.parseInt(subtoken.nextToken()));
	  				}
	  				break;
	  			}
	  		  		inp=br.readLine();
	  		  }
	    	  if(inp==null)
	    		  System.out.println("Term "+xa+" is not found ");
	      }
	      MyComparator comp = new MyComparator(count);
	      Map<String, Integer> countSorted = new TreeMap(comp);
	      countSorted.putAll(count);
	      int num=0;
	      StringBuilder buf=new StringBuilder();
	      for (Map.Entry entry : countSorted.entrySet()) 
	      {
	    	  if(num>=10) break;
	    	  String y=(((String)entry.getKey())+":"+((Integer)entry.getValue()))+" ";
	          buf.append(y);
	          num++;
	      }
	    System.out.println(terms+": "+buf.toString());  
		count.clear();
		countSorted.clear();
		buf.setLength(0);

	}
}
