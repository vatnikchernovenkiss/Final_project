import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class IRSession {
    public HashMap<String, Integer> time_spent;
    public String[] showed_urls;
    public int[] clicked_pos;
    public IRSession() {}
    public IRSession(String session) {
        String[] str_session = session.trim().split("\t");
        showed_urls = str_session[1].replace("http://","").replace("www.", "").replace("https://","").replace("/,",",").split(",");
        String[] clicked = str_session[2].replace("http://","").replace("www.", "").replace("https://","").replace("/,",",").split(",");
	String[] timestamps = str_session[3].split(",");
        clicked_pos = new int[clicked.length];
        int timestamp =  Integer.parseInt(timestamps[0]);
        for (int i = 0; i < clicked.length; i++) {
            for (int j = 0; j < showed_urls.length; j++) {
                if (clicked[i].equals(showed_urls[j])) {
                    if (i < clicked.length - 1) {
                    	time_spent.put(showed_urls[j], Integer.parseInt(timestamps[j + 1]) - timestamp);
                    	timestamp = Integer.parseInt(timestamps[j + 1]);
                    }
                    clicked_pos[i] = j;
                    break;
                }
            }
        }
    } 
}
