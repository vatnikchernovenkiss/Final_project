import java.util.Arrays;

public class StatCalc {

    public long shows = 0;
    public long clicks = 0;
    public long dbnShows = 0;
    public long dbnClicks = 0;
    public long dbnSat = 0;
    public long time = 0;
    public long posSum = 0;

    public StatCalc() {}

    public void inc_stat(int click, int pos, int lastClickPos, int time_spent) {
        shows++;
        clicks += click;
  	if (pos <= lastClickPos) {
            dbnShows++;
            dbnClicks += click;
            if (pos == lastClickPos) {
                dbnSat++;
            }
        }
        posSum += pos;
        time += time_spent;
    }

    public StringBuilder get_stat(int queryId) {
        StringBuilder res = new StringBuilder();
        if (queryId >= 0) {
            res.append(queryId);
            res.append(" ");
        }
  	res.append(shows);
  	res.append(" ");
 	res.append(clicks);
 	res.append(" ");
        res.append(dbnShows);
        res.append(" ");
        res.append(dbnClicks);
        res.append(" ");
        res.append(dbnSat);
        res.append(" ");
        res.append(((double) posSum) / shows);
        res.append(" ");
        if (clicks > 0) {
	      res.append(((double) time) / clicks);
	} else {
	      res.append(0);
	}
        return res;
    }
}
