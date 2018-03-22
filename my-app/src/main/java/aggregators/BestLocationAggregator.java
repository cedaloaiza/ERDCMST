package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;

import edu.icesi.app.Location;

/**
 * To get best location
 * @author cesardlq
 *
 */

public class BestLocationAggregator extends BasicAggregator<Location> {

	@Override
	public void aggregate(Location loc) {
		Location oldBestLoc = getAggregatedValue();
		if( loc.getCost() < oldBestLoc.getCost()){
			setAggregatedValue(loc);
		}
		
	}

	@Override
	public Location createInitialValue() {
		// TODO Auto-generated method stub
		return new Location();
	}

}
