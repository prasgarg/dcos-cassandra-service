package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.util.HashMap;
import java.util.Map;

public class MatrixTablestatsResponse {
	
	private Map<String, Integer> tableStats;
	
	public MatrixTablestatsResponse(){
		tableStats = new HashMap<>();
	}

	public Map<String, Integer> getTableStats() {
		return tableStats;
	}

	public void setTableStats(Map<String, Integer> tableStats) {
		this.tableStats = tableStats;
	}

	@Override
	public String toString() {
		return "MatrixTablestatsResponse [tableStats=" + tableStats + "]";
	}
}
