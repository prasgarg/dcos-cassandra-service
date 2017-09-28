package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.util.ArrayList;
import java.util.List;

public class SSTablesResponse {

	private List<String> sstablesFileNames = new ArrayList<>();

	public List<String> getSstablesFileNames() {
		return sstablesFileNames;
	}

	public void setSstablesFileNames(List<String> sstablesFileNames) {
		this.sstablesFileNames = sstablesFileNames;
	}

	void addSSTablesFileName(String fileName) {
		sstablesFileNames.add(fileName);
	}
}
