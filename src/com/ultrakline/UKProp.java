package com.ultrakline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UKProp {
	private Logger log = LoggerFactory.getLogger(UKProp.class);
	private String dhost = "";
	private String dport = "";
	private String durl = "";
	private String dlStoreDir = "";
	private String sqlStoreDir = "";
	private String kdayStoreDir = "";
	private String restFile = "";
	private String dType = "";
	private String dDays = "";
	private String startDate = "";
	private String endDate = "";
	private String listFileDir = "";
	private String projectHomeDir = "";
	private String tdxStoreDir = "";
	private String jsonStoreDir = "";
	private String tdxDList = "";
	private String firstDayFileName = "";
	private boolean isAllDateData=true;
	private static UKProp instance = null;
	private Map<String,String> stockStartDateMap=new HashMap<String,String>();
	private UKProp() {
		init();
	}

	private void init() {
		log.debug("....>>>>>>>>Read Properties Begin...");
		Properties prop = new Properties();
		try {

			String rootPath = new File("").getAbsolutePath();
			String initPropFileName = "init.properties";
			InputStream in = new FileInputStream(rootPath + File.separator
					+ initPropFileName);
			prop.load(in);
			this.dhost = prop.getProperty("dd.host");
			this.dport = prop.getProperty("dd.port", "80");
			this.durl = prop.getProperty("dd.url");
			this.projectHomeDir = prop.getProperty("project.home.dir", "./");
			this.dlStoreDir = projectHomeDir + File.separator
					+ prop.getProperty("dl.store.dir");
			this.kdayStoreDir = projectHomeDir + File.separator
					+ prop.getProperty("kday.store.dir");
			this.sqlStoreDir = projectHomeDir + File.separator
					+ prop.getProperty("sql.store.dir");

			this.dType = prop.getProperty("dtype", "all");
			this.dDays = prop.getProperty("ddays", "30");
			this.startDate = prop.getProperty("startdate", "2015-01-04");
			this.endDate= prop.getProperty("enddate", "2015-01-04");
			this.listFileDir = projectHomeDir + File.separator
					+ prop.getProperty("list.file.dir", "listfile");
			this.restFile = listFileDir + File.separator
					+ prop.getProperty("rest.date.file", "rest.txt");
			this.tdxStoreDir = projectHomeDir + File.separator
					+ prop.getProperty("tdx.store.dir", "tdxData");
			this.jsonStoreDir= projectHomeDir + File.separator
					+ prop.getProperty("json.store.dir", "json");
			this.tdxDList = prop.getProperty("tdx.dl.list", "shlday,szlday");
			this.firstDayFileName = listFileDir + File.separator
					+ prop.getProperty("first.day.file", "firstDay.txt");
			this.isAllDateData=(prop.getProperty("isAllDateData")).equalsIgnoreCase("true");
			stockStartDateMap.clear();
		} catch (Exception ex) {
			log.error("load prop error message is {}", ex.getMessage());
			ex.printStackTrace();
		}
		log.debug("....>>>>>>>>Read Properties End...");
	}

	public static UKProp getInstance() {
		if (instance == null) {
			instance = new UKProp();
		}

		return instance;
	}

	public String getFirstDayFileName() {
		return firstDayFileName;
	}

	public List<String> getStockCodeList() {
		String stockListFileName = listFileDir + File.separator + getdType() + ".txt";
		File stockCodeFile = new File(stockListFileName);
		List<String> stockCodeList = new ArrayList<String>();
		log.info("{} parse Begin ....>>>> ", stockCodeFile.getAbsolutePath());
		try {
			if (stockCodeFile.isDirectory()) {
				return stockCodeList;
			}
			InputStreamReader read = new InputStreamReader(new FileInputStream(
					stockCodeFile));
			BufferedReader reader = new BufferedReader(read);
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!stockCodeList.contains(line)) {
					stockCodeList.add(line);
				}
			}
			reader.close();
			log.info("{} parse End !!!! ", stockCodeFile.getAbsolutePath());
		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}

		return stockCodeList;
	}

	public List<String> getAllStockCodeList() {
		return (List<String>) getStockMap().get("ALL");
	}

	public String getJsonStoreDir(){
		return this.jsonStoreDir;
	}
	
	public Map<String, Object> getStockMap() {
		return parseFirstDay(new File(getFirstDayFileName()));
	}

	public Map<String, Object> parseFirstDay(File firstDateFile) {
		log.info("{} parse Begin ....>>>> ", firstDateFile.getAbsolutePath());
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			if (firstDateFile.isDirectory()) {
				return result;
			}
			InputStreamReader read = new InputStreamReader(new FileInputStream(
					firstDateFile));
			BufferedReader reader = new BufferedReader(read);
			String line = null;
			String tempCode = "";
			StringBuffer allStockSB = new StringBuffer();
			List<String> stockList = new ArrayList<String>();
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line != null && line.length() > 1) {
					log.info("Data is [{}]  !!!! ", line);
					result.put(line.substring(0, 6), line.substring(7));
					tempCode = line.substring(0, 6);
					if (!stockList.contains(tempCode)) {
						stockList.add(tempCode);
						allStockSB.append(tempCode).append("\r\n");
					}
				}
			}
			reader.close();
			log.info("{} parse End !!!! ", firstDateFile.getAbsolutePath());
			File allStockCodeFile = new File(firstDateFile.getParent(),
					"all.txt");
			FileWriter fw = new FileWriter(allStockCodeFile);
			fw.write(allStockSB.toString());
			fw.close();
			log.info("{} created  !!!! ", allStockCodeFile.getAbsolutePath());
			// ///////
			result.put("ALL", stockList);
		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}

		return result;
	}

	public String getDhost() {
		return dhost;
	}

	public String getDport() {
		return dport;
	}

	public String getDurl() {
		return durl;
	}

	public String getDLStoreDir() {
		return dlStoreDir;
	}

	public String getRestFile() {
		return restFile;
	}

	public String getdType() {
		return dType;
	}

	public String getdDays() {
		return dDays;
	}

	public String getStartDate() {
		return startDate;
	}

	public String getListFileDir() {
		return listFileDir;
	}

	public String getProjectHomeDir() {
		return projectHomeDir;
	}

	public String getTdxStoreDir() {
		return tdxStoreDir;
	}

	public String getTdxDList() {
		return tdxDList;
	}

	public String getSqlStoreDir() {
		return sqlStoreDir;
	}

	public String getKdayStoreDir() {
		return kdayStoreDir;
	}

	public String getEndDate() {
		return endDate;
	}
	
	public boolean isAllDateData(){
		return isAllDateData;
	}
	
	public void putStartDate(String stockCode,String startDate){
		stockStartDateMap.put(stockCode, startDate);
	}
	
	public String getStartDate(String stockCode){
		return stockStartDateMap.get(stockCode);
	}
}
