package com.ultrakline;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ultrakline.data.download.ExchangeRecord;
import com.ultrakline.data.download.TDXData;
import com.ultrakline.parse.ParseTDXDayFile;
import com.ultrakline.util.DateUtils;

public class Main {
	private static Logger log = LoggerFactory.getLogger(Main.class);

	private static UKProp ukprop = UKProp.getInstance();
	private ExecutorService exec;
	private CompletionService<Integer> completionService;

	public static void main(String[] args) {
		try {
			// int dateInt=19990721;

			// log.debug(result);
			Main m = new Main();
			//delete all old data 
			m.purge();
			//create data store dir
			m.mkStoreDirs();
			log.info(" ---> [0] store dir cleaned!");
			m.downloadTDXData(ukprop.getTdxDList());
			log.info(" ---> [1] download tdx data finished!");
			Thread.sleep(60000);
			m.unzipTdxZip();
			log.info(" --->[2]tdx data unziped!");
			//Thread.sleep(60000);
			m.parseTDXDataFile(ukprop.getTdxStoreDir());
			log.info(" --->[3]parse tdx data store in {}  finished!", ukprop.getTdxStoreDir());
			//m.downloadData(ukprop.isAllDateData());
			// log.debug(" --->[4]download detail data finished!");
			// m.downloadAllExtData();
			/*
			 * List<String> stockList =
			 * UKProp.getInstance().getAllStockCodeList(); Integer rr =
			 * m.mthreadDLData(stockList, 2); log.debug(
			 * " --->[4]record counts is {}!", rr); m.close(); System.exit(0);
			 */
			// mthreadDLData();
		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}
	}

	public Main() {
		init();
	}

	private void init() {
		Properties prop = new Properties();
		try {
			String rootPath = new File("").getAbsolutePath();
			String runPropFileName = "run.properties";
			InputStream in = new FileInputStream(rootPath + File.separator + runPropFileName);
			prop.load(in);
		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}
	}

	public void purge() {
		try {
			log.debug("delete all store dir");
			File tmpDir = null;
			String kdayStoreDir = ukprop.getKdayStoreDir();
			tmpDir = new File(kdayStoreDir);
			if (tmpDir.exists() && tmpDir.isDirectory()) {
				deleteDir(tmpDir);
			}
			String dlStoreDir = ukprop.getDLStoreDir();
			tmpDir = new File(dlStoreDir);
			if (tmpDir.exists() && tmpDir.isDirectory()) {
				deleteDir(tmpDir);
			}
			String tdxStoreDir = ukprop.getTdxStoreDir();
			tmpDir = new File(tdxStoreDir);
			if (tmpDir.exists() && tmpDir.isDirectory()) {
				// tmpDir.delete();
				deleteDir(tmpDir);
			}
			String sqlStoreDir = ukprop.getSqlStoreDir();
			tmpDir = new File(sqlStoreDir);
			if (tmpDir.exists() && tmpDir.isDirectory()) {
				// tmpDir.delete();
				deleteDir(tmpDir);
			}
			log.info("delete all store dir end");
		} catch (Exception ex) {
			log.debug("delete  store error {}", ex.getMessage());
		}

	}

	private boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		log.debug(" {} deleted ", dir.getAbsolutePath());
		return dir.delete();
	}

	public void mkStoreDirs() {
		File tmpDir = null;
		log.info(" create store dir begin ");
		String dlStoreDir = ukprop.getDLStoreDir();
		tmpDir = new File(dlStoreDir);
		tmpDir.mkdir();
		String kdayStoreDir = ukprop.getKdayStoreDir();
		tmpDir = new File(kdayStoreDir);
		tmpDir.mkdir();
		String tdxStoreDir = ukprop.getTdxStoreDir();
		tmpDir = new File(tdxStoreDir);
		tmpDir.mkdir();
		String sqlStoreDir = ukprop.getSqlStoreDir();
		tmpDir = new File(sqlStoreDir);
		tmpDir.mkdir();
		String jsonStoreDir = ukprop.getJsonStoreDir();
		tmpDir = new File(jsonStoreDir);
		tmpDir.mkdir();

		log.info(" create store dir end! ");
	}

	public void parseTDXDataFile(String fileName) {
		ParseTDXDayFile pf = new ParseTDXDayFile();

		try {
			File tdxDir = new File(fileName);
			log.info(" parse all file in {}", tdxDir.getAbsolutePath());
			String tmpFileName = "";
			String stockCode = "";
			StringBuffer sqlListSB = new StringBuffer();
			StringBuffer allStockCode = new StringBuffer();
			List<String> stockList=new ArrayList<String>();
			if (tdxDir.isDirectory()) {
				File[] fs = tdxDir.listFiles();
				log.info(" {} files need parse!", fs.length);
				for (int i = 0; i < fs.length; i++) {
					tmpFileName = fs[i].getName();
					if (tmpFileName.startsWith("sh60") || tmpFileName.startsWith("sz000")
							|| tmpFileName.startsWith("sz002") || tmpFileName.startsWith("sz300")
							|| tmpFileName.startsWith("sz001")) {
						stockCode = tmpFileName.substring(2, 8);
						pf.parse(fs[i], stockCode);
						sqlListSB.append("@").append(tmpFileName.replace(".day", ".sql")).append("\r\n");
						//

						allStockCode.append(stockCode).append("\r\n");
						stockList.add(stockCode);
					} else {
						log.error(" {} is not stock ,maybe is fund.", tmpFileName);
					}
				}
			}
			log.debug("...create list.sql!");
			FileWriter fw = new FileWriter(ukprop.getSqlStoreDir() + File.separator + "list.sql");
			sqlListSB.append("commit;\r\n");
			fw.write(sqlListSB.toString());
			fw.close();

			// /
			log.info("...create stock list file!");
			FileWriter fwStockCodeList = new FileWriter(ukprop.getListFileDir() + File.separator + "allStock.txt");
			// sqlListSB.append("commit;\r\n");
			fwStockCodeList.write(allStockCode.toString());
			fwStockCodeList.close();
			///////////create stock startdate
			FileWriter fwStockStartList = new FileWriter(ukprop.getListFileDir() + File.separator + "allStockFirstDay.txt");
			// sqlListSB.append("commit;\r\n");
			String tempCode;
			StringBuffer stockFD=new StringBuffer();
			for(int i=0;i<stockList.size();i++){
				tempCode=stockList.get(i);
				stockFD.append(tempCode).append(",").append(ukprop.getStartDate(tempCode)).append("\r\n");
			}
			fwStockStartList.write(stockFD.toString());
			fwStockStartList.close();
			//////
			
		} catch (Exception ex) {
			log.error("parseTDXDataFile  error msg is [{}]", ex.getMessage());
		}
	}

	public void downloadTDXData(String dTypes) {
		TDXData tdx = new TDXData();
		tdx.downloadTDXData(dTypes, UKProp.getInstance().getTdxStoreDir());
	}

	public void unzipTdxZip() {
		File tdxDir = new File(ukprop.getTdxStoreDir());
		StringTokenizer st = new StringTokenizer(ukprop.getTdxDList(), ",");
		String tempFileName = "";
		File tempFile = null;
		while (st.hasMoreElements()) {
			tempFileName = (String) st.nextElement();
			tempFile = new File(tdxDir, tempFileName + ".zip");
			try {
				BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(tempFile));
				ArchiveInputStream in = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.JAR,
						bufferedInputStream);
				JarArchiveEntry entry = null;
				while ((entry = (JarArchiveEntry) in.getNextEntry()) != null) {
					if (entry.isDirectory()) {
						new File(tdxDir, entry.getName()).mkdir();
					} else {
						OutputStream out = FileUtils.openOutputStream(new File(tdxDir, entry.getName()));
						IOUtils.copy(in, out);
						out.close();
					}
				}
				in.close();
			} catch (FileNotFoundException e) {
				log.error("can't find zip file {}", tempFile.getAbsolutePath());
			} catch (ArchiveException e) {
				log.error("this file {} is unsupport", tempFileName);
			} catch (IOException e) {
				log.error("unzip error , file {} ", tempFileName);
			}
		}

	}

	public void downloadData(boolean isAll) {
		//TODO 
		try {

			List<String> stockCodeList = new ArrayList<String>();
			if (UKProp.getInstance().getdType().equalsIgnoreCase("all")) {
				stockCodeList = UKProp.getInstance().getAllStockCodeList();
			} else {
				stockCodeList = UKProp.getInstance().getStockCodeList();

			}

			int days = Integer.parseInt(UKProp.getInstance().getdDays());
			days = 50;
			ExchangeRecord exrcd = new ExchangeRecord(stockCodeList, days);
			for (int i = 0; i < stockCodeList.size(); i++) {
				exrcd.downloadData(stockCodeList.get(i), days);
				Thread.sleep(2000);
				log.info(" {} download finished.sleep 2s", stockCodeList.get(i));
			}

		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}
	}

	public int mthreadDLData(List<String> stockList, int days) {
		log.info(" Muti Thread begin, stock number is {} ,days is {}.", stockList.size(), days);
		try {
			exec = Executors.newFixedThreadPool(5);
			List<String>[] stockListArray = new ArrayList[5];
			stockListArray[0] = new ArrayList<String>();
			stockListArray[1] = new ArrayList<String>();
			stockListArray[2] = new ArrayList<String>();
			stockListArray[3] = new ArrayList<String>();
			stockListArray[4] = new ArrayList<String>();
			// List<String> l1 = new ArrayList<String>();
			log.info(" Muti Thread 100.");
			for (int i = 0; i < stockList.size(); i++) {
				log.info(" Muti Thread i={}. i%5={}", i, i % 5);
				if (i % 5 == 0) {
					stockListArray[0].add(stockList.get(i));
					continue;
				} else if (i % 5 == 1) {
					stockListArray[1].add(stockList.get(i));
					continue;
				} else if (i % 5 == 2) {
					stockListArray[2].add(stockList.get(i));
					continue;
				} else if (i % 5 == 3) {
					stockListArray[3].add(stockList.get(i));
					continue;
				} else if (i % 5 == 4) {
					stockListArray[4].add(stockList.get(i));
				}
			}
			log.info(" Muti Thread 200.");
			for (int i = 0; i < 5; i++) {
				completionService = new ExecutorCompletionService<Integer>(exec);
				ExchangeRecord exrcd = new ExchangeRecord(stockListArray[i], days);
				if (!exec.isShutdown()) {
					exec.submit(exrcd);
				}
			}
		} catch (Exception ex) {
			log.error("  Muti Thread error is {} ", ex.getMessage());
		}

		return getResult();
	}

	public int downloadAllExtData(Map<String, Object> stockMap, List<String> stockList, int days) {
		int counts = 0;
		try {
			ExchangeRecord exrcd = new ExchangeRecord(stockList, days);

			String firstDateStr = null;
			Date firstDate = null;

			String stockCode;
			int count = 0;
			// Iterator<String> it =result.keySet().iterator();
			for (int i = 0; i < stockList.size(); i++) {
				stockCode = stockList.get(i);
				firstDateStr = (String) stockMap.get(stockCode);
				log.info(" code {}  start date {} Download Begin...", stockCode, firstDateStr);
				firstDate = new SimpleDateFormat("yyyymmdd").parse(firstDateStr);
				days = DateUtils.daysBefore(firstDate);
				if (days > 700)
					days = 300;
				counts += exrcd.downloadData(stockCode, days);
				Thread.sleep(2000);
				count++;
				log.info(" code {}  start date {} all data download finished.sleep 2s", stockCode, firstDateStr);
			}
			log.info(" {} data downloaded!", count);
		} catch (Exception ex) {
			log.error(" error is {}", ex.getMessage());
		}

		return counts;
	}

	public void close() {
		exec.shutdown();
	}

	public int getResult() {
		int result = 0;
		for (int i = 0; i < 5; i++) {
			try {
				int subSum = completionService.take().get();
				result += subSum;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

}
