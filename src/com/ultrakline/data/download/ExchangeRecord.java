package com.ultrakline.data.download;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ultrakline.UKProp;
import com.ultrakline.util.DateUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class ExchangeRecord implements Callable<Integer> {

	private List<String> restDateList = new ArrayList<String>();
	private Logger log = LoggerFactory.getLogger(ExchangeRecord.class);
	private List<String> stockList = null;
	private int days;

	public ExchangeRecord(List<String> stockList, int days) {
		init();
		this.stockList = stockList;
		this.days = days;
	}

	public Integer call() {
		int result = 0;
		int counts = 0;
		for (int i = 0; i < stockList.size(); i++) {
			counts = downloadData(stockList.get(i), days);
			result += counts;
		}
		return result;
	}

	private UKProp props = UKProp.getInstance();

	private void init() {
		try {

			InputStreamReader read = new InputStreamReader(new FileInputStream(
					props.getRestFile()));
			BufferedReader reader = new BufferedReader(read);
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!restDateList.contains(line)) {
					restDateList.add(line);
				}
			}
			reader.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void dlextData(String stockCode, String currentDate) {
		File storeFile = null;
		String ss = "ss";
		if (stockCode.startsWith("60")) {
			ss = "sh";
		} else {
			ss = "sz";
		}
		String tempUrl = "";
		try {
			String stockStorepath = props.getDLStoreDir() + File.separatorChar
					+ stockCode;
			File ssp = new File(stockStorepath);
			if (!ssp.exists()) {
				ssp.mkdir();
			}
			HttpClient httpClient = new DefaultHttpClient();
			HttpHost httpHost = new HttpHost(props.getDhost(),
					Integer.parseInt(props.getDport()));
			int week;

			try {
				Calendar calendar = Calendar.getInstance();

				currentDate = new SimpleDateFormat("yyyy-MM-dd")
						.format(calendar.getTime());
				if (restDateList.contains(currentDate)) {
					log.info("{} is rest day!", currentDate);
					return;
				}

				tempUrl = props.getDurl().replace("[1]", currentDate)
						.replace("[2]", ss + stockCode);
				HttpGet httpGet = new HttpGet(tempUrl);
				HttpResponse response = httpClient.execute(httpHost, httpGet);
				// System.out.println(" line:" + k + ","
				// + response.getStatusLine().getStatusCode());
				if (HttpStatus.SC_OK == response.getStatusLine()
						.getStatusCode()) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						storeFile = new File(stockStorepath, stockCode + "."
								+ currentDate + ".txt");
						FileOutputStream output = new FileOutputStream(
								storeFile);
						InputStream input = entity.getContent();
						byte b[] = new byte[1024];
						int j = 0;
						while ((j = input.read(b)) != -1) {
							output.write(b, 0, j);
						}
						output.flush();
						output.close();
						log.info("code:{}  date:{} download finished",
								stockCode, currentDate);
					}
					httpGet.releaseConnection();
					
				} else {
					log.info(" {} {} no data", stockCode, currentDate);
					return;
				}

			} catch (IllegalStateException ex) {
				log.error(" {} {} IllegalStateException", stockCode,
						currentDate);
				return;
			}

			// httpClient.
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public int downloadData(String stockCode, int days) {
		File storeFile = null;
		String ss = "ss";
		String currentDate = "";
		if (stockCode.startsWith("60")) {
			ss = "sh";
		} else {
			ss = "sz";
		}
		String tempUrl = "";
		int dlRcdCounts = 0;
		try {
			String stockStorepath = props.getDLStoreDir() + File.separatorChar
					+ stockCode;
			File ssp = new File(stockStorepath);
			if (!ssp.exists()) {
				ssp.mkdir();
			}
			HttpClient httpClient = new DefaultHttpClient();
			HttpHost httpHost = new HttpHost(props.getDhost(),
					Integer.parseInt(props.getDport()));
			int week;

			Map<String, Object> stockMap = UKProp.getInstance().getStockMap();
			String firstDateStr = (String) stockMap.get(stockCode);

			Date firstDate = new SimpleDateFormat("yyyymmdd")
					.parse(firstDateStr);
			Date endDate = new SimpleDateFormat("yyyy-mm-dd").parse(UKProp
					.getInstance().getEndDate());
			int realdays = DateUtils.daysBefore(firstDate);
			if (realdays < days) {
				days = realdays;
			}
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(new Date().getTime());

			Calendar ecalendar = Calendar.getInstance();
			ecalendar.setTimeInMillis(endDate.getTime());
			for (int i = 0 - days; i <= 0; i++) {
				Thread.sleep(2);
				try {
					calendar.add(Calendar.DATE, i);
					currentDate = new SimpleDateFormat("yyyy-MM-dd")
							.format(calendar.getTime());
					if (restDateList.contains(currentDate)) {
						log.info("{} is rest day!", currentDate);
						continue;
					}
					week = calendar.get(Calendar.DAY_OF_WEEK);
					if (week < 2 || week > 6) {
						continue;
					}

					if (ecalendar.getTimeInMillis() < calendar
							.getTimeInMillis()) {
						log.info("EndDay {} is, currentDate is {},break!",
								UKProp.getInstance().getEndDate(), currentDate);
						break;
					}

					tempUrl = props.getDurl().replace("[1]", currentDate)
							.replace("[2]", ss + stockCode);
					HttpGet httpGet = new HttpGet(tempUrl);
					HttpResponse response = httpClient.execute(httpHost,
							httpGet);
					// System.out.println(" line:" + k + ","
					// + response.getStatusLine().getStatusCode());
					if (HttpStatus.SC_OK == response.getStatusLine()
							.getStatusCode()) {
						HttpEntity entity = response.getEntity();
						if (entity != null) {
							storeFile = new File(stockStorepath, stockCode
									+ "." + currentDate + ".txt");
							FileOutputStream output = new FileOutputStream(
									storeFile);
							InputStream input = entity.getContent();
							byte b[] = new byte[1024];
							int j = 0;
							while ((j = input.read(b)) != -1) {
								output.write(b, 0, j);
							}
							output.flush();
							output.close();
							log.info("code:{}  date:{} download finished",
									stockCode, currentDate);
						}
						dlRcdCounts++;
						httpGet.releaseConnection();
					} else {
						log.info(" {} {} no data", stockCode, currentDate);
						continue;
					}

				} catch (IllegalStateException ex) {
					log.error(" {} {} IllegalStateException", stockCode,
							currentDate);
					continue;
				}
			}

			// httpClient.
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return dlRcdCounts;
	}

}