package com.ultrakline.data.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.StringTokenizer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDXData {
	private Logger log = LoggerFactory.getLogger(TDXData.class);

	public void downloadTDXData(String dlType, String storePath) {
		HttpClient httpClient = new DefaultHttpClient();
		HttpHost httpHost = new HttpHost("www.tdx.com.cn", 80);
		StringTokenizer st = new StringTokenizer(dlType, ",");
		String tempZip = "";
		String storeDir = null;
		try {
			String rootPath = new File("").getAbsolutePath();
			if (storePath != null && storePath.length() > 1) {
				storeDir = storePath;
			} else {
				storeDir = rootPath + File.separator + "tdxData";
			}
			File ssp = new File(storeDir);
			if (!ssp.exists()) {
				ssp.mkdir();
			}
			File storeFile = null;
			while (st.hasMoreElements()) {
				tempZip = (String) st.nextElement();
				log.info(" {} download begin", tempZip);
				HttpGet httpGet = new HttpGet(
						"http://www.tdx.com.cn/products/data/data/vipdoc/" + tempZip
								+ ".zip");
				HttpResponse response = httpClient.execute(httpHost, httpGet);
				if (HttpStatus.SC_OK == response.getStatusLine()
						.getStatusCode()) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						storeFile = new File(storeDir, tempZip + ".zip");
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
						log.info("cdata {} download finished", tempZip);
					}
					httpGet.releaseConnection();
				}
			}
		} catch (Exception ex) {
			log.error(" download data error {}", ex.getMessage());
		}

	}
}