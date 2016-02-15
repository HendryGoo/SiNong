package com.ultrakline.parse;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ultrakline.UKProp;
import com.ultrakline.domian.StockDayEntity;

public class ParseTDXDayFile {
	private Logger log = LoggerFactory.getLogger(ParseTDXDayFile.class);
	private UKProp ukprop = UKProp.getInstance();

	public void parse(File sFile, String stockCode) {
		try {
			String fileName = sFile.getName();
			String kdayDataDir = sFile.getParentFile().getParentFile().getAbsolutePath() + File.separator + "kdayData";
			File kdayFile = new File(kdayDataDir, fileName.replace(".day", ".csv"));
			File sqlFile = new File(ukprop.getSqlStoreDir(), fileName.replace(".day", ".sql"));
			File jsonFile = new File(ukprop.getJsonStoreDir(), fileName.replace(".day", ".js"));
			log.info("parse day file {} begin", fileName);
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(sFile)));
			byte[] buf = new byte[32];
			int preClose = 0;
			StringBuffer csvSB = new StringBuffer();
			StringBuffer sqlSB = new StringBuffer();
			StringBuffer jsonDataSB = new StringBuffer();
			String tmpSQL = "";
			jsonDataSB.append("var data=[\r\n");
			long days = sFile.length() / 32;
			log.info(" days is {}", days);
			String dateLine = "";
			String[] result = new String[5];
			// Map<String,String> firstDayMap=new HashMap<String,String>();

			for (long i = 0; i < days; i++) {

				dis.read(buf);
				result = splitDayData(buf);
				if (i == 0) {
					log.info(" stock is {} start date is {}", stockCode, result[4]);
					ukprop.putStartDate(stockCode, result[4]);
				}

				dateLine = result[0].replace("$preClose", preClose + "");
				tmpSQL = result[1].replace("$stockCode", stockCode);
				tmpSQL = tmpSQL.replace("$no", (i + 1) + "");
				tmpSQL = tmpSQL.replace("$preClose", preClose + "");
				sqlSB.append(tmpSQL).append("\r\n");
				csvSB.append(stockCode).append(",");
				csvSB.append(dateLine).append("\r\n");
				if (i == days - 1) {
					jsonDataSB.append(result[2].replace("$preClose", preClose + "")).append("\r\n");
				} else {
					jsonDataSB.append(result[2].replace("$preClose", preClose + "")).append(",").append("\r\n");
				}
				log.debug("[{}] -->[{} data is [{}] json data is {}", stockCode, i, dateLine, result[2]);
				preClose = Integer.parseInt(result[3]);
			}
			jsonDataSB.append("\r\n];");
			dis.close();
			// ///

			log.info("parse day file {} End", fileName);
			FileWriter fw = new FileWriter(kdayFile);
			csvSB.append("commit;\r\n");
			fw.write(csvSB.toString());
			fw.close();
			log.info(" csv convert finished [{}]  -->[{}]  ", fileName, kdayFile.getName());
			FileWriter sqlfw = new FileWriter(sqlFile);
			sqlfw.write(sqlSB.toString());
			sqlfw.close();
			log.info(" sql convert finished [{}]  -->[{}]  ", fileName, sqlFile.getName());
			FileWriter jsonfw = new FileWriter(jsonFile);
			jsonfw.write(jsonDataSB.toString());
			jsonfw.close();
			log.info(" sql convert finished [{}]  -->[{}]  ", fileName, jsonFile.getName());

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public int convert(byte[] bs) {
		StringBuffer hexBuf = new StringBuffer();
		String temp = "";
		String temp2 = "";
		String temp3 = "";
		for (int i = 0; i < bs.length; i++) {
			temp2 = temp2 + bs[i];
			temp3 = temp3 + (bs[i] & 0xFF);
			temp = Integer.toHexString(bs[i] & 0xFF);
			if (temp.length() == 1)
				hexBuf.append("0");
			hexBuf.append(Integer.toHexString(bs[i] & 0xFF));
		}

		int result = 0;
		try{
			result = Integer.valueOf(hexBuf.toString(), 16);
		}catch(Exception ex){
			log.error(" orig Byte is {} ,t3 is {} byte is {},date is {} ", temp2, temp3, hexBuf.toString(), result);
			
		}
		
		return result;
	}

	public String[] splitDayData(byte[] buf) {
		// TODO
		// StockDayEntity sde=new StockDayEntity();

		String[] result = new String[5];
		// TODO read from properties
		StringBuffer sqlSB = new StringBuffer(
				"insert into stock_ex_day (stock_code,ex_days,ex_date,pre_close,ex_open,ex_top,ex_low,ex_close,ex_amount,ex_vol) values (");
		sqlSB.append("'$stockCode',$no,");
		String openDay = "";
		StringBuffer dlsb = new StringBuffer();
		StringBuffer jsonSB = new StringBuffer();
		// //////////////////
		byte[] day = new byte[4];
		byte[] opens = new byte[4];
		byte[] top = new byte[4];
		byte[] low = new byte[4];
		byte[] cls = new byte[4];
		byte[] amont = new byte[4];
		byte[] vol = new byte[4];

		day[0] = buf[3];
		day[1] = buf[2];
		day[2] = buf[1];
		day[3] = buf[0];
		int days = convert(day);
		openDay = formatData(days);
		dlsb.append(days).append(",$preClose,");
		sqlSB.append(days).append(",$preClose,");
		jsonSB.append("[").append(formatData(days)).append(",$preClose,");
		// System.out.println(i+":"+buf[0]+" H:"+hex);
		// System.out.println(i +);
		///
		opens[0] = buf[7];
		opens[1] = buf[6];
		opens[2] = buf[5];
		opens[3] = buf[4];
		int openPrice = convert(opens);
		dlsb.append(openPrice).append(",");
		sqlSB.append(openPrice).append(",");
		jsonSB.append(openPrice).append(",");
		// ////////

		top[0] = buf[11];
		top[1] = buf[10];
		top[2] = buf[9];
		top[3] = buf[8];
		int topPrice = convert(top);
		dlsb.append(topPrice).append(",");
		sqlSB.append(topPrice).append(",");
		jsonSB.append(topPrice).append(",");
		// /
		low[0] = buf[15];
		low[1] = buf[14];
		low[2] = buf[13];
		low[3] = buf[12];
		int lowPrice = convert(low);
		dlsb.append(lowPrice).append(",");
		sqlSB.append(lowPrice).append(",");
		jsonSB.append(lowPrice).append(",");
		//
		cls[0] = buf[19];
		cls[1] = buf[18];
		cls[2] = buf[17];
		cls[3] = buf[16];
		int closePrice = convert(cls);
		dlsb.append(closePrice).append(",");
		sqlSB.append(closePrice).append(",");
		jsonSB.append(closePrice).append(",");
		// ////////////
		amont[0] = buf[23];
		amont[1] = buf[22];
		amont[2] = buf[21];
		amont[3] = buf[20];
		int total = convert(amont);
		dlsb.append(total).append(",");
		sqlSB.append(total).append(",");
		jsonSB.append(total).append(",");
		// /
		vol[0] = buf[27];
		vol[1] = buf[26];
		vol[2] = buf[25];
		vol[3] = buf[24];
		int voltotal = convert(vol);
		dlsb.append(voltotal);
		sqlSB.append(voltotal).append(");");
		jsonSB.append(voltotal).append("]");
		// ////////////////////

		result[0] = dlsb.toString();
		result[1] = sqlSB.toString();
		result[2] = jsonSB.toString();
		result[3] = closePrice + "";
		result[4] = openDay;
		return result;
	}

	public String formatData(int dateInt) {
		String temp = dateInt + "";
		String result = dateInt / 10000 + "-" + temp.substring(4, 6) + "-" + temp.substring(6);

		return result;
	}
}
