package com.ultrakline.domian;

public class StockDayEntity {
	
	String day ="";
	int openDays=1;
	double preOpen = 0.0;
	double opens = 0.0;
	double top = 0.0;
	double low = 0.0;
	double cls = 0.0;
	double amont = 0.0;
	double vol = 0.0;
	
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public int getOpenDays() {
		return openDays;
	}
	public void setOpenDays(int openDays) {
		this.openDays = openDays;
	}
	public double getPreOpen() {
		return preOpen;
	}
	public void setPreOpen(double preOpen) {
		this.preOpen = preOpen;
	}
	public double getOpens() {
		return opens;
	}
	public void setOpens(double opens) {
		this.opens = opens;
	}
	public double getTop() {
		return top;
	}
	public void setTop(double top) {
		this.top = top;
	}
	public double getLow() {
		return low;
	}
	public void setLow(double low) {
		this.low = low;
	}
	public double getCls() {
		return cls;
	}
	public void setCls(double cls) {
		this.cls = cls;
	}
	public double getAmont() {
		return amont;
	}
	public void setAmont(double amont) {
		this.amont = amont;
	}
	public double getVol() {
		return vol;
	}
	public void setVol(double vol) {
		this.vol = vol;
	}
	
	
	

}
