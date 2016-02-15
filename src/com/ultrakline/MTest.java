package com.ultrakline;

public class MTest {

	public static void main(String[] args) {
		String ss = "119-23-108-6";
		
		StringBuffer hexBuf = new StringBuffer();
		String temp = "";
		String temp2 = "";
		String temp3 = "";
		for (int i = 0; i < ss.length(); i++) {
			//temp2 = temp2 + bs[i];
			//temp3 = temp3 + (bs[i] & 0xFF);
			temp = Integer.toHexString(ss.charAt(i) & 0xFF);
			if (temp.length() == 1)
				hexBuf.append("0");
			hexBuf.append(Integer.toHexString(ss.charAt(i) & 0xFF));
		}
		int result = Integer.valueOf(ss, 16);
		System.out.println(result);
	}

}
