package whu.edu.cn.ogc.ogcAPIUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;


public class DataTimeUtil {
    private String getDateFormat(String str) {
        boolean year = false;
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        if(pattern.matcher(str.substring(0, 4)).matches()) {
            year = true;
        }
        StringBuilder sb = new StringBuilder();
        int index = 0;
        if(!year) {
            if(str.contains("月") || str.contains("-") || str.contains("/")) {
                if(Character.isDigit(str.charAt(0))) {
                    index = 1;
                }
            }else {
                index = 3;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char chr = str.charAt(i);
            if(Character.isDigit(chr)) {
                if(index==0) {
                    sb.append("y");
                }
                if(index==1) {
                    sb.append("M");
                }
                if(index==2) {
                    sb.append("d");
                }
                if(index==3) {
                    sb.append("H");
                }
                if(index==4) {
                    sb.append("m");
                }
                if(index==5) {
                    sb.append("s");
                }
                if(index==6) {
                    sb.append("S");
                }
            }else {
                if(i>0) {
                    char lastChar = str.charAt(i-1);
                    if(Character.isDigit(lastChar)) {
                        index++;
                    }
                }
                sb.append(chr);
            }
        }
        //ISO-8601时间
        if(sb.toString().equals("yyyy-MM-ddTHH:mm:ss+SS:")){
            return "yyyy-MM-dd'T'HH:mm:ssXXX";
        }else{
            return sb.toString();
        }
    }

    public Date str2Date(String dateStr) throws ParseException {
        String format = getDateFormat(dateStr);
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.parse(dateStr);
    }

    public boolean isHavePublicTime(List<String> time1, List<String> time2) throws ParseException {
        if(time2.get(0) == null){
            time2.set(0, Long.MIN_VALUE + "");
        }
        if(time2.get(1) == null){
            time2.set(1, Long.MAX_VALUE + "");
        }
        Date leftStartDate = str2Date(time1.get(0));
        Date rightStartDate = str2Date(time2.get(0));
        Date leftEndDate = str2Date(time1.get(1));
        Date rightEndDate = str2Date(time2.get(1));
        return ((leftStartDate.getTime() >= rightStartDate.getTime())
                && leftStartDate.getTime() < rightEndDate.getTime())
                ||
                ((rightStartDate.getTime() >= leftStartDate.getTime())
                        && rightStartDate.getTime() < leftEndDate.getTime())
                ||
                ((rightStartDate.getTime() == leftStartDate.getTime())
                        && rightEndDate.getTime() == leftEndDate.getTime());
    }
}
