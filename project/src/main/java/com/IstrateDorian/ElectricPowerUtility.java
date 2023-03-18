package com.IstrateDorian;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.stream.Stream;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.text.DateFormatSymbols;

public class ElectricPowerUtility implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String dateTime;
    private float temp, humid, windSpd, generalDif, dif, pcz1, pcz2, pcz3;
    private int weekH,yearH;

    public ElectricPowerUtility() {};

    public ElectricPowerUtility(String dateTime, float temp, float humid, float windSpd, float generalDif, float dif, float pcz1, float pcz2, float pcz3, int weekH, int yearH) {
        this.dateTime = dateTime;
        this.temp = temp;
        this.humid = humid;
        this.windSpd = windSpd;
        this.generalDif = generalDif;
        this.dif = dif;
        this.pcz1 = pcz1;
        this.pcz2 = pcz2;
        this.pcz3 = pcz3;
        this.weekH = weekH;
        this.yearH = yearH;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    public float getHumid() {
        return humid;
    }

    public void setHumid(float humid) {
        this.humid = humid;
    }

    public float getWindSpd() {
        return windSpd;
    }

    public void setWindSpd(float windSpd) {
        this.windSpd = windSpd;
    }

    public float getGeneralDif() {
        return generalDif;
    }

    public void setGeneralDif(float generalDif) {
        this.generalDif = generalDif;
    }

    public float getDif() {
        return dif;
    }

    public void setDif(float dif) {
        this.dif = dif;
    }

    public float getPcz1() {
        return pcz1;
    }

    public void setPcz1(float pcz1) {
        this.pcz1 = pcz1;
    }

    public float getPcz2() {
        return pcz2;
    }

    public void setPcz2(float pcz2) {
        this.pcz2 = pcz2;
    }

    public float getPcz3() {
        return pcz3;
    }

    public void setPcz3(float pcz3) {
        this.pcz3 = pcz3;
    }

    public int getWeekH() {
        return weekH;
    }

    public void setWeekH(int weekH) {
        this.weekH = weekH;
    }

    public int getYearH() {
        return yearH;
    }

    public void setYearH(int yearH) {
        this.yearH = yearH;
    }

    public String getRowRecord() {
        return new StringBuilder().append(this.temp).append(this.humid).append(this.windSpd)
                .append(this.generalDif).append(this.dif).append(this.pcz1).append(this.pcz2)
        .append(this.pcz3).toString();
    }

    public void setRowRecord(float  pcz3) {
        this.pcz3= pcz3;
    }

    public List<Float> getElectricPVector()
    {
        List<Float> numbers = new ArrayList<Float>();
        numbers.add(this.temp);
        numbers.add(this.humid);
        numbers.add(this.windSpd);
        numbers.add(this.generalDif);
        numbers.add(this.dif);
        numbers.add(this.pcz1);
        numbers.add(this.pcz2);
        numbers.add(this.pcz3);
        return(numbers);
    }

    public static String getChoiceAnalyzer(String str, int choice, int c, int i)
    {
        String[] fields = str.split(";");
        if (fields.length != 9)
        {
            System.out.println("The elements are ::" );
            Stream.of(fields).forEach(System.out::println);
            throw new IllegalArgumentException
                    ("Each line must contain 9 fields while the current line has ::"+fields.length);
        }
        String [] full_date = fields[c].trim().split("/");
        if (c==1)
        {
            full_date = fields[c].trim().split(":");
        }
        int selected_value = Integer.parseInt(full_date[i]);
        float attr_value = Float.parseFloat(fields[choice].trim());

        String to_send = selected_value + "," + attr_value;

        return to_send;

    }

    public static ElectricPowerUtility parseRecord(String str)
    {
        String[] fields = str.split(";");
        if (fields.length != 9)
        {
            System.out.println("The elements are ::" );
            Stream.of(fields).forEach(System.out::println);
            throw new IllegalArgumentException
                    ("Each line must contain 9 fields while the current line has ::"+fields.length);
        }
        int weekR = 0;
        int yearR =1900;
        String dateR = fields[0].trim();
        String format = "d/M/yy";
        SimpleDateFormat df = new SimpleDateFormat(format);
        try {
            Date dateRParse = df.parse(fields[0].trim());
            Calendar cal = Calendar.getInstance();
            cal.setTime(dateRParse);
            weekR = cal.get(Calendar.WEEK_OF_YEAR);
            yearR = cal.get(Calendar.YEAR);
        } catch (ParseException e) {
            e.printStackTrace();}
        float temp = Float.parseFloat(fields[1].trim());
        float humid = Float.parseFloat(fields[2].trim());
        float windSpd = Float.parseFloat(fields[3].trim());
        float generalDif = Float.parseFloat(fields[4].trim());
        float dif = Float.parseFloat(fields[5].trim());
        float pcz1 = Float.parseFloat(fields[6].trim());
        float pcz2 = Float.parseFloat(fields[7].trim());
        float pcz3 = Float.parseFloat(fields[7].trim());
        return new ElectricPowerUtility(dateR, temp, humid, windSpd, generalDif,
                dif, pcz1, pcz2, pcz3, yearR, weekR);
    }
}
