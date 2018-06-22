package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.Constants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by admin on 2018/6/22.
 */
public class FileSink {
    private CSVPrinter csvPrinter;

    public FileSink(String filePath) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(Constants.NEW_LINE_SEPARATOR);
        FileWriter writer = new FileWriter(filePath);
        csvPrinter = new CSVPrinter(writer, format);
    }

    public void write(String[] headers, String[] line) throws IOException{
        csvPrinter.printRecord(headers);
        csvPrinter.printRecord(line);
    }

    public void append(String[] line) throws IOException {
        csvPrinter.printRecord(line);
    }


}
