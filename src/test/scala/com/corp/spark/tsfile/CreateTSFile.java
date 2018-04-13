package com.corp.spark.tsfile;

import com.corp.tsfile.conf.TSFileDescriptor;
import com.corp.tsfile.constant.JsonFormatConstant;
import com.corp.tsfile.example.api.TSRecordWriteSupportV2;
import com.corp.tsfile.example.api.TSRecordWriter;
import com.corp.tsfile.file.metadata.enums.TSDataType;
import com.corp.tsfile.io.TSFileIOWriter;
import com.corp.tsfile.schema.FileSchema;
import com.corp.tsfile.timeseries.InternalRecordWriter;
import com.corp.tsfile.timeseries.TSRecord;
import com.corp.tsfile.timeseries.api.WriteSupport;
import com.corp.tsfile.utils.RandomAccessOutputStream;
import com.corp.tsfile.utils.RecordUtils;
import com.corp.tsfile.utils.TSRandomAccessFileWriter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by qjl on 16-9-18.
 */
public class CreateTSFile {

    private static final int ROW_COUNT = 10;

    public void createTSFile(String csvPath, String tsfilePath, String errorPath) throws Exception {
        File csvFile = new File(csvPath);
        File tsFile = new File(tsfilePath);
        File errortsFile = new File(errorPath);
        if(csvFile.exists()){
            csvFile.delete();
        }
        if (tsFile.exists()) {
            tsFile.delete();
        }
        if (errortsFile.exists()) {
            errortsFile.delete();
        }
		/*
		 * create the source file - test.csv
		 */
        createCSVFile(csvPath);
		/*
		 * create the tsfile
		 */
        JSONObject schema = generateTestSchema();
        FileSchema fileSchema = new FileSchema(schema);
        WriteSupport<TSRecord> support = new TSRecordWriteSupportV2(fileSchema);
        TSRandomAccessFileWriter tsRandomAccessFileWriter = new RandomAccessOutputStream(tsFile);
        TSRandomAccessFileWriter errorRandomAccessFileWriter = new RandomAccessOutputStream(errortsFile);
        TSFileIOWriter fileIOWriter = new TSFileIOWriter(fileSchema, tsRandomAccessFileWriter,
                errorRandomAccessFileWriter);
        InternalRecordWriter<TSRecord> internalRecordWriter = new TSRecordWriter(TSFileDescriptor.conf, fileIOWriter,
                support, fileSchema);
        createTestFile(fileSchema, internalRecordWriter, csvPath);
    }


    private void createTestFile(FileSchema schema, InternalRecordWriter<TSRecord> innerWriter, String path)
            throws IOException {
        Scanner in = getDataFile(path);
        while (in.hasNextLine()) {
            String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
            innerWriter.write(record);
        }
        innerWriter.close();

    }

    private Scanner getDataFile(String path) {
        File file = new File(path);
        try {
            Scanner in = new Scanner(file);
            return in;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void createCSVFile(String csvPath) throws IOException {
        File file = new File(csvPath);
        if (file.exists())
            return;
        FileWriter fw = new FileWriter(file);

        long startTime = System.currentTimeMillis();
        startTime = startTime - startTime % 1000;
        Random rm = new Random();
        for (int i = 0; i < ROW_COUNT; i++) {
            // write d1
            String d1 = "d1," + (startTime + i) + ",1,s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
            if (rm.nextInt(1000) < 100) {
                d1 = "d1," + (startTime + i) + ",1,s1,,s2," + (i * 10 + 2) + ",s4,HIGH";
            }
            if (i % 5 == 0)
                d1 += ",s3," + (i * 10 + 3);
            fw.write(d1 + "\r\n");

            // write d2
            String d2 = "d2," + (startTime + i) + ",2,s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3);
            if (rm.nextInt(1000) < 100) {
                d2 = "d2," + (startTime + i) + ",2,s2,,s3," + (i * 10 + 3) + ",s5,MAN";
            }
            if (i % 5 == 0)
                d2 += ",s1," + (i * 10 + 1);
            fw.write(d2 + "\r\n");
        }
        // write error
        String d =
                "d2,3," + (startTime + ROW_COUNT) + ",s2," + (ROW_COUNT * 10 + 2) + ",s3,"
                        + (ROW_COUNT * 10 + 3);
        fw.write(d + "\r\n");
        d = "d2," + (startTime + ROW_COUNT + 1) + ",2,s-1," + (ROW_COUNT * 10 + 2);
        fw.write(d + "\r\n");
        fw.close();

    }

    private JSONObject generateTestSchema() {
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.SENSOR_ID, "s1");
        s1.put(JsonFormatConstant.SENSOR_TYPE, TSDataType.INT64.toString());
        s1.put(JsonFormatConstant.SENSOR_ENCODING, TSFileDescriptor.conf.defaultSeriesEncoder.toString());
        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.SENSOR_ID, "s2");
        s2.put(JsonFormatConstant.SENSOR_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.SENSOR_ENCODING, TSFileDescriptor.conf.defaultSeriesEncoder.toString());
        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.SENSOR_ID, "s3");
        s3.put(JsonFormatConstant.SENSOR_TYPE, TSDataType.INT64.toString());
        s3.put(JsonFormatConstant.SENSOR_ENCODING, TSFileDescriptor.conf.defaultSeriesEncoder.toString());

        JSONArray fileArray = new JSONArray();
        JSONArray rowGroupArray = new JSONArray();
        JSONArray rowGroupArray2 = new JSONArray();

        JSONArray columnGroup1 = new JSONArray();
        columnGroup1.put(s1);
        columnGroup1.put(s2);
        rowGroupArray.put(columnGroup1);

        JSONArray columnGroup3 = new JSONArray();
        columnGroup3.put(s2);
        columnGroup3.put(s3);
        rowGroupArray2.put(columnGroup3);

        JSONObject rowGroup = new JSONObject();
        rowGroup.put(JsonFormatConstant.DEV_TYPE, "1");
        rowGroup.put(JsonFormatConstant.JSON_GROUPS, rowGroupArray);
        fileArray.put(rowGroup);

        JSONObject rowGroup2 = new JSONObject();
        rowGroup2.put(JsonFormatConstant.DEV_TYPE, "2");
        rowGroup2.put(JsonFormatConstant.JSON_GROUPS, rowGroupArray2);
        fileArray.put(rowGroup2);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, fileArray);
        return jsonSchema;
    }
}
