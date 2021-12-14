package com.eff.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Read large file sequentially and count distinct values in specified column.
 *
 * @author Ram
 */
public class SampleFileReader {
    private static final Map<String, Integer> stringCountMap = new TreeMap<>();

    public static void main(String[] args) throws IOException {
        if (args.length > 1) {
            String fileName = args[0];
            int columnNumber = Integer.parseInt(args[1]);
            readFileAndCountDistinct(fileName, columnNumber);
        } else {
            System.out.println("Pass <FileLocation>, <ColumnPositionToReadFromFile> program arguments to the FileReader");
        }
    }

    private static void readFileAndCountDistinct(String fileName, int columnNumber) throws IOException {
        Instant instant = Instant.now();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                stringCount(line, columnNumber);
            }
        }
        System.out.println("Time taken - " + Duration.between(instant, Instant.now()).toSeconds());
        printCounts();
    }

    private static void stringCount(String line, int columnNumber) {
        String[] lineArr = line.split("\\s*\\|\\s*");
        stringCountMap.merge(lineArr[columnNumber], 1, Integer::sum);
    }

    private static void printCounts() {
        AtomicLong totalCount = new AtomicLong();
        stringCountMap.forEach((key, value) -> {
            totalCount.addAndGet(value);
            System.out.println("Key=" + key + ", Value=" + value);
        });
        System.out.println("TotalCount=" + totalCount);
    }

}

