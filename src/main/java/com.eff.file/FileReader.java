package com.eff.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Read large file parallel using Completable Futures and count distinct values in specified column.
 *
 * @author Ram
 */
public class FileReader {
    private static Map<String, Integer> stringCountMap = new TreeMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length > 1) {
            String fileName = args[0];
            int columnNumber = Integer.parseInt(args[1]);
            readFileAndCountDistinct(fileName, columnNumber);
        } else {
            System.out.println("Pass <FileLocation>, <ColumnPositionToReadFromFile> program arguments to the FileReader");
        }
    }

    private static void readFileAndCountDistinct(String fileName, int columnNumber) throws IOException, ExecutionException, InterruptedException {
        Instant instant = Instant.now();
        List<String> lines = new ArrayList<>();
        List<CompletableFuture<Map<String, Integer>>> futures = new ArrayList<>();
        Files.lines(Path.of(fileName))
                .forEach(ln -> {
                    lines.add(ln);
                    if (lines.size() % 10000 == 0) {
                        futures.add(CompletableFuture.supplyAsync(stringCountSupplier(new ArrayList<>(lines), columnNumber)));
                        lines.clear();
                    }
                });

        if (lines.size() > 0) {
            futures.add(CompletableFuture.supplyAsync(stringCountSupplier(lines, columnNumber)));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                .thenApply($ -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()))
                .thenAccept(maps -> maps.forEach(map -> map.forEach((key, val) -> {
                    if (stringCountMap.containsKey(key)) {
                        stringCountMap.put(key, stringCountMap.get(key) + val);
                    } else
                        stringCountMap.put(key, val);
                })))
                .get();

        System.out.println("Time taken - " + Duration.between(instant, Instant.now()).toSeconds());
        printCounts();
    }

    private static Supplier<Map<String, Integer>> stringCountSupplier(List<String> lines, int columnNumber) {
        Map<String, Integer> map = new HashMap<>();
        return () -> {
            lines.forEach(line -> {
                String[] lineArr = line.split("\\s*\\|\\s*");
                map.merge(lineArr[columnNumber], 1, Integer::sum);
            });
            return map;
        };
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

