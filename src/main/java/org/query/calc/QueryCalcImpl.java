package org.query.calc;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;
import static java.lang.Double.valueOf;
import static java.math.RoundingMode.HALF_UP;

public class QueryCalcImpl implements QueryCalc {

    private static final String SPACE = " ";

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
        //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
        //  x respectively.See test resources for examples.
        // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
        // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
        // - output is table stored in the same format: first line is a number of rows, then each line is one row that
        //  contains two numbers: value for column a and s.
        //
        // Number of rows of all three tables lays in range [0, 1_000_000].
        // It's guaranteed that full content of all three tables fits into RAM.
        // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
        //
        // TODO: Implement following query, put a reasonable effort into making it efficiently from perspective of
        //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
        //  from a maven central.
        //
        // SELECT a, SUM(x * y * z) as s
        // FROM t1
        // JOIN t2
        // JOIN t3
        // WHERE a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // Limit 10
        // 
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order. 
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.

        Double[][] a1 = mapFileToArray(t1);
        Double[][] a2 = mapFileToArray(t2);
        Double[][] a3 = mapFileToArray(t3);

        var result = new HashMap<Double, Double>();

        for (Double[] ax : a1) {
            for (Double[] by : a2) {
                for (Double[] cz : a3) {
                    if (ax[0] < by[0] + cz[0]) {
                        if (result.computeIfPresent(ax[0], (key, val) -> val + (ax[1] * by[1] * cz[1])) == null) {
                            result.put(ax[0], (ax[1] * by[1] * cz[1]));
                        }
                    }
                }
            }
        }

        var sortedMap =
                result.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(10)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));


        try (var fileWriter = new FileWriter(output.toFile());
             var printWriter = new PrintWriter(fileWriter)) {
            printWriter.println(sortedMap.size());
            sortedMap.entrySet().forEach(
                    e -> {
                        var truncatedDouble = BigDecimal.valueOf(e.getValue())
                                .setScale(6, HALF_UP)
                                .doubleValue();
                        printWriter.println(e.getKey() + SPACE + truncatedDouble);
                    }
            );
        }
    }

    private Double[][] mapFileToArray(Path p) throws IOException {
        Double[][] result = null;

        try (var scanner = new Scanner(p)) {
            var i = 0;
            while (scanner.hasNextLine()) {
                String[] columns = scanner.nextLine().split(SPACE);

                if (i == 0 && columns.length == 1) {
                    final var count = valueOf(columns[0]);
                    result = new Double[count.intValue()][2];
                    continue;
                }
                result[i][0] = parseDouble(columns[0]);
                result[i][1] = parseDouble(columns[1]);
                i++;
            }
        }
        return result;
    }
}
