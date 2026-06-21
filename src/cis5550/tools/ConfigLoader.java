package cis5550.tools;

import cis5550.kvs.KVSClient;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ConfigLoader {

    public static int loadFileToTable(String kvsCoordinator, String filePath,
                                       String tableName, String columnName)
            throws Exception {
        KVSClient kvs = new KVSClient(kvsCoordinator);
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        int count = 0;
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
            String rowKey = String.format("row%04d", count);
            kvs.put(tableName, rowKey, columnName, trimmed);
            count++;
        }
        return count;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: ConfigLoader <kvsCoordinator> <filePath> <tableName> <columnName>");
            System.exit(1);
        }
        int loaded = loadFileToTable(args[0], args[1], args[2], args[3]);
        System.out.println("Loaded " + loaded + " entries from " + args[1]
                           + " into table '" + args[2] + "' (column: " + args[3] + ")");
    }
}
