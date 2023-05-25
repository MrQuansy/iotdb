package org.apache.iotdb;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.io.WALReader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WALAnalyzer {

  public static void main(String[] args) throws IOException {
    String dirName = "";
    if (args.length == 2) {
      dirName = args[1];
    }

    FileWriter writer = new FileWriter("result.txt");

    Map<File, List<File>> walFiles = getAllWalFileList(dirName);

    for (Map.Entry<File, List<File>> entry : walFiles.entrySet()) {
      String regionName = entry.getKey().getName();
      writer.write("RegionName:");
      writer.write(regionName + '\n');

      for (File walFile : entry.getValue()) {
        analysisWalFile(walFile, writer);
      }
    }
    writer.close();
  }

  public static Map<File, List<File>> getAllWalFileList(String dirName) {
    Map<File, List<File>> result = new HashMap<>();
    File currentDir = new File(dirName);
    File[] files = currentDir.listFiles();
    if (files != null) {
      for (File f : files) {
        List<File> walFileList = new ArrayList<>();
        File[] walFiles = f.listFiles();
        if (walFiles != null) {
          for (File walFile : walFiles) {
            String fileName = walFile.getName();
            int index = fileName.lastIndexOf('.');
            if (index > 0 && index < fileName.length() - 1) {
              if (fileName.substring(index + 1).equals("wal")) {
                walFileList.add(walFile);
              }
            }
          }
        }
        if (!walFileList.isEmpty()) {
          result.put(f, walFileList);
        }
      }
    }
    return result;
  }

  public static void analysisWalFile(File file, FileWriter writer) throws IOException {
    WALReader walReader = new WALReader(file);
    while (walReader.hasNext()) {
      WALEntry walEntry = walReader.next();
      writer.write(String.valueOf(walEntry.getType().getCode()));
      writer.write(" ");
      writer.write(String.valueOf(walEntry.getMemTableId()));
      writer.write(" ");

      switch (walEntry.getType()) {
        case INSERT_ROW_NODE:
          InsertRowNode insertRowNode = (InsertRowNode) walEntry.getValue();
          writer.write(insertRowNode.getDevicePath().getFullPath());
          writer.write('\n');
          break;
        case INSERT_TABLET_NODE:
          InsertTabletNode insertTabletNode = (InsertTabletNode) walEntry.getValue();
          writer.write(insertTabletNode.getDevicePath().getFullPath());
          writer.write('\n');
          break;
      }
    }
  }
}
