import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Main {

    private static void parse(String s, ISAM isam) {
        if (s.equals("")) {
            return;
        }
        String[] params = s.split(" ");
        int readsBefore = isam.getReads(), writesBefore = isam.getWrites();
        switch (s.charAt(0)) {
            case 'i':
            if (params.length == 2) {
                if (params[1].length() == 1 && params[1].charAt(0) < '!') {
                    System.err.println("Invalid input character");
                    return;
                }
                isam.insert(new Record(params[1].toCharArray()), -1);
                System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
            } else System.err.println("Invalid \'i\' command format");
            break;
            case 'd':
            if (params.length == 2) {
                try {
                    isam.delete(Integer.parseInt(params[1]));
                    System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
                } catch (NumberFormatException ex) {
                    System.err.println("Key has to be an integer number");
                }
            } else System.err.println("Invalid \'d\' command format");
            break;
            case 'u':
            if (params.length == 3) {
                try {
                    isam.updateRecord(Integer.parseInt(params[1]), params[2], -1);
                    System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
                } catch (NumberFormatException ex) {
                    System.err.println("Key has to be an integer number");
                }
            }
//                else if(params.length == 4){
//                    try{
//                        isam.updateRecord(Integer.parseInt(params[1]), params[2], Integer.parseInt(params[3]));
//                    }catch (NumberFormatException ex){
//                        System.err.println("Key has to be an integer number");
//                    }
//                }
                else System.err.println("Invalid \'u\' command format");
            break;
            case 'e':
            if (params.length == 2) {
                executeCommandFile(params[1], isam);
            } else System.err.println("Invalid \'e\' command format");
            break;
            case 'f':
            if (params.length == 2) {
                try {
                    Record r = isam.fetchRecord(Integer.parseInt(params[1]));
                    if (r == null) System.err.println("There is no record with specified key.");
                        else {
                        System.out.print("Key: " + r.getKey() + " | Data: " + Arrays.toString((r.getCharset())));
                        System.out.println(" | Offset Pointer: " + r.getOffsetPointer() + " | IsDeleted: " + r.isDeleted());
                    }
                    System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
                } catch (NumberFormatException ex) {
                    System.err.println("Key has to be an integer number");
                }
            } else System.err.println("Invalid \'e\' command format");
            break;
            case 'g':
            if (params.length == 2) {
                try {
                    isam.insertRandomRecords(Integer.parseInt(params[1]));
                    System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
                } catch (NumberFormatException ex) {
                    System.err.println("Number of records has to be an integer number");
                }
            } else System.err.println("Invalid \'g\' command format");
            break;
            case 'r':
            isam.reorganize();
            System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
            break;
            case 'x':
                isam.deleteAll();
                System.out.println("Reads: " + (isam.getReads() - readsBefore) + " Writes: " + (isam.getWrites() - writesBefore));
                break;
            case 'p':
                printDataFile(isam);
                break;
            case 'h':
                printHelp();
                break;
            case 'q':
                return;
            default:
                System.err.println("Command syntax unrecognized, try again.");
        }
    }

    public static void printDataFile(ISAM isam) {
        ArrayList<Record> page;
        System.out.println("============ PRIMARY AREA =============");
        for (int i = 1; i <= isam.getPagesNumber(); i++) {
            page = isam.getPage(i);
            if (page.size() == 0) break;
            if (i > isam.getIndex().size()) System.out.println("\n============ OVERFLOW AREA ============");
            System.out.println("Page " + i + ": ");
            for (Record r : page) {
                System.out.print("Key: " + r.getKey() + " | Data: " + Arrays.toString((r.getCharset())));
                System.out.println(" | Offset Pointer: " + r.getOffsetPointer() + " | IsDeleted: " + r.isDeleted());
            }
        }
        System.out.println("\n================ INDEX ================");
        for (IndexEntry e : isam.getIndex()) {
            System.out.println("Key: " + e.getKey() + " | Page Number: " + e.getPageNo());
        }
    }

    public static void executeCommandFile(String fileName, ISAM isam) {
        String line;
        try (FileReader fr = new FileReader(fileName);
             BufferedReader br = new BufferedReader(fr)) {
            while ((line = br.readLine()) != null) {
                parse(line, isam);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Specified file doesn't exist");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void printHelp() {
        System.out.println("List of available commands:");
        System.out.println("\'i\' followed by record content (string) -> Insert record containing specified value");
        System.out.println("\'d\' followed by key (integer) -> Delete record with specified key");
        System.out.println("\'p\' -> Print content of primary area, overflow area and index");
        System.out.println("\'r\' -> Force reorganization");
        System.out.println("\'u\' followed by key (integer) followed by record content (string) followed" +
                " -> Update record pointed by key with specified content");
        System.out.println("\'e\' followed by filename -> Execute file containing commands");
        System.out.println("\'f\' followed by key (integer) -> Fetch record with specified key");
        System.out.println("\'h\' -> Print information about available commands");
        System.out.println("\'g\' followed by n (integer) -> Generate and insert n random records");
        System.out.println("\'q\' -> Save changes to disk and quit program");
    }

    public static void handleMenu(ISAM isam) {
        printHelp();
        Scanner in = new Scanner(System.in);
        String line;
        do {
            line = in.nextLine();
            parse(line, isam);
        }
        while (!line.equals("q"));
        isam.saveIndex();
    }

    public static void main(String[] args) {
        ISAM isam = new ISAM();
        handleMenu(isam);
    }


}
