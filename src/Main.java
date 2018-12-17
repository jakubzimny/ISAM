import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Main {

    private static void parse(String s, ISAM isam){
        if (s.equals("")){
            return;
        }
        switch (s.charAt(0)){
            case 'i':
                String[] params = s.split(" ");
                if(params.length == 2)
                    isam.insert(new Record(params[1].toCharArray()));
                else System.err.println("Wrong \'i\' command format");
                break;
            case 'd':
                //delete()
                break;
            case 'u':
                //update()
                break;
            case 'r':
                isam.reorganize();
                break;
            case 'p':
                printDataFile(isam);
                break;
            case 'q':
                return;
            default:
                System.err.println("Command syntax unrecognized, try again.");
        }
    }

    public static void printDataFile(ISAM isam){
        ArrayList<Record> page;
        System.out.println("============ PRIMARY AREA =============");
        for(int i=1; i<=isam.getPagesNumber(); i++){
            page = isam.getPage(i);
            if(page.size() == 0)break;
            if(i> isam.getIndex().size())System.out.println("\n============ OVERFLOW AREA ============");
            System.out.println("Page "+i+ ": ");
            for(Record r: page){
                System.out.print("Key: "+r.getKey()+" | Data: "+ Arrays.toString((r.getCharset())));
                System.out.println(" | Offset Pointer: " +r.getOffsetPointer() + " | IsDeleted: "+r.isDeleted());
            }
        }
        System.out.println("\n================ INDEX ================");
        for(IndexEntry e: isam.getIndex()){
            System.out.println("Key: "+e.getKey()+" | Page Number: "+ e.getPageNo());
        }
    }

    public static void handleMenu(ISAM isam){
        System.out.println("List of available commands:");
        System.out.println("\'i\' followed by record content (string) -> Insert record containing specified value");
        System.out.println("\'d\' followed by key (integer) -> Delete record with specified key");
        System.out.println("\'p\' -> Print content of primary area, overflow area and index");
        System.out.println("\'r\' -> Force reorganization");
        System.out.println("\'u\' followed by key (integer) followed by record content (string)" +
                " -> Update record pointed by key with specified content");
        System.out.println("\'q\' -> Save changes to disk and quit program");
        Scanner in = new Scanner(System.in);
        String line;
        do {
            line = in.nextLine();
            parse(line, isam);
        }
        while(!line.equals("q"));
        isam.saveIndex();
    }

    public static void main(String[] args) {
        ISAM isam = new ISAM();
        handleMenu(isam);
    }


}
