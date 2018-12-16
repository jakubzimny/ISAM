import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Main {

    public static void parse(String s, ISAM isam){
        switch (s.charAt(0)){
            case 'i':
                isam.insert(new Record(s.substring(2).toCharArray()));
                break;
            case 'r':
                //remove()
                break;
            case 'u':
                //update()
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
        System.out.println("============ PRIMARY AREA ============");
        for(int i=1; i<=isam.getPagesNumber(); i++){
            page = isam.getPage(i);
            if(page.size() == 0)break;
            if(i> isam.getIndex().size())System.out.println("============ OVERFLOW AREA ============");
            System.out.println("Page "+i+ ": ");
            for(Record r: page){
                System.out.print("Key: "+r.getKey()+" Data: "+ Arrays.toString((r.getCharset())));
                System.out.println(" Offset Pointer: " +r.getOffsetPointer() + " IsDeleted: "+r.isDeleted());
            }
        }
    }

    public static void handleMenu(ISAM isam){
        Scanner in = new Scanner(System.in);
        String line;
        do {
            line = in.nextLine();
            parse(line, isam);
        }
        while(!line.equals("q"));
    }

    public static void main(String[] args) {
        ISAM isam = new ISAM();
        handleMenu(isam);
        //ArrayList<Record> page = b.getPage(0);
    }


}
