package cn.edu.xmu.dm.hashjoin.tokenize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;


/**
 * 将字符串转换成由token组成的集合，每个token由长度为N的短串组成
 * @author Administrator
 *
 */
public class NGramTokenizer implements ITokenizer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static void main(String args[]) {
        ITokenizer tokenizer = new NGramTokenizer();
        String a = "haha";
        System.out.println(a + ":" + tokenizer.tokenize(a));
    }

    private final int gramLength;

    /**
     * padding used in q gram calculation.
     */
    private final char QGRAMENDPADDING = '$';

    /**
     * padding used in q gram calculation.
     */
//    private final char QGRAMSTARTPADDING = '$';

    public NGramTokenizer() {
        gramLength = 2;
    }

    public NGramTokenizer(int gramLength) {
        this.gramLength = gramLength;
    }

    private StringBuffer getAdjustedString(String input) {
        final StringBuffer adjustedString = new StringBuffer();
//        for (int i = 0; i < gramLength - 1; i++) {
//            adjustedString.append(QGRAMSTARTPADDING);
//        }
        adjustedString.append(input);
        for (int i = 0; i < gramLength - 1; i++) {
            adjustedString.append(QGRAMENDPADDING);
        }
        return adjustedString;
    }

    public List<String> tokenize(String input) {
        final ArrayList<String> returnVect = new ArrayList<String>();
        Pattern rePunctuation = Pattern.compile("[^:\\p{L}\\p{N}]");//标点符号统一换成空格
        input=rePunctuation.matcher(input).replaceAll(" ");
        final StringBuffer adjustedString = getAdjustedString(input);
//        
//        int index=adjustedString.indexOf(Main.WORD_SEPARATOR);
//        returnVect.add(adjustedString.substring(0,index));
//        if(index<0){
//        	throw new RuntimeException("generate record error");
//        }
        int curPos = 0;
        final int length = adjustedString.length() - (gramLength - 1);
        final HashMap<String, Integer> grams = new HashMap<String, Integer>();
        while (curPos < length) {
            final String term = adjustedString.substring(curPos, curPos
                    + gramLength);
            Integer count = grams.get(term);
            if (count == null) {
                count = new Integer(0);
            }
            count++;
            grams.put(term, count);
            returnVect.add(term);
            curPos++;
        }
        return returnVect;
    }
}
