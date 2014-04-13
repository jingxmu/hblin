package cn.edu.xmu.dm.hashjoin.tokenize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 将字符串转换成由token组成的集合，每个token由N个单词组成
 * 
 * @author Administrator
 * 
 */
public class NWordTokenizer implements ITokenizer {

	private static final long serialVersionUID = 1L;

	public static void main(String args[]) {
		ITokenizer tokenizer = new NWordTokenizer();
		String a = "a   b    c d  ";
		System.out.println(a + ":" + tokenizer.tokenize(a));
	}

	private final int gramLength;
	private final String wordSeparator;
	private final String tokenSeparator;
	private final String QGRAMENDPADDING = "$";

	public NWordTokenizer() {
		// gramLength=3;
		this(1, " ", "_");
	}

	public NWordTokenizer(int gramLength, String wordSeperator,
			String tokenSeperator) {
		this.gramLength = gramLength;
		this.wordSeparator = wordSeperator;
		this.tokenSeparator = tokenSeperator;
	}

	public StringBuffer getAdjuestedString(String text) {
		StringBuffer adjuestedString = new StringBuffer();
		adjuestedString.append(text);
		// adjuestedString.append(wordSeparator);
		for (int i = 0; i < gramLength - 1; i++) {
			adjuestedString.append(wordSeparator + QGRAMENDPADDING);
		}
		return adjuestedString;
	}

	@Override
	public List<String> tokenize(String text) {

		final ArrayList<String> returnVect = new ArrayList<String>();
		final HashMap<String, Integer> NTokens = new HashMap<String, Integer>();
		Pattern rePunctuation = Pattern.compile("[^:\\p{L}\\p{N}]");        // 标点符号统一换成空格
		text = rePunctuation.matcher(text).replaceAll(wordSeparator);

		String adjuestedString = getAdjuestedString(text).toString();
		String[] tmp = adjuestedString.split(wordSeparator);
		int length = tmp.length - (gramLength - 1);
		int currentPos = 0;
		StringBuffer terms = new StringBuffer();
		// returnVect.add(tmp[0]);
		while (currentPos < length) {
			if (tmp[currentPos].length() != 0) {
				terms.replace(0, terms.length(), "");
				int k = gramLength;
				for (int i = currentPos; i < k + currentPos; i++) {
					if (tmp[i].length() == 0) {
						k++;
					} else {
						terms.append(tmp[i] + tokenSeparator);
					}
				}
				Integer count = NTokens.get(terms);
				if (count == null) {
					count = new Integer(0);
				}
				count++;
				NTokens.put(terms.toString(), count);
				returnVect.add(terms.toString());
			}
			currentPos++;
		}
		return returnVect;
	}

}
