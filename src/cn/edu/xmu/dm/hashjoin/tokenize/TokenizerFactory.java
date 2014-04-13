package cn.edu.xmu.dm.hashjoin.tokenize;

/**
 * 工厂模式的使用，根据用户输入的tokenizer类型选择特定的分词器
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class TokenizerFactory {
	
	/**
	 * 根据用户输入的tokenizer类型选择特定的分词器,目前可用的有NGram,NWord
	 * @param tokenizer 分词器类型
	 * @param gramLength token的长度
	 * @param wordSeparator 词与词之间的分隔符，默认为" "
	 * @param tokenSeparator token之间的分隔符，默认为" "
	 * @return
	 */
    public static ITokenizer getTokenizer(String tokenizer,int gramLength,
            String wordSeparator, String tokenSeparator) {
        if (tokenizer.equals("NGram")) {
            return new NGramTokenizer(gramLength);
        } else if(tokenizer.equals("NWord")){
        	return new NWordTokenizer(gramLength,wordSeparator,tokenSeparator);
        }
        throw new RuntimeException("Unknown tokenizer \"" + tokenizer + "\".");
    }
}

