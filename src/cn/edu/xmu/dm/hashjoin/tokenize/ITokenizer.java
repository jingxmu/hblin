package cn.edu.xmu.dm.hashjoin.tokenize;

import java.io.Serializable;
import java.util.List;

public interface ITokenizer extends Serializable {
    public List<String> tokenize(String text);
}

