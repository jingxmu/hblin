package cn.edu.xmu.dm.hashjoin.minhashing;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 使用(ax+b)mod n, gcd(a,n)=1 来产生[0,n-1]之间的随机排列
 * @version 2013-5-9
 * @author Administrator
 * @Reviewer
 *
 */
public class HashMatrix {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		getHashFunction(548, 400, "/home/dm/test", conf);
	}

	/**
	 * 为了对[0,tokensNum-1]的数进行随机排序，使用(ax+b)mod tokensNum进行模拟
	 * 该函数产生满足条件的a,b,并将其写入文件，以便之后使用
	 * @param tokensNum token的总数
	 * @param hashNum   需要的哈希次数，即哈希签名的长度
	 * @param outpath	产生的hashNum对a,b写入到outpath指定的文件中
	 * @param conf		传递所需的运行参数
	 * @throws IOException
	 */
	public static void getHashFunction(int tokensNum, int hashNum,
			String outpath, Configuration conf)
			throws IOException {

		Path path = new Path(outpath);
		FileSystem fs = FileSystem.get(URI.create(outpath), conf);
		
		/* 如果path指定的文件存在，则将其删除，如果不进行此操作，无法自动覆盖该文件，会使程序出错  */
		FileSystem.get(conf).delete(path, true);
		FSDataOutputStream out = fs.create(path);
		Random r = new Random(100);
		int totalHash = hashNum;
		int copyOftoken = tokensNum;
		int primeFactorNum = 0;
		double primeNum = tokensNum;
		List<Integer> factors = new LinkedList<Integer>();
		int[] factorOrder = new int[totalHash];
		boolean flag = true;
		Set<Integer> order = new HashSet<Integer>();
		int temp;
		int n = -1;
		int k = 0;
		
		/*
		 *array中的第k个元素存放的是第k个与tokensNum互质的数,作为ai+b mod tokensNum的a
		 *bArray中的第k个元素存放的是0到tokensNum-1的数,作为ai+b mod tokensNum的b
		 */
		int[] array = new int[totalHash];
		int[] bArray = new int[totalHash];
		Set<Integer> bSet = new HashSet<Integer>();
		
		/* factors中存放的是tokensNum的质因子，这是为了找到与tokensNum互质的数时使用  */
		for (int i = 2; copyOftoken != 1; ++i) {
			if (copyOftoken % i == 0) {
				factors.add(i);
				while (copyOftoken % i == 0) {
					copyOftoken /= i;
				}
			}
		}
		
		/* 
		 * 根据欧拉函数计算出小于tokensNum且与其互质的数个数primeFactorNum
		 * primeFactorNum=tokensNum(1-1/factors(1))(1-1/factors(2))...(1-1/factors(n))
		 */
		for (Integer f : factors) {
			primeNum *= 1.0 * (f - 1) / f;
		}
		primeFactorNum = (int) primeNum;
		
		/*
		 *  随机产生totalHash个数存入factorOrder中，totalHash在[0,primeFactorNum-1]之间，
		 * factorOrder(i)的值代表第factorOrder(i)个与tokensNum互质的数
		 */
		for (int i = 0; i < totalHash; ++i) {
			temp = Math.abs(r.nextInt(primeFactorNum));

			while (order.size() < primeFactorNum && order.contains(temp)) {
				temp = Math.abs(r.nextInt(primeFactorNum));
			}
			order.add(temp);
			factorOrder[i] = temp;
		}
		Arrays.sort(factorOrder);
		
		/* 产生totalHash个不同的b，b在[0,tokensNum-1]之间 */
		for (int i = 0; i < totalHash; i++) {
			temp = Math.abs(r.nextInt(tokensNum));
			while (bSet.contains(temp)) {
				temp = Math.abs(r.nextInt(tokensNum));
			}
			bArray[i] = temp;
		}
		
		/* 产生totalHash个与tokensNum互质的a */
		for (int i = 1; i < tokensNum; i++) {
			flag = true;
			for (Integer f : factors) {
				if (i % f == 0) {
					flag = false;
					break;
				}
			}
			if (flag) {
				++n;
				if (n == factorOrder[k]) {
					array[k] = i;
					k++;
					while (k < factorOrder.length && n == factorOrder[k]) {
						array[k] = i;
						k++;
					}
					if (k == factorOrder.length) {
						break;
					}
				}
			}
		}

		/* 将产生的a,b写入文件中，在MinHashing时使用 */
		for (int i = 0; i < totalHash; i++) {
			out.writeInt(array[i]);
			out.writeInt(bArray[i]);
		}
		out.close();
	}
}
