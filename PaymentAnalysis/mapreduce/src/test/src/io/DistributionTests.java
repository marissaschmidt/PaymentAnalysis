package test.src.io;

public class DistributionTests {
	public static void main(String[] args) {

		int maxN = 15;
		int t = 1 << (maxN); // 2^maxN
		int ITERATIONS = 200;
		int n;
		
		for (int i = 0; i < ITERATIONS; i++) {
			n =  maxN - (int) Math.floor(-25.93 * Math.log((Math.random() * t) + 59.54) / Math.log(2));
			System.out.println(n);
		}
	}
}
