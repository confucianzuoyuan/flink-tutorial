import java.util.HashMap;

class ClimbingStairs {
    public static void main(String[] args) {
        Long t1 = System.currentTimeMillis();
        System.out.println(dpfibonacci(50));
        Long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }

    // 假设时间复杂度是 T(N) = T(N-1) + T(N-2) = ?
    public static long fibonacci(int N) {
        if (N == 0) return 1;
        if (N == 1) return 1;
        if (N == 2) return 2;
        // 时间复杂度： T(N-1) + T(N-2)
        return fibonacci(N - 1) + fibonacci(N - 2);
    }

    // 动态规划的核心思想：缓存，不重复计算
    // 时间复杂度：O(N)
    public static long dpfibonacci(int N) {
        int[] arr = new int[N + 1];
        arr[0] = 1;
        arr[1] = 1;
        arr[2] = 2;
        for (int i = 3; i < N + 1; i++) {
            arr[i] = arr[i - 1] + arr[i - 2];
        }
        return arr[N];
    }

    public static HashMap<Integer, Long> cache = new HashMap<>();

    public static Long linearfibonacci(int N) {
        if (!cache.containsKey(N)) {
            cache.put(N, _fib(N));
        }
        return cache.get(N);
    }

    public static Long _fib(int N) {
        if (N == 1) {
            return 1L;
        }
        if (N == 2) {
            return 2L;
        }
        return linearfibonacci(N - 1) + linearfibonacci(N - 2);
    }
}