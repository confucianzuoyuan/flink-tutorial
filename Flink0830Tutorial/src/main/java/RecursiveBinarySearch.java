class RecursiveBinarySearch {
    // 二分查找要求数组必须是拍好序的
    // 相当于数据库中对某个字段建索引
    public static void main(String[] args) {
        // 初始化了一亿个数
        int[] a = new int[100000000];
        for (int i = 0; i < a.length; ++i) {
            a[i] = i;
        }

        Long t1 = System.currentTimeMillis();
        System.out.println(recursiveBinarySearch(a, 100000001, 0, a.length - 1));
        Long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }

    // 假设二分查找的时间复杂度是 T(N)
    // T(N) = O(1) + T(N/2)
    public static boolean recursiveBinarySearch(int[] array, int target, int left, int right) {
        if (left <= right) {                                                            // O(1)
            int mid = (left + right) / 2;                                               // O(1)
            if (array[mid] < target) {                                                  // O(1)
                return recursiveBinarySearch(array, target, mid + 1, right);       // T(N/2)
            } else if (array[mid] > target) {                                           // O(1)
                return recursiveBinarySearch(array, target, left, mid - 1);       // T(N/2)
            } else {
                return true;                                                            // O(1)
            }
        }
        return false;                                                                   // O(1)
    }
}