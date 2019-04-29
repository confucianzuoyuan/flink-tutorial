class BinarySearch {
    public static void main(String[] args) {
        // 初始化了一亿个数
        int[] a = new int[100000000];
        for (int i = 0; i < a.length; ++i) {
            a[i] = i;
        }

        Long t1 = System.currentTimeMillis();
        binarySearch(a, 100000001);
        Long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }

    public static boolean binarySearch(int[] array, int target) {
        int left = 0;
        int right = array.length - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            if (array[mid] < target) {
                left = mid + 1;
            } else if (array[mid] > target) {
                right = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }
}