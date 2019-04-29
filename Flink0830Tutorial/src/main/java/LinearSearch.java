class LinearSearch {
    public static void main(String[] args) {
        // 初始化了一亿个数
        int[] a = new int[100000000];
        for (int i = 0; i < a.length; ++i) {
            a[i] = i;
        }

        Long t1 = System.currentTimeMillis();
        linearSearch(a, 100000001);
        Long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);

    }

    // 数据库里的什么查找相当于线性查找？
    // select * from table where name = like "%abc%";
    // 查找了一亿次
    public static boolean linearSearch(int[] array, int target) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == target) {
                return true;
            }
        }
        return false;
    }
}