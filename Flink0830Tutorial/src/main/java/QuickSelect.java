import java.util.Arrays;

class QuickSelect {
    public static void main(String[] args) {
        int[] arr = {2, 8, 7, 1, 3, 5, 6, 4};
        System.out.println(quickSelect(arr, 5,0, 7));
    }
    // T(N) = T(N/2) + O(N) = O(N)
    public static int quickSelect(int[] A, int k, int p, int r) {
        if (p <= r) {
            // q是哨兵的索引
            int q = partition(A, p, r); // O(N)
            if (k - 1 == q) {
                return A[q];
            } else if (k - 1 < q) {
                return quickSelect(A, k, p, q - 1); // T(N/2)
            } else {
                return quickSelect(A, k, q + 1, r); // T(N/2)
            }
        }
        return A[p];
    }
    public static int partition(int[] A, int p, int r) {
        int x = A[r];
        int i = p - 1;
        for (int j = p; j < r; j++) {
            // 降序排列
            if (A[j] >= x) {
                i = i + 1;
                swap(A, i, j);
            }
        }
        swap(A, i + 1, r);
        return i + 1;
    }
    public static void swap(int[] A, int i, int j) {
        int tmp = 0;
        tmp = A[i];
        A[i] = A[j];
        A[j] = tmp;
    }
}