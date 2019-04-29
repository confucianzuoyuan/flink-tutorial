import java.util.Arrays;

// 来自《算法导论》
class InsertSort {
    public static void main(String[] args) {
        int[] A = {5, 2, 4, 6, 1, 3};
        insertSort(A);
        System.out.println(Arrays.toString(A));
    }
    // O(N^2)
    public static void insertSort(int[] A) {
        for (int j = 1; j < A.length; j++) {
            int key = A[j];                        // O(1)
            int i = j - 1;                         // O(1)
            while (i >= 0 && A[i] > key) {
                A[i + 1] = A[i];                   // O(1)
                i = i - 1;                         // O(1)
            }
            A[i + 1] = key;                        // O(1)
        }
    }
}