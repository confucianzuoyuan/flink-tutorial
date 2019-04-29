import java.util.Arrays;

class QuickSort {
    public static void main(String[] args) {
        int[] arr = {2, 8, 7, 1, 3, 5, 6, 4};
        quicksort(arr, 0, 7);
        System.out.println(Arrays.toString(arr));
    }

    // T(N) = 2T(N/2) + O(N)
    public static void quicksort(int[] A, int p, int r) {
        if (p < r) {
            int q = partition(A, p, r);     // O(N)
            quicksort(A, p, q - 1);      // T(N/2)
            quicksort(A, q + 1, r);      // T(N/2)
        }
    }

    public static int partition(int[] A, int p, int r) {
        int x = A[r];
        int i = p - 1;
        for (int j = p; j < r; j++) {
            if (A[j] <= x) {
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