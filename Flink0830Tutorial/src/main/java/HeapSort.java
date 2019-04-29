import java.util.Arrays;

class HeapSort {
    public static int heapsize; // 堆的大小，初始值为 0

    public static void main(String[] args) {
        int[] array = {0,9,8,7,6,5,4,3,2,1};
        heapSort(array, array.length);
        System.out.println(Arrays.toString(array));
    }
    // 用来维护最大堆的性质，以下标为 i 的元素为根节点的最大堆的性质
    // 时间复杂度： O(logN)
    public static void maxHeapify(int[] arr, int i) {
        int l = 2 * i, r = 2 * i + 1, largest;
        if (l < heapsize && arr[l] > arr[i]) largest = l;
        else largest = i;
        if (r < heapsize && arr[r] > arr[largest]) largest = r;
        if (largest != i) {
            swap(arr, i, largest);
            maxHeapify(arr, largest);
        }
    }
    public static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
    // 时间复杂度：O(N)
    // 将无序数组建成最大堆
    // 所以建堆完成以后，根节点是最大的元素
    // 常用于优先队列
    public static void buildMaxHeap(int[] arr, int len) {
        // 最开始时，堆的大小初始化为数组的大小
        heapsize = len;
        for (int i = len / 2; i >= 0; i--) {
            maxHeapify(arr, i);
        }
    }
    // 堆排序的时间复杂度：O(NlogN)
    public static void heapSort(int[] arr, int len) {
        // 第一步，将数组建成最大堆
        buildMaxHeap(arr, len);                        // O(N)
        for (int i = len - 1; i >= 1; i--) {           // O(N)
            // 第一轮循环时，将最大堆的顶部元素（也就是数组中的最大值）和最后一个元素交换
            swap(arr, 0, i);
            heapsize = heapsize - 1;
            maxHeapify(arr, 0);                     // O(logN)
        }
    }
}