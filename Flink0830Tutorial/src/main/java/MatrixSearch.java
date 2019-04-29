class MatrixSearch {
    public static void main(String[] args) {
        int[][] matrix = {
                {1,3,5,7},
                {10,11,16,20},
                {23,30,34,50}
        };

        boolean val = searchMatrix(matrix, 13);
        System.out.println(val);
    }

    public static boolean searchMatrix(int[][] matrix, int target) {
        if (matrix.length == 0) return false;
        int i = 0;
        int j = matrix[0].length - 1;
        System.out.println(matrix.length);
        while (i < matrix.length && j >= 0) {
            if (matrix[i][j] == target) {
                System.out.println(matrix[i][j]);
                return true;
            } else if (matrix[i][j] > target) {
                j -= 1;
            } else {
                i += 1;
            }
        }
        return false;
    }
}