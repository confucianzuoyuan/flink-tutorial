class BinarySearchTree {
    public static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;
        TreeNode(int x) { val = x; }
    }

    public static void main(String[] args) {
        TreeNode root = new TreeNode(5);
        insert(root, 3);
    }

    public static boolean search(TreeNode root, int val) {
        if (root != null) {
            if (root.val == val) {
                return true;
            } else if (root.val > val) {
                return search(root.left, val);
            } else {
                return search(root.right, val);
            }
        }
        return false;
    }

    public static void insert(TreeNode root, int val) {
        TreeNode x = root;
        while (x != null) {
            if (x.val > val) {
                if (x.left == null) {
                    x.left = new TreeNode(val);
                    return;
                }
                insert(x.left, val);
            } else if (x.val < val) {
                if (x.right == null) {
                    x.right = new TreeNode(val);
                    return;
                }
                insert(x.right, val);

            }
        }
    }
}