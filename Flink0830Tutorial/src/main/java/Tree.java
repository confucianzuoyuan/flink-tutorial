/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode(int x) { val = x; }
 * }
 */

class Tree {
    public static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;
        TreeNode(int x) { val = x; }
    }

    public static void main(String[] args) {
        /**
         *       1
         *     /  \
         *    2    3
         *  /  \
         * 4   5
         * */
        TreeNode root = new TreeNode(1);
        root.left = new TreeNode(2);
        root.right = new TreeNode(3);
        root.left.left = new TreeNode(4);
        root.left.right = new TreeNode(5);
        System.out.println("============= preorder ==============");
        preorder(root);
        System.out.println("=====================================");
        System.out.println("============= inorder ==============");
        inorder(root);
        System.out.println("=====================================");
        System.out.println("============= postorder ==============");
        postorder(root);
        System.out.println("=====================================");

    }

    // 前序遍历
    public static void preorder(TreeNode root) {
        if (root != null) {
            System.out.println(root.val);
            preorder(root.left);
            preorder(root.right);
        }
    }

    // 中序遍历
    public static void inorder(TreeNode root) {
        if (root != null) {
            inorder(root.left);
            System.out.println(root.val);
            inorder(root.right);
        }
    }

    public static void postorder(TreeNode root) {
        if (root != null) {
            postorder(root.left);
            postorder(root.right);
            System.out.println(root.val);
        }
    }
}