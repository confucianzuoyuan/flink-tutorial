class ReverseList {
    public static class ListNode {
        int val;
        ListNode next;
        ListNode(int x) { val = x; }
    }
    public static void main(String[] args) {
        ListNode head = new ListNode(1);
        head.next = new ListNode(2);
        head.next.next = new ListNode(3);
        ListNode tmp = head;
        while (tmp != null) {
            System.out.println(tmp.val);
            tmp = tmp.next;
        }
        head = reverseList(head);
        tmp = head;
        while (tmp != null) {
            System.out.println(tmp.val);
            tmp = tmp.next;
        }
    }
    public static ListNode reverseList(ListNode head) {
        ListNode dummy = head;
        ListNode tmp = head;

        while (head != null && head.next != null) {
            dummy = head.next;
            head.next = dummy.next;
            dummy.next = tmp;
            tmp = dummy;
        }
        return dummy;
    }
}