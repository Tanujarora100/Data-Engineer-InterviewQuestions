from typing import Optional

from Python.LinkedList.ReverseLinkedList import ListNode


class Solution:
    def isPalindrome(self, head: Optional[ListNode]) -> bool:
        if not head or not head.next:
            return True

        mid_node = self.middle_node(head)
        second_half_start = self.reverse(mid_node)

        first_half_start = head
        second_half_iter = second_half_start
        result = True

        while result and second_half_iter:
            if first_half_start.val != second_half_iter.val:
                result = False
            first_half_start = first_half_start.next
            second_half_iter = second_half_iter.next

        return result

    def middle_node(self, head: ListNode) -> ListNode:
        slow = head
        fast = head
        while fast and fast.next:
            slow = slow.next
            fast = fast.next.next
        return slow

    def reverse(self, head: ListNode) -> ListNode:
        prev = None
        curr = head
        while curr:
            next_temp = curr.next
            curr.next = prev
            prev = curr
            curr = next_temp
        return prev
