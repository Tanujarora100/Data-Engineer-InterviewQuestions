# Definition for singly-linked list.
class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next
from typing import Optional


class Solution:
   # This function reverses a singly linked list in-place.
    def reverseList(self, head: Optional[ListNode]) -> Optional[ListNode]:
        # If the input linked list is empty, return the head node as it is.
        if head is None:
            return head

        # Initialize three variables: prev, curr, and forward.
        # prev is initialized to None, curr is initialized to the head node,
        # and forward is initialized to the next attribute of the curr node.
        prev = None
        curr = head
        forward = None if curr is None else curr.next

        # Enter a loop that continues until the curr node is None.
        # Inside the loop, break the link between the current node and the next node,
        # reverse the link between the previous node and the current node,
        # move the "pointer" one step back in the linked list,
        # and move the "pointer" one step forward in the linked list.
        while curr is not None:
            # Break the link between the current node and the next node.
            forward =curr.next
            # Reverse the link between the previous node and the current node.
            curr.next = prev
            # Move the "pointer" one step back in the linked list.
            prev = curr
            # Move the "pointer" one step forward in the linked list.
            curr = forward

        # Return the prev variable, which is the head node of the reversed linked list.
        return prev