from typing import Optional


class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class Solution:
    # This function calculates the sum of all left leaves in a binary tree.
    def sumOfLeftLeaves(self, root: Optional[TreeNode]) -> int:
        # A helper function to recursively calculate the sum of left leaves.
        def sum_leaf(root, curr_sum, flag):
            # If the current node is None, return 0.
            if root is None:
                return 0

            # If the current node is a leaf node (i.e., it has no left or right child and the flag is True),
            # add the value of the current node to the running sum curr_sum and return the updated sum.
            if root.left is None and root.right is None and flag:
                curr_sum += root.val
                return curr_sum

            # If the current node is not a leaf node, recursively call itself for the left and right children of the current node,
            # updating the curr_sum and flag as necessary. Finally, return the sum of the left and right subtrees.
            left_ans = sum_leaf(root.left, curr_sum, True)
            right_ans = sum_leaf(root.right, curr_sum, False)
            return left_ans + right_ans

        # Return the result of the sum_leaf function, which is the sum of the values of all the left leaves in the binary tree.
        return sum_leaf(root, 0, False)
