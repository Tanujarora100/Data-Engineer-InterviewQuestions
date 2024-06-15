from collections import deque
from typing import Optional
class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

class Solution:
    
    def invertTree(self, root: Optional[TreeNode]) -> Optional[TreeNode]:

        if not root:
            return
        queue = deque([root])

        while queue:
            # Dequeue the first node from the current level
            node = queue.popleft()

            # Swap the left and right children of the current node
            node.left, node.right = node.right, node.left

            # Enqueue the left child if it exists
            if node.left:
                queue.append(node.left)
            # Enqueue the right child if it exists
            if node.right:
                queue.append(node.right)

        # Return the root of the inverted tree
        return root
