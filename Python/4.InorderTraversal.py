
from typing import List, Optional

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

class Solution:
    def inorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        return self.inorder(root, [])
    def inorder(self,root,nums):
        if root is None:
            return 
        self.inorder(root.left,nums)
        nums.append(root.val)
        self.inorder(root.right,nums)
        return nums
        
