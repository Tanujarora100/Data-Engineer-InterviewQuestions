from collections import defaultdict
from typing import List


class Solution:
    def mostFrequentEven(self, nums: List[int]) -> int:
        hash_map = defaultdict(int)
        maximum_frequency = 0
        maximum_frequent_value = float("inf")
        for i in range(len(nums)):
            if (nums[i] & 1) == 0:
                hash_map[nums[i]] += 1
                if hash_map[nums[i]] >= maximum_frequency:
                    if hash_map[nums[i]] == maximum_frequency:
                        maximum_frequent_value = min(nums[i], maximum_frequent_value)
                    maximum_frequency = hash_map[nums[i]]
                    maximum_frequent_value = nums[i]
        return maximum_frequent_value if maximum_frequent_value != float('inf') else -1

nums = [4, 4, 2, 2, 3, 3, 4, 2]
edge_case=[0,0,0,0,0,1,1,1,1,1,1,2,2,2]
obj= Solution()
print(obj.mostFrequentEven(edge_case))