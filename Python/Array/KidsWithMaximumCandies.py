from ast import List


class Solution:
    def kidsWithCandies(self, candies: List[int], extraCandies: int) -> List[bool]:
        maximum_candies = max(candies)
        result = []
        for i in range(len(candies)):
            result.append(candies[i] + extraCandies >= maximum_candies)
        return result
