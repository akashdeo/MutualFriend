# MutualFriend

Write a MapReduce program in Hadoop that implements a simple
“Mutual/Common friend list of two friends". The key idea is that if two people
are friend then they have a lot of mutual/common friends. This question will give
any two Users as input, output the list of the user id of their mutual friends.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy
As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this
case you may exclude them from your output).

Input:
Input files
1. q1.txt
The input contains the adjacency list and has multiple lines in the
following format:
<User><TAB><Friends>
Here, <User> is a unique integer ID corresponding to a unique user and <Friends>
is a comma-separated list of unique IDs (<User> ID) corresponding to the friends of
the user. Note that the friendships are mutual (i.e., edges are undirected): if A is
friend with B then B is also friend with A. The data provided is consistent with that
rule as there is an explicit entry for each side of each edge. So when you make the
pair, always consider (A, B) or (B, A) for user A and B but not both.
Output: The output should contain one line per user in the following
format:
<User_A>, <User_B><TAB><Mutual/Common Friend List>
where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A
and B are friend). < Mutual/Common Friend List > is a comma-separated list of
unique IDs corresponding to mutual friend list of User A and B.
  
Please find the above output for the following pairs.
(0,4), (20, 22939), (1, 29826), (6222, 19272), (28041, 28056)

Part-2:

Please answer this question by using dataset from Q1.
Find friend pairs whose common friend number are within the top-10 in all the pairs. Please
output them in decreasing order.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>
