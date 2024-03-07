# Whether to include the special node [-1, -1, -1] when constructing a new graph?
Actually, it doesn't matter if we include the special node or not.

- if we include the special node both in DF_node, DF_arc, and the DataFrame storing co-boundary triangles of saddles, all the components that are not connected to critical triangles will be connected through this special node.
- this will not impact the extraction of V2-paths.

Let's take the dataset cos_sum.tri as an example,
- ## the results of V2-paths if we include the special node:
****************************
[Row(_1=400, _2=[512, 513, 896, 897, 654, 1040, 785, 1041, 787, 786, 789, 657, 792, 793, 794, 795, 796, 797, 414, 798, 799, 417, 802, 801, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 560, 944, 561, 945, 1088, 1089, 704, 705, 462, 463, 846, 849, 606, 736, 993, 992, 739, 740, 609, 742, 743, 745, 750, 1136, 1137, 752, 755, 753, 510]),
****************************
Row(_1=-1, _2=[128, 129, 770, 771, 772, 773, 774, 775, 776, 768, 10, 778, 12, 780, 14, 15, 782, 17, 783, 16, 13, 19, 271, 18, 21, 769, 32, 33, 176, 177, 318, 321, 270, 779, 81, 80, 726, 729, 781, 222, 224, 225, 368, 369, -1])]

- ## the results of V2-paths if we do not include the special node:
****************************
Row(_1=400, _2=[512, 513, 896, 897, 654, 1040, 785, 1041, 787, 786, 789, 657, 792, 793, 794, 795, 796, 797, 414, 798, 799, 417, 802, 801, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 560, 561, 944, 945, 1088, 1089, 704, 705, 462, 463, 846, 849, 606, 736, 993, 992, 739, 740, 609, 742, 743, 745, 750, 1136, 1137, 752, 755, 753, 510])
65
****************************
Row(_1=30, _2=[224, 321, 225, 128, 129, 32, 33, 270, 271, 368, 369, 176, 177, 81, 80, None, 222, 318])
18
****************************
Row(_1=398, _2=[768, 769, 770, 771, 772, 773, 774, 775, 776, 778, 779, 780, 781, 782, 783, 726, None, 729])
18
****************************
Row(_1=14, _2=[14, 16, 17, 18, 19, 21, None])
7
****************************
Row(_1=10, _2=[10, 12, 13, 15, None])
5
****************************

## Note:
1. If we treat the None in each V2-path as "-1", they are the same.
2. I would recommend encoding the special node, which could make the results easier to process.
3. When extracting the real V2-paths and compute the critical net, we should filter out those paths containing "-1".
