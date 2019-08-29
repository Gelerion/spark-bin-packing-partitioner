My adoption to Spark 2.4 of bin packing algorithm initially written in Clojure. 
Hopefully in a more readable way.  

https://jsofra.github.io/bin-packing-presentation/

1. Find the book in each Gutenberg bookshelf that those terms are most important to.
2. Find the Gutenberg bookshelf that those terms are most important to.

1. Distributed TfIdf Calculation 
2. Distributed Web Crawling / Html Parsing
3. Bin Packing Partitioner

+ custom trie implementation
+ custom bin packing algorithm implementation
scala features
+ stackable modifiers
+ using monad
+ type aliases
+ rational type

# Data Skew
How do you know if the computation is suffering from skewed data?
1. Gangila metrics
2. Spark UI - event timeline -> computation time

##Partition into exact number of bookshelves
 
 - Overhead from small tasks
 - Poor scheduling
 - Not enough hardware
 - Unused capacity 
 
## Bim Packing
The problem roughly falls into a class of optimization problems known as packing problems.  
 - https://en.wikipedia.org/wiki/Packing_problems  
  
Set packing  
Bin packing problem  
Slothouber-Graatsma puzzle  
Conway puzzle  
Tetris  
Covering problem  
Knapsack problem  
Tetrahedron packing  
Cutting stock problem  
Kissing number problem  
Close-packing of equal spheres  
Random close pack  

These are NP-hard problems so we will need to make use of heuristical methods or 
possibly a constraint solver.  

Our problem seems most closely related to the Bin packing problem (minimum number of bins 
of a certain volume) or maybe the Multiprocessor scheduling problem (pack into a specific 
number of bins)  