| :exclamation: IMPORTANT |
|:------------------------|
| Toron is currently pre-alpha software.  It is under development and cannot be installed or used at this time.


# Toron #

`toron` is a tool kit for managing data joinability and ecological
inference problems in data science.  It implements a specialized graph
database to handle relationships between labeled data sets from
multiple sources.

While it's desirable to work with data sources that conform to a
shared standard, or are otherwise directly relatable, this is not
always an option.  Independently designed sources often contain records
that cannot seamlessly join across datasets.  `toron` provides a
framework for integrating such sources into a single, coherent system
to inform real-world decision-making.


## Features ##

 * Implements a graph database to manage relationships between data
   sources.  Each node represents a data source and each edge describes
   the relationship from one source to another.
 * Maintains the original labels used by each source.
 * Easily updates existing relationships when better information becomes
   available later.
 * Generates translation tables for any pair of data sources (so long as
   a path exists between both nodes).
 * Retabulates data from any source using the labels of any other source
   in the graph (given a strongly connected graph).
 * Graphs are stored as collections of node files using the extension
   `.node`.  Relationships between nodes are also stored in the node
   files themselves (each node contains its incoming edges).
 * Complex graphs can be organized using folders and sub-folders of
   connected components.

