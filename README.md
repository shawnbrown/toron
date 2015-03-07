!!! IMPORTANT: GPN is currently pre-alpha software.  It is under 
development and cannot be installed or used at this time.


# `gpn`: Granular Partition Network #

`gpn` is a Python package, and command-line utility, for managing the
joinability problem in data science.  It implements a specialized graph
database to handle relationships between labeled data sets from multiple
sources.

While it's preferable to work with data sources that conform to a shared
standard, or are otherwise directly relatable, this is not always an
option.  Independently designed sources may use labels (i.e., 
categories) that cannot be precisely, completely, or consistently 
related to one another.  `gpn` helps integrate such sources into single, 
coherent systems to inform real-world decision-making.

Using `gpn` effectively requires a general understanding of graph
theory, familiarity with command-line operations (or the Python
programming language), and good judgment regarding the subject being
analyzed.


## Features ##

 * Implements a graph database to manage relationships between data
   sources.  Each node represents a data source and each edge describes
   the relationship from one source to another.
 * Maintains the original labels used by each source.
 * Allows one-to-one, many-to-one, one-to-many, and many-to-many
   relationships.
 * Easily updates existing relationships if better information becomes
   available later.
 * Generates translation tables for any pair of data sources (so long as
   a path exists between both nodes).
 * Retabulates data from any source using the labels of any other source
   in the graph (given a strongly connected graph).
 * Exports  third-party formats (NetworkX, GraphViz) for analysis or
   visualization.
 * Graphs are stored as collections of NODE files in a folder (using
   the file extension `.node`).  Relationships between nodes are also
   stored in the NODE files themselves.
 * Complex graphs can be organized using sub-folders of connected
   components.


## Setup ##

To use `gpn`, you must have Python installed on your system.  You can
get the latest version of Python at http://www.python.org/download/ or
use your operating system's package manager.

Install GPN with one of the following methods:

 * If you have `pip` on your system, you can install GPN by opening a
   terminal and typing the following command:

           pip install gpn
           
 * Install GPN "from source" by downloading it from
   https://pypi.python.org/pypi/gpn and running the standard setup
   script:

           python setup.py install

GPN requires no dependencies beyond Python's built-in Standard Library.
As an optional dependency, the `colorama` package provides color
terminal output when available.


## Design Philosophy ##

The design of `gpn` was influenced and inspired by *Soft Systems 
Methodology* and the *Theory of Granular Partitions*.  Both of these 
frameworks favor a self-aware, epistemic approach to problem 
solving--that is to say, the theories account for their own limits as 
mere cognitive tools and are clearly differentiated from the real-world 
observations they describe.

Decision makers crave certainty and classical intuition suggest that 
certainty, however elusive, is ultimately possible.  But complex 
situations that continually evolve have a way of confounding or 
invalidating strict, formal definition.  In `gpn`, nodes are *islands of 
classical certainty* connected by edges that allow for *varying degrees* 
of precision and completeness when necessary.


### Soft Systems Methodology ###

Soft Systems Methodology is a structured approach for learning about 
situations for which formal descriptions are elusive or incomplete.

> Systems thinking is only an epistemology, a particular way of
> describing the world.  It does not tell us what the world is.
> Hence, strictly speaking, *we should never say of something in the
> world: "It is a system", only: "It may be described as a system"*.
> (Peter Checkland, 1983)


### Granular Partitions ###

A *granular partition* is a hierarchical grid of labeled cells for
categorizing objects and their constituent parts.  The concept of a
granular partition was formally introduced in 2001 by Thomas Bittner and
Barry Smith.  The opening paragraph of their 2003 follow-up paper, "A
Theory of Granular Partitions", describes the concept simply:

> Imagine that you are standing on a bridge above a highway checking
> off the makes and models of the cars that are passing underneath. Or
> that you are a postal clerk dividing envelopes into bundles; or a
> laboratory technician sorting samples of bacteria into species and
> subspecies.  Or imagine that you are making a list of the fossils in
> your museum, or of the guests in your hotel on a certain night.  In
> each of these cases you are employing a certain grid of labeled cells,
> and you are recognizing certain objects as being located in those
> cells.  Such a grid of labeled cells is an example of what we shall
> call a *granular partition*.  We shall argue that granular
> partitions are involved in all naming, listing, sorting, counting,
> cataloguing and mapping activities.  Division into units, counting and
> parceling out, mapping, listing, sorting, pigeonholing, cataloguing
> are activities performed by human beings in their traffic with the
> world.  Partitions are the cognitive devices designed and built by
> human beings to fulfill these various listing, mapping and classifying
> purposes.


## Reference Material ##

 * For help installing packages with `pip`:
     https://docs.python.org/3/installing/
 * Wikipedia's *Glossary of Graph Theory*:
     https://en.wikipedia.org/wiki/Glossary_of_graph_theory
 * Thomas Bittner and Barry Smith's 2003 paper "A Theory of Granular Partitions":
     http://ontology.buffalo.edu/smith/articles/partitions.pdf
