# JSON2RDBMS Package

This packages "walks" a JSON data structure recursively and then outputs the lowest level JSON nodes to separate entities defined in different formats. These can be writing each node to its own flat, delimited file or just outputting "insert" statements that would populate a relational dtaabase table for that node.

In addition, it uses an XSD to do type checking and can also validate the incoming JSON payload
