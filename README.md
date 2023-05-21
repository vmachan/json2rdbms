# JSON2RDBMS Package

This packages "walks" a JSON data structure recursively and then outputs the lowest level JSON nodes to separate entities defined in different formats. These can be writing each node to its own flat, delimited file or just outputting "insert" statements that would populate a relational dtaabase table for that node.

In addition, it uses an XSD to do type checking and can also validate the incoming JSON payload

# Assumptions
# 1. The target table names are same as the JSON node names
# 2. The JSON content is valid i.e. please pre-validate that the JSON is valid before sending it to this method
# Limitation/Enhancements
# 1. As of now, the data type mapping is hard-coded to a few, this could be improved to run off a generic user-defined mapping construct (TBD)
