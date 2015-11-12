#aerospike-ql

SQL wrapper for Aerospike database.

##Query syntax
    SELECT expr1 [[AS] alias1] [, expr2 [[AS] alias2], ...]
    FROM namespace.set
    [WHERE condition]
    [GROUP BY (field1|alias1) [, (field2|alias2), ...]]
    [HAVING condition]
    [ORDER BY field1|alias1 [DESC|ASC] [, field2|alias2 [DESC|ASC], ...]]
    [LIMIT n]
    
##Documentation
Read more about it in the [aerospike-ql reference](https://docs.google.com/document/d/1ocEWvK1fKjJsUXK0m3XX-9hAnh5JMueypk5a846qLms/edit?usp=sharing).
    

    
    
    
