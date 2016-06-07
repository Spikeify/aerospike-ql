#aerospike-ql

SQL wrapper for Aerospike database.

aerospike-ql is a SQL wrapper for Aerospike database. It transforms SQL query to LUA code and executes it as Stream UDF on Aerospike database. It enables:

 - writing and executing SQL queries against Aerospike database,
 - maps database records to Java objects or returns them as Map objects,
 - count, min, max, avg and sum aggregations, 
 - group by, order by and limit operations,
 - essential functions for working with strings, lists, maps, dates types in Aerospike database,
 - inject custom conditions to LUA script,
 - monitoring query execution.   

##Query syntax
    SELECT expr1 [[AS] alias1] [, expr2 [[AS] alias2], ...]
    FROM namespace.set
    [WHERE condition]
    [GROUP BY (field1|alias1) [, (field2|alias2), ...]]
    [HAVING condition]
    [ORDER BY field1|alias1 [DESC|ASC] [, field2|alias2 [DESC|ASC], ...]]
    [LIMIT n]

##Setup
Simply include the aerospikeql-xx.jar lib in your project or add a maven dependency:

    <dependency>
        <groupId>com.spikeify</groupId>
        <artifactId>aerospike-ql</artifactId>
        <version>check for latest version</version>
    </dependency>


##Documentation
aerospike-ql supports adhoc and static queries:

 - adhoc query API saves LUA code locally and on Aerospike servers, executes it and removes it, 
 - static query API only executes LUA code, so code needs to be generated and UDF registered in advance with QueryUtils API. 



## Basic Usage

### Getting AerospikeQlService instance
    SpikeifyService.globalConfig("defaultNamespace", port, hostnames);
    Spikeify sfy = SpikeifyService.sfy();
    AerospikeQlService aerospikeQlService = new AerospikeQlService(sfy);

### Run adhoc query   
    String query = "select cluster, 
                        sum(value) as sumValue, 
                        min(value) as minValue, 
                        count(*) as counter 
                    from defaultNamespace.Entity 
                    group by cluster 
                    order by cluster";
    List<Map<String, Object>> resultsList = aerospikeQlService.execAdhoc(query).now();

### Run typed adhoc query
Typed queries can be only used with select * statement. You can add filters (where) and ordering (order by) of results.

    String query = "select * from defaultNamespace.Entity where value > 10 order by value desc";
    List<Entity> resultsList = aerospikeQlService.execAdhoc(Entity.class, query).now();

### Run static query
Static queries are intended to be used for frequent queries. Firstly, static query needs to registered on aerospike server. Then it can be executed multiple times. If query changes, it needs to be registered as new query. 

    QueryUtils queryUtils = new QueryUtils(sfy);
    String queryName = "countQuery";
    String query = "select count(1) as counter1, count(*) as counter2  from defaultNamespace.Entity";

    queryUtils.addUdf(queryName, query); //save and register query

    //execute query multiple times
    List<Map<String, Object>> resultList1 = aerospikeQlService.execStatic(query, queryName).now(); 
    List<Map<String, Object>> resultList2 = aerospikeQlService.execStatic(query, queryName).now();

    queryUtils.removeUdf(queryName); //remove and unregister query

### Profile query execution
    Executor<Map<String, Object>> executor = aerospikeQlService.execAdhoc(query);
    executor.now();

    Profile profile = executor.getProfile();
    long columnsQueried = profile.getColumnsQueried();
    long rowsRetrieved = profile.getRowsRetrieved();
    long rowsQueried = profile.getRowsQueried();

##Function reference
Read more about supported functions in the [aerospike-ql reference](https://docs.google.com/document/d/1ocEWvK1fKjJsUXK0m3XX-9hAnh5JMueypk5a846qLms/edit?usp=sharing).

## Release notes

### version 0.1.3 (Released June 7, 2016)
    - null checks for map_retrieve and map_contains functions,
    - updated dependencies.
    
    

    
    
    
