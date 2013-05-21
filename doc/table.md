# Table in Ardb
A Table with a few keys and columns is like a 'table' in relation databases. You can store/retrive data by the commands list below. 


#### TCreate tablename numkeys key [key …]
Create a table named by 'tablename' with keys.
##### Examples
	tcreate mytable domain url 

#### TLen tablename
Returns the row size (number of elements) of the table stored at tablename.
##### Examples
	Tlen mytable 

#### TDesc tablename
Returns the key/column description of the table stored at tablename.
##### Examples
	Tdesc mytable 

#### TInsert tablename numkvs key value [key value …]
Insert keys and column values into a table.
##### Examples
	tinsert mytable 4 dm www.google.com url page1 pv 10 uv 2 
 
#### TReplace tablename numkvs key value [key value …]
Replace keys and column values into a table.
##### Examples
	treplace mytable 4 dm www.google.com url page1 pv 10 uv 2  
	
#### TUpdate tablename numkvs key value [key value …] where condition [[and|or] condtion ...]
Update a table with conditions. 'condition' is a compare expression.
##### Condition
Condition := KEY CMP_OPERATOR VALUE

Note: Only key defined in 'TCreate' can be used as KEY.

Only 6 compare operators(=|<=|>=|!=|>|<) supported 

	domain=www.google.com
	domian<=www.google.com
	domain>=www.google.com
	domain<www.google.com
	domain>www.google.com
	domain!=www.google.com
##### Examples
	tupdate mytable 1 pv 2 where url=page4 and dm=www.google.com
	
  
#### TDel tablename [where condition [[and|or] condtion …]]
Delete from a table with conditions. 'condition' is a compare expression.

##### Examples
	tdel mytable  //clear a table
	tdel mytable where url=page2
	
#### TGetAll tablename
Retrive all key and column values from a table.

##### Examples
	tgetall mytable  
	
#### TGet tablename numnames name [name …] [ORDERBY pattern] [LIMIT offset count] [ASC|DESC] [ALPHA] [AGGREGATE SUM|MIN|MAX|COUNT|AVG] [WHERE condition [[AND|OR] condtion …]]
Return key/column values from a table.

##### Examples
	tget mytable 1 pv aggregate sum where domain=www.google.com
	tget mytable 2 domain pv where domain=www.google.com and url=page1
	
