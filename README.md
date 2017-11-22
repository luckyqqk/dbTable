# dbTable
load mysql tables and table references

### 设计目的
* node天生对json的友好,表数据和结构均可用json表示.
* 加载mysql某个数据库中的所有表结构和表关联到内存.
* 根据表结构关系,很容易组建CRUD语句.方便快捷.

### 使用注意
* 仅作为工具使用,开服需调用init加载.
* 需传入可执行sql的dbClient和数据库名.

### 方法支持

#### 获得某表信息
```
getTable(tableName)
```
#### 获得某表数据的默认JSON供插入需求使用.返回的JSON不会包含自增属性的字段.
```
getAllInsertJson(table, jsonArray)
```
#### 根据json,组建表相关的json,去除额外字段,添加默认字段
```
getAllTableJson(table, jsonArray)
```
#### 生成插入sql
```
getInsertSqlByJson(table, allJsonArray)
```
#### 根据主键/外键生成select语句
```
getSelectSql(tableName, sign)
```
#### 根据json生成update语句
```
getUpdateSqlByJson(tableName, json)
```
#### 根据主键,生成删除语句
```
getDeleteSql(tableName, json)
```
#### 根据外键,生成删除语句
```
getDeleteSqlByForeign(tableName, foreignValue)
```
