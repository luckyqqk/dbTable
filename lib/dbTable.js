var async = require("async");

/**
 * 表结构关系工具类
 * 收录某库全部表信息,外键关联信息.以及组建CRUD语句.
 * 为了支持分库分表,该工具类并非单例模式
 * date:16/12/5
 * @author wqk
 * @param dbClient {object} 执行sql的对象
 * @param databaseName {string} 数据库名
 */
var DBTable = function(dbClient, databaseName) {
    this.dbClient = dbClient;
    this.databaseName = databaseName;
    this.name = "dbTable-" + databaseName;
    this.tables = new Map();
};

var TABLE_NAME              = "tableName";
var COLUMN                  = "column";
var COLUMN_NAME             = "columnName";
var COLUMN_NAMES            = "columnNames";
var COLUMN_DEFAULT_VALUE    = "columnDefaultValue";
var PRIMARY_KEY             = "primaryKey";
var FOREIGN_KEY             = "foreignKey";
var SON_KEY                 = "sonKey";
var AUTO_INCREMENT          = "autoIncrement";
var SPLIT = ":", REFERENCED = "refer";

module.exports = DBTable;
var pro = DBTable.prototype;

/**
 * 加载表数据,和表关联关系
 * @param cb {function}
 */
pro.init = function (cb) {
    var self = this;
    self.dbClient.query("show tables;", [], function (err, data) {
        if (!!err) {
            cb(err);
            return;
        } else if (!data || data.length < 1) {
            cb(`no tables in database:${self.databaseName}`);
            return;
        }
        data = JSON.parse(JSON.stringify(data));
        var funcArray = [];
        var tableKey = `Tables_in_${self.databaseName}`;
        for (var v of data) {
            var tableName = v[tableKey];
            funcArray.push(new _describeTable(self, tableName));
        }
        async.parallel(funcArray, ()=>{
            loadForeignTable(self, cb);
        });
    });
};
/**
 * 表信息
 * @param self
 * @param tableName
 * @returns {Function}
 * @private
 */
var _describeTable = function(self, tableName) {
    return function(cb) {
        self.dbClient.query(`describe ${tableName}`, [], (err, columns)=> {
            if (!!err) {
                cb(err);
                return;
            }
            columns = JSON.parse(JSON.stringify(columns));
            //{
            //    Field: 'ID',
            //    Type: 'int(11)',
            //    Null: 'NO',
            //    Key: 'PRI',
            //    Default: null,
            //    Extra: 'auto_increment' },
            var table = {};
            table[TABLE_NAME] = tableName;
            var tableColumns = [];
            var columnNames = [];
            for (var col of columns) {
                var columnMap = {};
                columnMap[COLUMN_NAME] = col["Field"];
                if (col["Key"] == "PRI") {
                    if (col["Field"].indexOf(SPLIT) != -1 && col["Field"].split(SPLIT)[0] == REFERENCED) {
                        table[FOREIGN_KEY] = col["Field"];
                    } else {
                        table[PRIMARY_KEY] = col["Field"];
                        columnMap[PRIMARY_KEY] = 1;
                    }
                }
                if (col["Extra"] == 'auto_increment')
                    columnMap[AUTO_INCREMENT] = 1;

                columnNames.push(col["Field"]);
                var colType = col["Type"].substr(0, col["Type"].indexOf("("));
                columnMap[COLUMN_DEFAULT_VALUE] = col['Default'];
                tableColumns.push(columnMap);
            }
            table[COLUMN] = tableColumns;
            table[COLUMN_NAMES] = columnNames;
            self.tables.set(tableName, table);
            cb();
        });
    };
};

/**
 * 表外键信息
 * @param self
 * @param cb
 */
var loadForeignTable = function(self, cb) {
    self.dbClient.query(`select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_SCHEMA='${self.databaseName}' and REFERENCED_TABLE_NAME is not null`, [], (err, constraints)=> {
        if (!!err) {
            cb(err);
            return;
        }
        constraints = JSON.parse(JSON.stringify(constraints));
        for (var v of constraints) {
            var theTableName = v['TABLE_NAME'];
            var theColumnName = v['COLUMN_NAME'];

            //var fatherColumnName = constraints['REFERENCED_COLUMN_NAME'];

            var tableSon = self.tables.get(theTableName);
            if (!tableSon) {
                console.warn(`can not find tableSon :: ${theTableName}`);
                continue;
            }
            if (!tableSon[FOREIGN_KEY]) {
                tableSon[FOREIGN_KEY] = theColumnName;
                // 数据只需要一个爹, 暂不需要多外键联合键
                //} else {
                //tableSon[FOREIGN_KEY] += "+";
                //tableSon[FOREIGN_KEY] += theColumnName;
            }

            var fatherTableName = v['REFERENCED_TABLE_NAME'];
            var tableFather = self.tables.get(fatherTableName);
            if (!tableFather) {
                console.warn(`can not find tableFather :: ${theTableName}`);
                continue;
            }
            var sonArray = tableFather[SON_KEY] = tableFather[SON_KEY] || [];
            sonArray.push(theTableName);
        }
        cb();
        //{
        //    CONSTRAINT_CATALOG: 'def',
        //    CONSTRAINT_SCHEMA: 'main_bj',
        //    CONSTRAINT_NAME: 'player_id',
        //    TABLE_CATALOG: 'def',
        //    TABLE_SCHEMA: 'main_bj',
        //    TABLE_NAME: 'u_tank',
        //    COLUMN_NAME: 'uid',
        //    ORDINAL_POSITION: 1,
        //    POSITION_IN_UNIQUE_CONSTRAINT: 1,
        //    REFERENCED_TABLE_SCHEMA: 'main_bj',
        //    REFERENCED_TABLE_NAME: 'u_palyer',
        //    REFERENCED_COLUMN_NAME: 'ID' }
    });
};

/**
 * 获得某表信息
 * @param tableName
 * @returns {V}
 */
pro.getTable = function(tableName) {
    return this.tables.get(tableName);
};

/**
 * 获得某表数据的默认JSON供插入需求使用.返回的JSON不会包含自增属性的字段.
 * @param table {object}    必填 数据库表对象
 * @param jsonArray         必填 表数据键值对,只需传入必须字段,其他字段由程序自动默认添加<br/>
 *                          若传入的是一个数组,则也返回一个数组
 * @returns
 */
pro.getAllInsertJson = function(table, jsonArray) {
    if (!table || !jsonArray)
        return "";
    var getInsertJson = (table, json)=>{
        var insertJson = {};
        var cols = table[COLUMN];
        for (var col of cols) {
            if (col[AUTO_INCREMENT]) {
                continue;  // 不包含自增
            }
            var jsonKey = col[COLUMN_NAME];
            insertJson[jsonKey] = !!json[jsonKey] ? json[jsonKey] : col[COLUMN_DEFAULT_VALUE];
        }
        return insertJson;
    };
    // 单条的直接返回
    if (Array.isArray(jsonArray)) {
        var result = [];
        jsonArray.forEach((json)=>{
            result.push(getInsertJson(table, json));
        });
        return result;
    } else
        return getInsertJson(table, jsonArray);
};

/**
 * 根据json,组建表相关的json,去除额外字段,添加默认字段
 * @param table
 * @param jsonArray
 * @returns {object}
 */
pro.getAllTableJson = function(table, jsonArray) {
    if (!table || !jsonArray)
        return "";
    var getInsertJson = (table, json)=>{
        var insertJson = {};
        var cols = table[COLUMN];
        for (var col of cols) {
            var jsonKey = col[COLUMN_NAME];
            insertJson[jsonKey] = !!json[jsonKey] ? json[jsonKey] : col[COLUMN_DEFAULT_VALUE];
        }
        return insertJson;
    };
    // 单条的直接返回
    if (Array.isArray(jsonArray)) {
        var result = [];
        jsonArray.forEach((json)=>{
            result.push(getInsertJson(table, json));
        });
        return result;
    } else
        return getInsertJson(table, jsonArray);
};

/**
 * 生成插入sql
 * @param table {object}必填 数据库表对象
 * @param allJsonArray {JSON}  必填 需包含全部字段的键值对,示例:{nick:"Kai", age:"18"...}表示"(nick,age)values('Kai',18)"
 *                          若传入的是一个数组,则返回一个批量插入的sql
 * @returns String
 */
pro.getInsertSqlByJson = function (table, allJsonArray) {
    // make key
    var _makeKeys_ = (json)=>{
        var keys = '';
        for (var columnKey in json) {
            keys += "`";
            keys += columnKey;
            keys += "`";
            keys += ",";
        }
        return keys.slice(0, -1);
    };
    // make value
    var _makeValue_ = (json)=>{
        var params = "(";
        for (var key in json) {
            var v = json[key];
            if (isNaN(v)) v = '"' + v + '"';
            params += v;
            params += ",";
        }
        return params.slice(0, -1) + '),';
    };
    var insertJson = this.getAllInsertJson(table, allJsonArray);
    var keys = '', values='';
    if (Array.isArray(allJsonArray)) {
        keys = _makeKeys_(insertJson[0]);
        insertJson.forEach((json)=>{
            values += _makeValue_(json);
        });
    } else {
        keys = _makeKeys_(insertJson);
        values = _makeValue_(insertJson);
    }
    if (!keys || !values)
        return '';
    var sql = "insert into `" + table[TABLE_NAME] + "` (";
    sql += keys;
    sql += ") values ";
    sql += values.slice(0, -1);
    sql += ";";
    return sql;
};
/**
 * 生成删除sql
 * @param table {object}必填 数据库表对象
 * @param conditionJson {JSON} 主键值
 * @returns String
 */
//pro.getDeleteSql = function (table, conditionJson) {
//    if (!table || !conditionJson)
//        return "";
//    var sql = "delete from `" + table.get(TABLE_NAME) + "` where ";
//    for (var k in conditionJson) {
//        if (table.get(COLUMN_NAMES).indexOf(k) == "-1") {
//            //console.log(k + " can not find in table " + table.get(TABLE_NAME));
//            continue;
//        }
//        sql += "`" + k + "` = ";
//        var v = conditionJson[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += " and ";
//    }
//    sql = sql.substr(0, sql.length - 4);
//    return sql;
//};
/**
 * 生成更新sql:更新列仅包含json中的key.where条件语句仅支持and连接.
 * @param table {object}必填 数据库表对象
 * @param json {JSON}   必填 示例:{nick:"Kai", age:"18"}表示"set nick='Kai',age=18"
 * @param conditionJson {JSON} 选填 更新条件.示例:{nick:"Kai", age:18}表示"where nick='Kai' and age=18".若不填则用主键作为条件 示例:"where id =1"
 * @returns String
 */
//pro.getUpdateSqlByJson = function (table, json, conditionJson) {
//    if (!table || !json)
//        return "";
//    var priArray = table.get(PRIMARY_KEY);
//    var isPri = (key)=>{
//        for (var pri of priArray) {
//            if (pri == key)
//                return true;
//        }
//        return false;
//    };
//    var sql = "update `" + table.get(TABLE_NAME) + "` set ";
//    for (var k in json) {
//        if (table.get(COLUMN_NAMES).indexOf(k) == "-1") {
//            console.log(k + " can not find in table " + table.get(TABLE_NAME));
//            continue;
//        }
//        if (isPri(k)) {   // update语句不含主键
//            continue;
//        }
//        sql +=  '`' + k + '`';
//        sql += " = ";
//        var v = json[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += ",";
//    }
//    sql = sql.substr(0, sql.length - 1);
//    sql += " where ";
//    if (!conditionJson) {
//        for (var priK of priArray) {
//            if (!priK || typeof(priK) == "function")
//                continue;
//            sql +=  '`' + priK + '`';
//            sql += " = ";
//            sql += json[priK];
//            sql += " and ";
//        }
//    } else {
//        for (var key in conditionJson) {
//            sql +=  '`' + key + '`';
//            sql += " = ";
//            var v = conditionJson[k];
//            if (isNaN(v)) v = '"' + v + '"';
//            v = !v ? '""' : v;
//            sql += v;
//            sql += " and ";
//        }
//    }
//    return sql.substr(0, sql.length - 4);
//};
/**
 * 生成查询sql:仅提供条件and相接
 * @param table {object}必填 数据库表对象
 * @param conditionJson {JSON} 查询条件,示例:{nick:"Kai", age:18}表示"where nick='Kai' and age=18"
 * @returns String
 */
//pro.getSelectSql = function (table, conditionJson) {
//    if (!table)
//        return "";
//    var sql = "select * from `" + table.get(TABLE_NAME) + "` where ";
//    for (var k in conditionJson) {
//        sql += "`" + k + "`";
//        sql += " = ";
//        var v = conditionJson[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += " and ";
//    }
//    sql = sql.substr(0, sql.length - 4);
//    return sql;
//};

/**
 * 根据主键/外键生成select语句
 * @param tableName
 * @param sign
 * @returns {*}
 */
pro.getSelectSql = function(tableName, sign) {
    var table = this.tables.get(tableName);
    if (!table)
        return "";
    if (isNaN(sign)) sign = '"' + sign + '"';
    sign = !sign ? '""' : sign;
    var mainKey = table[FOREIGN_KEY] || table[PRIMARY_KEY];
    var sql = "select * from `" + tableName + "` where ";
    sql += "`" + mainKey + "` = ";
    sql += sign;
    return sql;
};

/**
 * 根据json生成update语句
 * @param tableName
 * @param json
 * @returns {*}
 */
pro.getUpdateSqlByJson = function (tableName, json) {
    if (!tableName || !json)
        return "";
    var table = this.tables.get(tableName);
    var priKey = table[PRIMARY_KEY];
    if (!priKey)
        return "";
    var priValue = json[priKey];
    if (isNaN(priValue))
        priValue = '"' + priValue + '"';
    var sql = "update `" + tableName + "` set ";
    for (var k in json) {
        if (table[COLUMN_NAMES].indexOf(k) == "-1") {
            console.log(k + " can not find in table " + table[TABLE_NAME]);
            continue;
        }
        if (k == priKey) {   // update语句不含主键
            continue;
        }
        sql +=  '`' + k + '`';
        sql += " = ";
        var v = json[k];
        if (isNaN(v)) {
            v = '"' + v + '"';
            v = !v ? '""' : v;
        }
        sql += v;
        sql += ",";
    }
    sql = sql.substr(0, sql.length - 1);
    sql += " where ";
    sql += "`" + priKey + "` = ";
    sql += priValue;
    return sql;
};

/**
 * 根据主键,生成删除语句
 * @param tableName
 * @param json
 * @returns {string} sql
 */
pro.getDeleteSql = function(tableName, json) {
    if (!tableName || !json)
        return "";
    var table = this.tables.get(tableName);
    var sql = "delete from `" + tableName + "` where ";
    if (Array.isArray(json)) {
        json.forEach((data)=>{
            sql += "`" + table[PRIMARY_KEY] + "` = ";
            sql += data[table[PRIMARY_KEY]];
            sql += " or "
        });
        sql = sql.substr(0, sql.length - 3);
    } else {
        sql += "`" + table[PRIMARY_KEY] + "` = ";
        sql += json[table[PRIMARY_KEY]];
    }
    return sql;
};

/**
 * 根据外键,生成删除语句
 * @param tableName
 * @param foreignValue
 * @returns {string} sql
 */
pro.getDeleteSqlByForeign = function(tableName, foreignValue) {
    if (!tableName)
        return "";
    var table = DBTable.tables.get(tableName);
    if (!table[FOREIGN_KEY]) {
        return "";
    }
    return "delete from `" + tableName + "` where " + table[FOREIGN_KEY] + " = " + foreignValue;
};