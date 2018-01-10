var async = require("async");

/**
 * 表结构关系工具类
 * 收录某库全部表信息,外键关联信息.以及组建CRUD语句.
 * 为了支持分库分表,该工具类并非单例模式
 * date:16/12/5
 * @depan
 * @author wqk
 * @param app {object}
 * @param opts {object}
 */
var DBTable = function(app, opts) {
    this.app = app;
    this.dbClient = null;
    this.databaseName = opts.databaseName;
    this.name = "dbTable-" + this.databaseName;
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

pro.start = function(cb) {
    var self = this;
    self.dbClient = self.app.get('dbMgr');
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

pro.stop = function(cb) {
    cb();
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
 * 获得插入sql,支持同表批量
 * @param tableName
 * @param insertJson    必填 根据表结构取json中的值,不包含的值用mysql字段默认值
 *                          若传入的是一个数组,则返回一个批量插入的sql
 * @returns {*}
 */
pro.getInsertSql = function(tableName, insertJson) {
    var table = this.tables.get(tableName);
    if (!table)
        return "";
    var cols = table[COLUMN];
    var keys = '` (';
    cols.forEach(col=>{
        if (col[AUTO_INCREMENT])
            return;  // 不包含自增
        keys += "`";
        keys += col[COLUMN_NAME];
        keys += "`,";
    });
    keys = keys.substring(0, keys.length - 1);
    // make value
    var _makeValue_ = (json, cols)=>{
        var params = "(", value;
        cols.forEach(col=>{
            if (col[AUTO_INCREMENT])
                return;  // 不包含自增
            value = json[col[COLUMN_NAME]] = json[col[COLUMN_NAME]] || col[COLUMN_DEFAULT_VALUE];
            if (isNaN(value)) value = '"' + value + '"';
            params += value;
            params += ",";
        });
        return params.slice(0, -1) + '),';
    };
    var values = '';
    if (Array.isArray(insertJson)) {
        insertJson.forEach((json)=>{
            values += _makeValue_(json, cols);
        });
    } else {
        values += _makeValue_(insertJson, cols);
    }
    values = values.substring(0, values.length - 1);
    var sql = "insert into `" + table[TABLE_NAME];
    sql += keys;
    sql += ") values ";
    sql += values;
    sql += ";";
    return sql;
};

/**
 * 获取删除sql
 * @param tableName
 * @param priValue  删除主键对应数据,若主键为0,删除外键相关所有数据
 * @param forValue
 * @returns {*}
 */
pro.getDeleteSql = function(tableName, priValue, forValue) {
    var table = this.tables.get(tableName);
    if (!table)
        return "";
    var sql = "delete from `" + tableName + "` where `";
    if (!!priValue) {
        sql += table[PRIMARY_KEY];
        sql += "`=";
        sql += priValue;
    } else {
        sql += table[FOREIGN_KEY];
        sql += "`=";
        forValue = isNaN(forValue) ? '"' + forValue + '"' : forValue;
        sql += forValue;
    }
    return sql;
};

/**
 * 获得更新sql,不支持批量
 * @param tableName
 * @param updJson
 * @returns {*}
 */
pro.getUpdateSql = function(tableName, updJson) {
    var table = this.tables.get(tableName);
    if (!table)
        return "";
    var sql = "update `" + tableName + "` set ";
    var cols = table[COLUMN], key, value;
    cols.forEach(col=>{
        if (col[AUTO_INCREMENT])
            return;
        key = col[COLUMN_NAME];
        value = updJson[key] = updJson[key] || col[COLUMN_DEFAULT_VALUE];
        value = isNaN(value) ? '"' + value + '"': value;
        sql += '`';
        sql += key;
        sql += "`=";
        sql += value;
        sql += ",";
    });
    sql = sql.substring(0, sql.length - 1);
    sql += ' where `';
    sql += table[PRIMARY_KEY];
    sql += '`=';
    sql += updJson[table[PRIMARY_KEY]];
    return sql;
};

/**
 * 根据主键或外键生成select语句
 * @param tableName
 * @param priValue
 * @param forValue
 * @returns {*}
 */
pro.getSelectSql = function(tableName, priValue, forValue) {
    var table = this.tables.get(tableName);
    if (!table)
        return "";

    var key = table[PRIMARY_KEY], value = priValue;
    if (!value) {
        key = table[FOREIGN_KEY];
        value = forValue;
    }
    value = isNaN(value) ? '"' + value + '"' : value;
    var sql = "select * from `" + tableName + "` where ";
    sql += "`" + key + "` = ";
    sql += value;
    return sql;
};