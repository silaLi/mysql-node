var mysql = require('mysql');
var log = console.log;
var conn = undefined;
var config = undefined;

function StringFirstUpperCase(str) {
    return str.replace(/^\S/, function(s) {
        return s.toUpperCase();
    });
}

function ObjectValues(val) {
    var self = val || this;
    var keys = Object.keys(self);
    var values = [];
    for (var i = 0; i < keys.length; i++) {
        values.push(self[keys[i]]);
    };
    return values;
}

function init(_config) {
    config = _config
    for (var entity_key in config.entities) {
        if (!config.entities.hasOwnProperty(entity_key)) {
            continue;
        }
        var sql = '';
        var entity = config.entities[entity_key];
        var props = entity.props
        var PRIMARY_KEY = entity.PRIMARY_KEY
        if (!props[PRIMARY_KEY]) {
            log("creat database PRIMARY_KEY error");
            return;
        }
        log("creat database PRIMARY_KEY clear");

        sql = 'CREATE TABLE IF NOT EXISTS `' + entity_key + '` ('

        for (var key in props) {
            if (!props.hasOwnProperty(key)) {
                continue;
            }
            if (PRIMARY_KEY === key) {
                sql += '`' + key + '` ' + props[key] + ' NOT NULL,'
            } else {
                sql += '`' + key + '` ' + props[key] + ' DEFAULT NULL,'
            }
        }
        sql += 'PRIMARY KEY (`' + PRIMARY_KEY + '`)' +
            ') ENGINE=InnoDB DEFAULT CHARSET=utf8;';

        run({
            sql: sql
        });
    }
}

function buildEntityMap() {
    for (var $entity_name in config.entities) {
        if (!config.entities.hasOwnProperty($entity_name)) {
            continue;
        }
        var $entity = config.entities[$entity_name];
        var $entity_upperName = StringFirstUpperCase($entity_name);
        var $interface = {};
        initDBInterface($entity_name, $entity_upperName, $entity);
        this.entitiesDao[$entity_name + "Dao"] = $interface;

        // 初始化数据库的
        function initDBInterface($entity_name, $entity_upperName, $entity) {
            var $interface = {};
            $interface["save" + $entity_upperName] = save;
            $interface["delete" + $entity_upperName + "ByProps"] = deleteByProps;
            $interface["query" + $entity_upperName + "ByProps"] = queryByProps;
            $interface["queryCount" + $entity_upperName] = queryCount;
            $interface["update" + $entity_upperName + "ByProps"] = updateByProps;
            $interface.runSql = runSql;

            return $interface;

            function save(saveEntity, callBack) {
                var self = this;
                if (saveEntity instanceof Array) {
                     var keys = Object.keys(saveEntity[0]);
                    var sql = "INSERT INTO " + $entity_name + "(" + keys.join(",") + ") VALUES (" + keys.fill("?").join(",") + ")";
                    var entities = saveEntity;
                    var temp = [];

                    function savePromise(index) {
                        var entity = entities[index];
                        new Promise(function(resolve, reject) {
                            run({
                                sql: sql,
                                sqlParam: ObjectValues(entity),
                                callBack: function(rows, err, fields) {
                                    if (err) {
                                        reject("mysql error!");
                                        return;
                                    }
                                    temp.push(entity);
                                    if (temp.length === entity.length) {
                                        if (callBack) {
                                            callBack.call(self, rows, err, fields);
                                        }
                                    }
                                    resolve(++index);
                                }
                            });
                        }).then(function(index) {
                            if (index < saveEntity.length) {
                                savePromise(index);
                            }
                        }).catch(function(reason) {
                            self["delete" + $entity_upperName + "ByProps"]({
                                where: temp.fill($entity.PRIMARY_KEY + "=?").join(" or "),
                                sqlParam: temp.reduce(function(prev, curr, currentIndex, array) {
                                    prev.push(curr[PRIMARY_KEY]);
                                    return prev;
                                }, [])
                            });
                        });
                    }
                    savePromise(0);
                } else {
                    var keys = Object.keys(saveEntity);
                    var sql = "INSERT INTO " + $entity_name + "(" + keys.join(",") + ") VALUES (" + keys.fill("?").join(",") + ")";
                    run({
                        sql: sql,
                        sqlParam: ObjectValues(saveEntity),
                        callBack: function(rows, err, fields) {
                            if (err) throw err;
                            if (callBack) {
                                callBack.call(self, rows, err, fields);
                            }
                        }
                    })
                }
            }

            function deleteByProps(opt, callBack) {
                var sql = "delete from " + $entity_name;
                var param = null;
                if (opt && opt.where) {
                    sql += " where " + opt.where;
                    param = opt.param
                }

                run({
                    sql: sql,
                    sqlParam: param,
                    callBack: callBack
                })
            }

            function queryByProps(opt, callBack) {
                opt.props = opt.props || ["*"];
                var props = opt.props.join(", ");
                var limit = opt.limit || '';
                var sql = "SELECT " + props + " from " + $entity_name;
                var param = null;
                if (opt.where) {
                    sql += " where " + opt.where + " " + limit;
                    param = opt.param
                }

                run({
                    sql: sql,
                    sqlParam: param,
                    callBack: callBack
                })
            }

            function queryCount(opt, callBack) {
                var alias = opt.alias || '';
                var sql = "SELECT count(*) " + alias + " from " + $entity_name;
                var param = null;
                if (opt.where) {
                    sql += " where " + opt.where;
                    param = opt.param
                }

                run({
                    sql: sql,
                    sqlParam: param,
                    callBack: callBack
                })
            }

            function updateByProps(opt, callBack) {
                if (opt.props && opt.props.length > 0) {
                    var props = opt.props.reduce(function(prev, curr) {
                        prev.push(curr + "=?");
                        return prev;
                    }, []).join(", ");
                    var sql = "UPDATE " + $entity_name + " SET " + props;
                    var param = null;
                    if (opt) {
                        sql += " where " + opt.where;
                        param = opt.param
                    }

                    run({
                        sql: sql,
                        sqlParam: param,
                        callBack: callBack
                    })
                }
            }

            function runSql(opt, callBack) {
                run({
                    sql: opt.sql,
                    sqlParam: opt.param,
                    callBack: callBack
                })
            }

        }
    }
}

function run(opt) {
    var sql = opt.sql;
    var sqlParam = opt.sqlParam;
    var callBack = opt.callBack;
    conn = mysql.createConnection(config);
    conn.connect();

    log("sql : " + sql);
    log("sqlParam : " + sqlParam);
    conn.query(sql, sqlParam, function(err, rows, fields) {
        // if (err) throw err;
        if (err) console.log(err);
        if (callBack) {
            opt.callBack(rows, err, fields);
        }
    });

    conn.end();
}

module.exports = {
    config: function(config) {
        init(config);
        buildEntityMap.call(this, config);
        return this;
    },
    run: run,
    entitiesDao: {}
}
