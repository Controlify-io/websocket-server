const mysql = require('mysql');

module.exports = class DB {
    constructor(options) {
        let defaults = {
            connectionLimit : 100,
            host     : 'localhost',
            debug    :  false,
        };
        for (var option in defaults) {
            if (typeof(options[option])==undefined) options[option]=defaults[options];
        }
        this.pool = mysql.createPool(options);
    } 

    getValue(...args) {
        return this.query(...args).then(function(results){
            if (results.length==0) return undefined;
            let firstRow = results.shift();
            return firstRow[Object.keys(firstRow)[0]];
        });
    }

    getRow(...args) {
        return this.query(...args).then(function(results){
            if (results.length==0) return false;
            return results.shift();
        });
    }

    getRows(...args) {
        return this.query(...args).then(function(results){
            if (results.length==0) return false;
            return results;
        });
    }

    insert(table,data,type='INSERT') {
        if (['INSERT','INSERT IGNORE','REPLACE'].indexOf(type) < 0) type='INSERT';
        return this.query(type+' INTO ?? SET ?',table,data).then(function(results){
            return results.insertId;
        });
            
    }

    delete(table,wheres) {
        var [ whereSql, whereParams ] = this.buildConditions(wheres);
        var params = ['DELETE FROM ?? WHERE '+whereSql,table];
        if (whereParams.length) params.push.apply(params,whereParams);

        return this.query.apply(this,params).then(function(results){
            return results.affectedRows;
        });
    }

    update(table,wheres,data) {
        var [ whereSql, whereParams ] = this.buildConditions(wheres);
        var params = [table,data];
        let sql = 'UPDATE ?? SET ? WHERE '+whereSql;
        if (whereParams.length) params.push.apply(params,whereParams);
        params.unshift(sql);
        return this.query.apply(this,params).then(function(results){
            if (results.length==0) return undefined;
            return results.changedRows;
        });
    }
    
    exec(...args) {
        return this.query(...args).then(function(results){
            return results.insertId;
        });
    }

    selectRow(table,wheres,columns='*') {
        let params = [];
        if (typeof(columns)=='object' || typeof(columns)=='array') {
            let columnStr = '';
            for (var column in columns) {
                columnStr += '??,';
                params.push(column);
            }
            if (columns=='') columns='*';
            else columns=columnStr.slice(0,-1);
        }

        var [ whereSql, whereParams ] = this.buildConditions(wheres);
        let sql = 'SELECT '+columns+' FROM ?? WHERE '+whereSql;
        params.push(table);

        // add the where params on the end of the list of params
        if (whereParams.length) params.push.apply(params,whereParams);
        // put the sql on the front of the list of params
        params.unshift(sql);
        return this.query.apply(this,params).then(function(results){
            if (results.length==0) return undefined;
            return results.shift();
        });
    }

    buildConditions(wheres) {
        if (!Object.keys(wheres).length) return [' 1=1 ',[]];
        let whereSql = ' ';
        let whereParams = [];
        for (var column in wheres) {
            whereSql += '?? = ? AND';
            whereParams.push( column, wheres[column] );
        }
        whereSql = whereSql.slice(0,-3);
        return [ whereSql, whereParams ];
    }

    query() {
        var params = Array.prototype.slice.call(arguments);
        var sql = params.shift();
        var self = this;

        return new Promise(function(resolve, reject) {
           
            self.pool.getConnection(function(error, connection) {
                if (error) {
                    if (self.errorHandler) self.errorHandler(error);
                    return reject(error);
                }
                connection.query(sql, params, function (error, results, fields) {
                    connection.release();
                    if (error) {
                        return reject(error);
                    }
                    return resolve(results,fields);
                });
                
            });
        });
    }
}

