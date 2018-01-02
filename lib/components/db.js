var dbTable = require('../dbTable');
module.exports = function(app, opts) {
    var server = new dbTable(app, opts);
    app.set('dbTable', server, true);
    return server;
};