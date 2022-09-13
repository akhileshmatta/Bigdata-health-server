const sql = require("./db.js");

// constructor
const database = function(user) {
    this.Query = user.Query;
};

database.process = (Query, result) => {
    // console.log(Query);
    sql.query(Query, (err, res) => {
        if (err) {
            console.log("error: ", err);
            result(null, err);
            return;
        }
        result(null, res);
    });
};

module.exports = database;
