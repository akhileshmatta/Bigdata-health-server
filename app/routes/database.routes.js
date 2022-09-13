
module.exports = app => {
    const database = require("../controllers/database.controller.js");
    app.get("/query", database.query);
}
