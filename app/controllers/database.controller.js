const database = require("../models/database.model.js");

exports.query = (req, res) => {
    database.process(req.query.statement, (err, data) => {
        if (err) {
            res.status(500).send({
                message: err.message || "Some error occurred while retrieving users."
            });
        } else res.send(data);
    });
};
