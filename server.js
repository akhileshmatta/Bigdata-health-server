const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { spawn } = require('child_process');
const csv = require('csv-parser');


const app = express();
app.use(cors({ origin: "*" }));
app.use(bodyParser.json());

const PORT = process.env.PORT || 13467;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}.`);
});

app.use(bodyParser.urlencoded({ extended: true }));

// simple route
app.get("/", (req, res) => {
    res.json({ message: "Welcome to Nodejs Express API application." });
});

app.get('/fraud/:id', (req, res) => {
    const patient_id = req.params.id;
    const process = spawn('python', ['./deviation.py', patient_id]);
    process.stdout.on('end', () => {
        const file = require('fs');
        file.readFile('deviation', 'utf8', (err, data) => {
            let memo = {
                deviation: +data, 
                status: 'completed'
            }
            res.json(memo);
        })
    });
});

app.get('/discharge', (req, res) => {
    // 53346892
    console.log();
    console.log();
    console.time('sql-query-time');
    const process = spawn('python', ['./discharge.py', req.query.patient_id]);
    process.on('error', e => {
        res.send({'errno': e.message});
        return;
    });
    process.stdout.on('out', (e) => {
        console.log(e)
    });
    
    process.stdout.on('end', () => {
        console.timeEnd('sql-query-time');
        res.send({message: 'file created'})
    });
});

app.get('/insurance', (req, res) => {
    console.log('hello');
    // console.time('hdfs-time');
    result = [];
    const file = require('fs');
    // let path = `/usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/${req.query.patient_id}.csv/part-00011-cf315c36-fc57-4e5a-9e3b-1039b836b3f6-c000.csv`;
    let path = `/usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/${req.query.patient_id}.csv`;
    // file.readdir(path, (err, files) => {
    //     if (err) {
    //         res.send({'errno': err.message}); 
    //         return;
    //     }
    //     let file_path;
    //     for (const file of files) {
    //         if (file.slice(0, 10) === 'part-00011') {
    //             file_path = path + '/' + file;
    //             break;
    //         }
    //     }
        console.log(`./panda_excel/${req.query.patient_id}.csv`);
        file.createReadStream(`./panda_excel/${req.query.patient_id}.csv`)
        // file.createReadStream(file_path)
        .on('error', (err) => {
            console.log(err);
            res.send({'errno': err.message}); 
            return;
        })
        .pipe(csv({}))
        .on('data', (data) => result.push(data))
        .on('end', () => {
            // console.timeEnd('hdfs-time');
            res.send(result[0]);
        });
    // })
});

require("./app/routes/database.routes.js")(app);

