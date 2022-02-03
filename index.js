const tmi = require('tmi.js');
var mysql = require('mysql');

var con = mysql.createConnection({
	host: "localhost",
	user: "root",
	password: "",
	database: "twitch"
});

var streamer="5uppp"

con.connect();

const client = new tmi.Client({
	connection: { reconnect: true },
	channels: [ streamer ]
});

createTable(streamer)

client.connect();

client.on('message', (channel, tags, message, self) => {
	// "Alca: Hello, World!"
	insertMessage(streamer,tags.username,message);
	console.log(channel+" inserted "+ message);
});

function createTable(name) {

	con.query("CREATE TABLE IF NOT EXISTS "+name+" ( id int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,user varchar(256) NOT NULL,body text NOT NULL,created_at timestamp NOT NULL DEFAULT current_timestamp())", function (error, results, fields) {
		if (error) throw error;
		// connected!
		console.log("Table created "+name);
	});

}

function insertMessage(stream,user,message) {

	var sql = "INSERT INTO "+stream+" (user, body) VALUES ?";
  var values = [
    [user, message]
  ];
  con.query(sql, [values], function (err, result) {
    if (err) throw err;
  });
	
}