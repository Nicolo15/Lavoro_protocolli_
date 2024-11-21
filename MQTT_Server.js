const restify = require('restify');
const mqtt = require('mqtt');
const mysql = require('mysql2');

// Crea il server RESTful
var server = restify.createServer();
server.use(restify.plugins.bodyParser());

// Configurazione della connessione al database MySQL
const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Vmware1!',
    database: 'water_coolers_db'
});

// Connessione al broker MQTT
const mqttClient = mqtt.connect('mqtt://test.mosquitto.org');

// Quando il client MQTT è connesso
mqttClient.on('connect', function () {
    console.log('Connesso al broker MQTT per ricevere comandi');
    mqttClient.subscribe('sensori/#', function (err) {
        if (!err) {
            console.log('Sottoscritto ai topic dei comandi');
        }
    });
});

// Funzione per salvare i dati nel database in modo automatico
function saveOrUpdate(field, value) {
    // Cerca una riga esistente con valori parziali
    const queryFind = `SELECT id FROM waters_coolers WHERE ${field} IS NULL LIMIT 1`;

    db.execute(queryFind, function (err, results) {
        if (err) {
            console.error('Errore durante la ricerca nel database:', err);
        } else if (results.length > 0) {
            // Esiste una riga con il campo vuoto, aggiorna quella riga
            const idToUpdate = results[0].id;
            const queryUpdate = `UPDATE waters_coolers SET ${field} = ? WHERE id = ?`;
            db.execute(queryUpdate, [value, idToUpdate], function (err, updateResults) {
                if (err) {
                    console.error(`Errore durante l'aggiornamento del campo ${field} nel database:`, err);
                } else {
                    console.log(`${field} aggiornato a ${value} per la riga con ID ${idToUpdate}.`);
                }
            });
        } else {
            // Nessuna riga esistente, inserisce una nuova
            const queryInsert = `INSERT INTO waters_coolers (${field}) VALUES (?)`;
            db.execute(queryInsert, [value], function (err, insertResults) {
                if (err) {
                    console.error(`Errore durante l'inserimento del campo ${field} nel database:`, err);
                } else {
                    console.log(`${field} inserito come ${value} in una nuova riga.`);
                }
            });
        }
    });
}

// Quando ricevi un messaggio MQTT
mqttClient.on('message', function (topic, message) {
    console.log(`Messaggio ricevuto su ${topic}:`, message.toString());

    if (topic === 'sensori/test.mosquitto.org/watertemp') {
        const temperature = parseFloat(message.toString());
        if (!isNaN(temperature)) {
            saveOrUpdate('temperature', temperature);
        } else {
            console.error('Il valore ricevuto per watertemp non è un numero valido:', message.toString());
        }
    } else if (topic === 'sensori/test.mosquitto.org/lightstate') {
        const lightstate = message.toString();
        saveOrUpdate('lightstate', lightstate);
    } else {
        console.log('Topic non gestito:', topic);
    }
});



// Avvia il server RESTful
server.listen(8011, function () {
    console.log('%s listening at %s', server.name, server.url);
});
