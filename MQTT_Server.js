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

// Variabile per contare gli inserimenti
let insertCounter = 0;

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
function saveData(field, value) {
    // Inserisce sempre una nuova riga nella tabella principale
    const queryInsert = `INSERT INTO waters_coolers (${field}) VALUES (?)`;
    db.execute(queryInsert, [value], function (err, insertResults) {
        if (err) {
            console.error(`Errore durante l'inserimento del campo ${field} nel database:`, err);
        } else {
            console.log(`${field} inserito come ${value} in una nuova riga.`);
            insertCounter++;  // Incrementa il contatore degli inserimenti

            // Verifica se sono stati inseriti 20 record
            if (insertCounter >= 20) {
                updateSummaryTable();  // Aggiorna la tabella riassuntiva
                insertCounter = 0;  // Reset del contatore
            }
        }
    });
}

// Funzione per aggiornare la tabella riassuntiva ogni 20 inserimenti
function updateSummaryTable() {
    // Esegui una query per ottenere i dati dalla tabella principale
    const querySummaryInsert = `INSERT INTO water_coolers_summary (temperature, lightstate) 
                                SELECT temperature, lightstate 
                                FROM waters_coolers 
                                ORDER BY id DESC LIMIT 20`;

    db.execute(querySummaryInsert, function (err, results) {
        if (err) {
            console.error('Errore durante l\'aggiornamento della tabella riassuntiva:', err);
        } else {
            console.log('Tabella riassuntiva aggiornata con i nuovi dati.');
        }
    });
}

// Quando ricevi un messaggio MQTT
mqttClient.on('message', function (topic, message) {
    console.log(`Messaggio ricevuto su ${topic}:`, message.toString());

    if (topic === 'sensori/test.mosquitto.org/watertemp') {
        const temperature = parseFloat(message.toString());
        if (!isNaN(temperature)) {
            saveData('temperature', temperature);
        } else {
            console.error('Il valore ricevuto per watertemp non è un numero valido:', message.toString());
        }
    } else if (topic === 'sensori/test.mosquitto.org/lightstate') {
        const lightstate = message.toString();
        saveData('lightstate', lightstate);
    } else {
        console.log('Topic non gestito:', topic);
    }
});

// Avvia il server RESTful
server.listen(8011, function () {
    console.log('%s listening at %s', server.name, server.url);
});
