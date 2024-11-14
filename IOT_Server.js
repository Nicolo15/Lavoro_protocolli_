// Importa i moduli necessari
var restify = require('restify');
var mysql = require('mysql2');

// Crea il server Restify
var server = restify.createServer();
server.use(restify.plugins.bodyParser());

// Configura la connessione al database MySQL
var db = mysql.createConnection({
    host: 'localhost', // Host del database
    user: 'root',      // Cambia con il tuo utente del database
    password: 'Vmware1!', // Cambia con la tua password del database
    database: 'water_monitoring' // Nome del database
});

// Connetti al database
db.connect(function(err) {
    if (err) {
        console.error('Errore di connessione al database: ' + err.stack);
        return;
    }
    console.log('Connesso al database MySQL come ID ' + db.threadId);
});

// Endpoint per ottenere la lista delle casette dell'acqua
server.get('/water_coolers', function(req, res, next) {
    db.query('SELECT * FROM coolers', function(err, results) {
        if (err) {
            res.send(500, { error: err.message });
            return next();
        }
        res.send(results);
        return next();
    });
});

// Endpoint per ottenere i dettagli di una casetta specifica
server.get('/water_coolers/:id', function(req, res, next) {
    var coolerId = req.params['id'];
    db.query('SELECT * FROM coolers WHERE id = ?', [coolerId], function(err, result) {
        if (err) {
            res.send(500, { error: err.message });
            return next();
        }
        res.send(result);
        return next();
    });
});

// Endpoint per salvare i dati inviati da una casetta
server.post('/water_coolers/:id', function(req, res, next) {
    var coolerId = req.params['id'];  // Il CoolerId viene preso come parametro nella URL
    var data = req.body;  // I dati vengono presi dal corpo della richiesta

    var query = 'INSERT INTO cooler_data (CoolerId, LitersDispensed, WaterTemperature, FilterStatus, NightLight, MaintenanceMode) VALUES (?, ?, ?, ?, ?, ?)';

    db.query(query, [
        coolerId,
        data.LitersDispensed,
        data.WaterTemperature,
        data.FilterStatus,  // Correzione del nome 'filter_status' a 'FilterStatus'
        data.NightLight,    // Correzione del nome 'night_light' a 'NightLight'
        data.MaintenanceMode  // Correzione del nome 'maintenance_mode' a 'MaintenanceMode'
    ], function(err, result) {
        if (err) {
            res.send(500, { error: err.message });
            return next();
        }
        res.send({ message: 'Dati ricevuti e salvati con successo', id: result.insertId });
        console.log('Dati ricevuti:', data);
        return next();
    });
});

// Avvio del server
server.listen(8011, function() {
    console.log('%s listening at %s', server.name, server.url);
});
