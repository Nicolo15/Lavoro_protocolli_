using RabbitMQ.Client;
using System;
using System.Text;

namespace NetCoreClient.Protocols
{
    internal class Amqp : IProtocolInterface
    {
        private const string EXCHANGE_NAME = "sensori_exchange";
        private const string ROUTING_KEY_PREFIX = "casette.v1.id_1.sensori.";
        private readonly IConnection connection;
        private readonly IModel channel;

        public Amqp(string connectionString)
        {
            //var endpoint = "amqps://nnpejxpu:wVyTzPikUKw63EzRruNgwqXD6uKlHbbr@cow.rmq2.cloudamqp.com/nnpejxpu";

            var factory = new ConnectionFactory()
            {
                Uri = new Uri(connectionString), // Inserisci l'URL del server
                AutomaticRecoveryEnabled = true, // Abilita il ripristino automatico in caso di disconnessione
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10) // Tempo di attesa prima del tentativo di ripristino
            };

            try
            {

                this.connection = factory.CreateConnection();

                this.channel = connection.CreateModel();


                // Crea la connessione
                Console.WriteLine("Connessione AMQP riuscita.");

                // Crea il canale
                Console.WriteLine("Canale AMQP creato con successo.");

                // Dichiarazione di un exchange (Direct)
                channel.ExchangeDeclare(exchange: EXCHANGE_NAME, type: ExchangeType.Topic, durable: true);
                Console.WriteLine($"Exchange '{EXCHANGE_NAME}' dichiarato.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Errore nella connessione o configurazione AMQP: {ex.Message}");
                throw;
            }
        }

        public void Send(string data, string sensor)
        {
            try
            {
                // Costruzione della routing key
                var routingKey = ROUTING_KEY_PREFIX + sensor;

                // Conversione del payload in array di byte
                var messageBody = Encoding.UTF8.GetBytes(data);

                // Pubblicazione del messaggio
                channel.BasicPublish(
                    exchange: EXCHANGE_NAME,
                    routingKey: routingKey,
                    basicProperties: null,
                    body: messageBody
                );

                Console.WriteLine($"[AMQP] Messaggio inviato a '{routingKey}': {data}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Errore nell'invio del messaggio: {ex.Message}");
                throw;
            }
        }

        ~Amqp()
        {
            try
            {
                // Chiudi il canale e la connessione
                channel?.Close();
                connection?.Close();
                Console.WriteLine("Connessione AMQP chiusa.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Errore durante la chiusura della connessione: {ex.Message}");
            }
        }
    }
}