using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace NetCoreClient.Protocols
{
    public class Http : ProtocolInterface
    {
        private readonly string _endpoint;
        private readonly HttpClient _client;

        public Http(string endpoint)
        {
            _endpoint = endpoint;
            _client = new HttpClient();
        }

        // Metodo per inviare i dati al server
        public async void Send(string jsonData)
        {
            try
            {
                var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                // Invia la richiesta POST
                var response = await _client.PostAsync(_endpoint, content);

                // Verifica la risposta del server
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("Dati inviati con successo.");
                }
                else
                {
                    // Mostra il codice di stato e il contenuto della risposta in caso di errore
                    string errorMessage = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Errore nell'invio dei dati. Codice errore: " + response.StatusCode);
                    Console.WriteLine("Messaggio di errore: " + errorMessage);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Errore di connessione: " + ex.Message);
            }
        }
    }
}
