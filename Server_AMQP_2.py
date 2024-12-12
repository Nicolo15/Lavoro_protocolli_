import pika
import mysql.connector

# Configurazione della connessione al database MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Vmware1!",
    database="water_coolers_db"
)

# Configurazione RabbitMQ
amqp_url = 'amqps://nnpejxpu:wVyTzPikUKw63EzRruNgwqXD6uKlHbbr@cow.rmq2.cloudamqp.com/nnpejxpu'
queue_names = [
    'casette.v1.id_1.sensori.watertemp',
    'casette.v1.id_1.sensori.lightstate'
]

def setup_rabbitmq():
    """Configura RabbitMQ e inizia a consumare i messaggi."""
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Dichiara tutte le code con 'durable=True'
    for queue_name in queue_names:
        channel.queue_declare(queue=queue_name, durable=True)
    
    print("Connesso a RabbitMQ e code create.")
    
    # Funzione per consumare i messaggi
    def on_message(ch, method, properties, body):
        message = body.decode('utf-8')
        print(f"Messaggio ricevuto: {message}")
        save_or_update('temperature' if 'watertemp' in method.routing_key else 'lightstate', message)
    
    # Consuma i messaggi
    for queue_name in queue_names:
        channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
    
    print("In ascolto per i messaggi...")
    channel.start_consuming()

def save_or_update(field, value):
    """Funzione per salvare o aggiornare i dati nel database."""
    cursor = db.cursor()
    
    # Verifica se il campo è null e aggiorna
    query_find = f"SELECT id FROM waters_coolers WHERE {field} IS NULL LIMIT 1"
    cursor.execute(query_find)
    result = cursor.fetchone()
    
    if result:
        id_to_update = result[0]
        query_update = f"UPDATE waters_coolers SET {field} = %s WHERE id = %s"
        cursor.execute(query_update, (value, id_to_update))
        db.commit()
        print(f"{field} aggiornato a {value} per la riga con ID {id_to_update}.")
    else:
        query_insert = f"INSERT INTO waters_coolers ({field}) VALUES (%s)"
        cursor.execute(query_insert, (value,))
        db.commit()
        print(f"{field} inserito come {value} in una nuova riga.")

# Avvia la configurazione RabbitMQ e inizia a consumare i messaggi
setup_rabbitmq()
