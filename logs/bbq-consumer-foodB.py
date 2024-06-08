""" 
BBQ Consumer B
Name: Hanna Anenia
Date: 6/7/24

This Python script monitors "Food A" temperature by reading messages from a RabbitMQ 
queue and tracking recent readings with a deque. If the temperature change is minimal 
over 10 minutes,

"""

import pika
import sys
from collections import deque
from util_logger import setup_logger

# Constants
HOST = 'localhost'
MAX_DEQUE_SPACE = 20
QUEUE_NAME = '02-Food-B'

# Setup custom logging
logger, logname = setup_logger(__file__)

# Create a deque window
window = deque(maxlen=MAX_DEQUE_SPACE)

def deque_and_flag(msg_body):
    try:
        timestamp, temperature = msg_body.split(", ")
        temperature = float(temperature)
        window.append(temperature)
        logger.info(f"Appended temperature: {temperature}. Current deque: {list(window)}")

        if len(window) == MAX_DEQUE_SPACE:
            temp_diff = abs(window[0] - window[-1])
            if 0 <= temp_diff <= 1.0:
                logger.info(f'FLAG: Temperature Stagnate in Food B')
                logger.info(f'\tOld Temperature: {window[0]}\n\tNew Temperature: {window[-1]}')
                logger.info(f'\tTimestamp: {timestamp}\n')
    except ValueError as e:
        logger.error(f"Error processing message: {e}")

def callback(ch, method, properties, body):
    logger.info(f"Received message: {body.decode()}")
    deque_and_flag(body.decode())
    logger.info(f'Received and processed {body.decode()}')
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(host_name='localhost', queue_name='02-Food-B'):
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=host_name))
    except Exception as e:
        logger.error(f"ERROR: connection to RabbitMQ server failed. The error is {e}.")
        sys.exit(1)

    try:
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        logger.info('Ready for action! Press CTRL + C to manually close the connection.')
        ch.start_consuming()
    except Exception as e:
        logger.error(f"ERROR: something went wrong. The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info('User interrupted continuous listening process')
        sys.exit(0)
    finally:
        logger.info('Closing Connection...')
        conn.close()

if __name__ == '__main__':
    main(HOST, QUEUE_NAME)

