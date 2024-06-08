""" 
BBQ Consumer A
Name: Hanna Anenia
Date: 6/7/24

"""
import pika
import sys
import time
from collections import deque
import re
from util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# Define deque outside of functions
foodA_deque = deque(maxlen=20)

def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    logger.info(f"[x] Received: {body.decode()}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        body_decode = body.decode('utf-8')
        temps = re.findall(r'(\d+\.\d+)', body_decode)
        if temps:
            temps_float = float(temps[0])
            foodA_deque.append(temps_float)
            logger.info(f"Appended temperature: {temps_float}. Current deque: {list(foodA_deque)}")

            if len(foodA_deque) == foodA_deque.maxlen:
                temp_diff = abs(foodA_deque[0] - temps_float)
                if temp_diff <= 1:
                    logger.info(f'''
                        ************************ [FOOD-A ALERT!!!!] *****************************
                        Food-A Temperature Stalled! Change: {temp_diff} in 10 minutes!
                        Please Check Fuel Source and Lid Closure!!!
                        *************************************************************************
                    ''')
        else:
            logger.warning(f"No valid temperature found in message: {body_decode}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main(hn: str = "localhost", qn: str = "02-Food-A"):
    """ Continuously listen for task messages on a named queue."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        logger.error(f"ERROR: connection to RabbitMQ server failed. The error is {e}.")
        sys.exit(1)

    try:
        channel = connection.channel()
        channel.queue_declare(queue=qn, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=qn, on_message_callback=callback, auto_ack=False)
        logger.info(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Error: Something went wrong. Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt. Stopping the Program")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()

if __name__ == "__main__":
    main("localhost", "02-Food-A")

