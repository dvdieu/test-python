import pika

credentials = pika.PlainCredentials('lgcikfpo', 'GWsAJEi8fuU7_yD-GiDXh6oz1-Uqm774')


def connectAMQP():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='cougar.rmq.cloudamqp.com', port=5672, virtual_host="lgcikfpo", credentials=credentials))
    return connection

