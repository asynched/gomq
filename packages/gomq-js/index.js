const net = require('net')

const json = (data) => JSON.stringify(data)

const StreamType = {
  PRODUCER: 'PRODUCER',
  CONSUMER: 'CONSUMER',
}

class Producer {
  #onConnectedHandlers
  #onDisconnectedHandlers
  #onErrorHandlers
  #connected
  #connection

  /**
   * # Producer
   * 
   * GOMQ stream producer.
   * 
   * @example
   * const producer = new Producer({ ... })
   * 
   * @param { { host: string, port: number, topic: string }} options Options for
   * the stream producer.
   */
  constructor({ host, port, topic }) {
    this.#connected = false

    this.host = host
    this.port = port
    this.topic = topic

    this.#onConnectedHandlers = []
    this.#onDisconnectedHandlers = []
    this.#onErrorHandlers = []

    this.#connection = net.createConnection({
      host,
      port
    })

    this.#connection.write(json({
      type: StreamType.PRODUCER,
      topic,
    }))

    this.#setupEvents()
  }

  #setupEvents() {
    this.#connection.on('error', () => {
      this.#connected = false
      this.#emitError()
    })

    this.#connection.on('close', () => {
      this.#connected = false
      this.#emitDisconnected()
    })

    this.#connection.on('ready', () => {
      this.#connected = true
      this.#emitConnected()
    })
  }

  #emitConnected() {
    this.#onConnectedHandlers.forEach(f => f())
  }

  #emitDisconnected() {
    this.#onDisconnectedHandlers.forEach(f => f())
  }

  #emitError() {
    this.#onErrorHandlers.forEach(f => f())
  }

  /**
   * # connect
   * 
   * Method that resolves when the connection has been established.
   * 
   * @example
   * const producer = new Producer({ ... })
   * await producer.connect()
   * 
   * @returns { Promise<void> }
   */
  connect() {
    return new Promise((res, rej) => {
      this.#onConnectedHandlers.push(() => res())
      this.#onErrorHandlers.push(() => rej())
      this.#onDisconnectedHandlers.push(() => rej())
    })
  }

  /**
   * # disconnect
   * 
   * Method that disconnects the stream producer from the GOMQ server.
   * 
   * @example
   * const producer = new Producer({ ... })
   * producer.disconnect()
   */
  disconnect() {
    this.#connection.end()
  }

  /**
   * # write
   * 
   * Method that writes a message to the stream producer.
   * 
   * @example
   * const producer = new Producer({ ... })
   * producer.write({ message: 'Hello, world!' })
   * 
   * @param { Record<string, any> } data Data to send to the GOMQ server.
   */
  write(data) {
    if (!this.#connected) {
      throw new Error('Not connected to the server.')
    }

    this.#connection.write(json(data) + '\0')
  }
}

class Consumer {
  /**
   * 
   * @param { { host: string, port: number, topic: string } } options 
   */
  constructor({ host, port, topic }) {
    this.conn = net.createConnection({
      host,
      port
    })

    this.conn.write(json({
      type: StreamType.CONSUMER,
      topic,
    }))
  }

  /**
   * # handle
   * 
   * Method that handles the messages received from the GOMQ server.
   * 
   * @example
   * const consumer = new Consumer({ ... })
   * consumer.handle((data) => {
   *   console.log(data.toString())
   * })
   * 
   * @param { (data: Buffer) => unknown } handler Handler function that will be
   * called when a message is received from the GOMQ server.
   */
  handle(handler) {
    this.conn.on('data', handler)
  }
}

module.exports = {
  Producer,
  Consumer,
}
