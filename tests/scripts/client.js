const net = require('net')

const toJSON = (data) => JSON.stringify(data)
const fromJSON = (data) => JSON.parse(data)
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const pipe = (data) => ({
  collect: () => data,
  map: (fn) => pipe(fn(data)),
})

const producer = async () => {
  const conn = net.createConnection({
    host: '127.0.0.1',
    port: 3333
  })

  conn.write(toJSON({
    type: 'PRODUCER',
    topic: '/hello',
  }))


  conn.on('data', (data) => console.log(fromJSON(data)))
  conn.on('close', () => process.exit(0))

  for (let i = 0; i < 100; i++) {
    conn.write(toJSON({
      message: 'Hello, world! ' + i,
    }))

    await delay(10)
  }

  conn.end()
}

const consumer = async () => {
  const conn = net.createConnection({
    host: '127.0.0.1',
    port: 3333
  })

  conn.write(toJSON({
    type: 'CONSUMER',
    topic: '/hello',
  }))

  conn.on('ready', () => console.log('Connected to the server.'))
  conn.on('close', () => console.log('Disconnected from the server.'))
  conn.on('data', (raw) => {
    const data = fromJSON(raw)
    data.payload = fromJSON(data.payload)

    console.log(data)
  })
}

producer()
delay(0).then(consumer)
