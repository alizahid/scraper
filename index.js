const API_KEYS = [
  'u23bhd59r763q3a5zbtn7tdjwc3qyu6z',
  '4b55nn5e39n5ysvjrpdffedjdsh7zeqd',
  'ud2c3qa8kevg72k36p5p2tus74h6hjc7',
  'cqb42h7f596feq3qmgskc7kt9wm97g6v',
  'ef4ejs8zy2d7dj2p3vjcwgef3h8ca2mc',
  'v6eb33v33qmbav9cksf5mcr55zr6wsfc'
]

const fs = require('fs')
const http = require('http')

const fetch = require('node-fetch')
const mongo = require('mongodb').MongoClient
const moment = require('moment')
const range = require('lodash.range')

class Scraper {
  constructor() {
    this.key = API_KEYS[0]

    mongo.connect('mongodb://localhost:27017', (err, client) => {
      if (err) {
        throw err
      }

      this.db = client.db('bigglesworth')

      this.server = http.createServer(async (request, response) => {
        const { url } = request

        if (url === '/?download') {
          const src = fs.createReadStream('../wowhead.tar.gz')

          src.pipe(response)

          return
        }

        const { current, quotaNow, quota } = this

        const last = await this.last()
        const total = await this.count()

        const json = {
          current,
          last,
          total,
          quotaNow,
          quota
        }

        const data = JSON.stringify(json)

        response.setHeader('content-type', 'application/json')

        response.end(data)
      })

      this.server.listen(3030, err => {
        if (err) {
          throw error
        }
      })

      this.data()
    })
  }

  async data() {
    const data = [
      {
        id: 'pet',
        collection: 'pets'
      },
      {
        id: 'boss',
        collection: 'bosses'
      },
      {
        id: 'mount',
        collection: 'mounts'
      },
      {
        id: 'zone',
        collection: 'zones'
      }
    ]

    for (const type of data) {
      const { id, collection } = type

      const response = await fetch(
        `https://us.api.battle.net/wow/${id}/?locale=en_US&apikey=${this.key}`
      )

      const json = await response.json()

      for (const item of json[collection]) {
        await this.add(item, collection)
      }
    }
  }

  async start() {
    const start = await this.last()
    const max = 52000

    if (start > max) {
      return
    }

    const quests = range(start + 1, max)

    for (const id of quests) {
      await this.fetch(id)
    }
  }

  async fetch(id) {
    const { key } = this

    this.current = id

    console.log('fetching', id)

    const response = await fetch(
      `https://us.api.battle.net/wow/quest/${id}?locale=en_US&apikey=${key}`
    )

    const { headers, status } = response

    const currentNow = parseInt(headers.get('x-plan-qps-current')) + 2
    const maxNow = parseInt(headers.get('x-plan-qps-allotted'))

    this.quotaNow = {
      curret: currentNow,
      max: maxNow
    }

    if (currentNow >= maxNow) {
      console.log('\t', '\t', 'delaying')

      await this.delay()
    }

    const currentTotal = parseInt(headers.get('x-plan-quota-current')) + 5
    const maxTotal = parseInt(headers.get('x-plan-quota-allotted'))

    this.quota = {
      current: currentTotal,
      max: maxTotal
    }

    if (currentTotal >= maxTotal) {
      this.switchKey()
    }

    if (status !== 200) {
      console.log('\t', 'invalid')

      return
    }

    const quest = await response.json()

    console.log('\t', 'saving')

    this.add(quest, 'quests')
  }

  add(item, collection) {
    const { db } = this

    const items = db.collection(collection)

    return new Promise((resolve, reject) =>
      items.insert(item, (err, result) => {
        if (err) {
          return reject(err)
        }

        resolve(result)
      })
    )
  }

  switchKey() {
    console.log('-', 'switching key')

    const { key } = this

    const next = API_KEYS.findIndex(k => k === key) + 1

    if (API_KEYS[next]) {
      this.key = API_KEYS[next]
    } else {
      this.key = API_KEYS[0]
    }
  }

  delay() {
    const time = moment()
      .startOf('minute')
      .add(1, 'minutes')
      .diff(moment(), 'milliseconds')

    return new Promise(resolve => setTimeout(resolve, time))
  }

  async last() {
    const { db } = this

    const quests = db.collection('quests')

    const quest = (await quests
      .find()
      .sort({
        id: -1
      })
      .limit(1)
      .toArray()).pop()

    return quest ? quest.id : 0
  }

  count() {
    const { db } = this

    const quests = db.collection('quests')

    return quests.count()
  }
}

const scraper = new Scraper()
