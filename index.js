require('dotenv').config()

const {
  BLIZZARD_CLIENT_ID,
  BLIZZARD_CLIENT_SECRET,
  MONGO_DB,
  MONGO_URI
} = process.env

const { MongoClient: mongo } = require('mongodb')
const { get, range } = require('lodash')
const async = require('async')
const request = require('request-promise-native')

class Scraper {
  static async init() {
    this.queue = async.queue(async (task, callback) => {
      const { type } = task

      if (type === 'collection') {
        const { collection, key, parser, uri } = task

        const response = await this.request(uri)

        let data = get(response, key)

        if (parser) {
          data = parser(data)
        }

        console.log('collections', data.length, key)

        await this.db.collection(collection || key).insertMany(data)
      } else if (type === 'data') {
        const { collection, id, uri } = task

        const data = await this.request(uri.replace('{id}', id))

        const query = {}

        if (collection === 'pets') {
          query.creatureId = data.creatureId
        } else {
          query.id = id
        }

        console.log('data', collection, id)

        await this.db.collection(collection).updateOne(
          query,
          {
            $set: data
          },
          {
            upsert: true
          }
        )
      }

      callback()
    }, 40)

    this.queue.drain = () => {
      process.exit()
    }

    const client = await mongo.connect(
      MONGO_URI,
      {
        useNewUrlParser: true
      }
    )

    this.db = client.db(MONGO_DB)

    await this.getAccessToken()

    this.collections()
    this.data()
  }

  static async getAccessToken() {
    const { access_token } = await request.post({
      uri: 'https://us.battle.net/oauth/token',
      json: true,
      auth: {
        username: BLIZZARD_CLIENT_ID,
        password: BLIZZARD_CLIENT_SECRET
      },
      formData: {
        grant_type: 'client_credentials'
      }
    })

    this.accessToken = access_token
  }

  static async collections() {
    const tasks = [
      {
        type: 'collection',
        key: 'achievements',
        uri: '/data/character/achievements',
        parser: data =>
          data.reduce((all, { achievements, categories }) => {
            if (achievements) {
              all.push(...achievements)
            }

            if (categories) {
              categories.forEach(({ achievements }) => {
                if (achievements) {
                  all.push(...achievements)
                }
              })
            }

            return all
          }, [])
      },
      {
        type: 'collection',
        key: 'bosses',
        uri: '/boss/'
      },
      {
        type: 'collection',
        key: 'mounts',
        uri: '/mount/'
      },
      {
        type: 'collection',
        key: 'pets',
        uri: '/pet/'
      },
      {
        type: 'collection',
        key: 'zones',
        uri: '/zone/'
      },
      {
        type: 'collection',
        key: 'races',
        collection: 'character_races',
        uri: '/data/character/races'
      },
      {
        type: 'collection',
        key: 'classes',
        collection: 'character_classes',
        uri: '/data/character/classes'
      },
      {
        type: 'collection',
        key: 'classes',
        collection: 'item_classes',
        uri: '/data/item/classes'
      }
    ]

    this.queue.push(tasks)
  }

  static async data() {
    const tasks = [
      {
        collection: 'pets',
        max: 2569 + 50,
        uri: '/pet/species/{id}'
      },
      {
        collection: 'items',
        max: 166999 + 1000,
        uri: '/item/{id}'
      },
      {
        collection: 'quests',
        max: 54978 + 200,
        uri: '/quest/{id}'
      }
    ].reduce((tasks, { collection, max, uri }) => {
      range(1, max).forEach(id =>
        tasks.push({
          collection,
          id,
          max,
          uri,
          type: 'data'
        })
      )

      return tasks
    }, [])

    this.queue.push(tasks)
  }

  static async request(uri) {
    try {
      console.log('request', `https://us.api.blizzard.com/wow${uri}`)

      const response = request({
        uri: `https://us.api.blizzard.com/wow${uri}`,
        json: true,
        headers: {
          authorization: `Bearer ${this.accessToken}`
        }
      })

      return response
    } catch (error) {
      const { body, statusCode } = error

      if (statusCode === 429) {
        await this.delay()

        return this.request(uri)
      }

      console.log('error', uri, JSON.stringify(body))
    }
  }

  static delay() {
    return new Promise(resolve => setTimeout(resolve, 1000))
  }
}

Scraper.init()
