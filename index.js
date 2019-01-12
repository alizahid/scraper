require('dotenv').config()

const {
  BLIZZARD_CLIENT_ID,
  BLIZZARD_CLIENT_SECRET,
  MAX_ITEMS,
  MAX_PETS,
  MAX_QUESTS,
  MONGO_DB,
  MONGO_URI
} = process.env

const { MongoClient: mongo } = require('mongodb')
const { get, range } = require('lodash')
const async = require('async')
const request = require('request-promise-native')

class Scraper {
  static async init() {
    const start = Date.now()

    this.queue = async.queue(async (task, callback) => {
      const { type } = task

      if (type === 'collection') {
        const { collection, key, parser, primaryKey, uri } = task

        const response = await this.request(uri)

        let data = get(response, key)

        if (parser) {
          data = parser(data)
        }

        console.log('collections', data.length, key)

        await Promise.all(
          data.map(item =>
            this.db.collection(collection || key).updateOne(
              {
                [primaryKey || 'id']: item[primaryKey || 'id']
              },
              {
                $set: item
              },
              {
                upsert: true
              }
            )
          )
        )
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
    }, 80)

    this.queue.drain = () => {
      console.log('done', Date.now() - start / 1000)

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
        key: 'bosses',
        uri: '/boss/'
      },
      {
        key: 'mounts',
        primaryKey: 'spellId',
        uri: '/mount/'
      },
      {
        key: 'pets',
        primaryKey: 'creatureId',
        uri: '/pet/'
      },
      {
        key: 'zones',
        uri: '/zone/'
      },
      {
        collection: 'character_races',
        key: 'races',
        uri: '/data/character/races'
      },
      {
        collection: 'character_classes',
        key: 'classes',
        uri: '/data/character/classes'
      },
      {
        collection: 'item_classes',
        key: 'classes',
        primaryKey: 'class',
        uri: '/data/item/classes'
      }
    ]

    this.queue.push(
      tasks.map(collection => ({
        ...collection,
        type: 'collection'
      }))
    )
  }

  static async data() {
    const tasks = [
      {
        collection: 'pets',
        max: Number(MAX_PETS),
        uri: '/pet/species/{id}'
      },
      {
        collection: 'items',
        max: Number(MAX_ITEMS),
        uri: '/item/{id}'
      },
      {
        collection: 'quests',
        max: Number(MAX_QUESTS),
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
        console.log('request', 'throttled', uri)

        await this.delay()

        return this.request(uri)
      }

      console.log('request', 'error', uri, JSON.stringify(body))
    }
  }

  static delay() {
    return new Promise(resolve => setTimeout(resolve, 1000))
  }
}

Scraper.init()
