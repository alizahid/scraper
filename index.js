const {
  BLIZZARD_CLIENT_ID,
  BLIZZARD_CLIENT_SECRET,
  MAX_ITEMS,
  MAX_PETS,
  MAX_QUESTS,
  MAX_SPELLS,
  MONGO_DB,
  MONGO_URI,
  SEARCH_URI
} = process.env

const { MongoClient: mongo } = require('mongodb')
const { get, range } = require('lodash')
const async = require('async')
const request = require('request-promise-native')

class Scraper {
  static async init() {
    const start = Date.now()

    const client = await mongo.connect(
      MONGO_URI,
      {
        useNewUrlParser: true
      }
    )

    this.db = client.db(MONGO_DB)

    await this.getAccessToken()

    await this.collections()
    await this.data()

    await request(SEARCH_URI + '?action=reload')

    console.log('done', Date.now() - start / 1000)

    process.exit()
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

  static collections() {
    return new Promise(resolve => {
      const tasks = [
        {
          key: 'achievements',
          uri: '/data/character/achievements',
          parser: data =>
            data.reduce((data, { achievements, categories }) => {
              if (achievements) {
                data.push(...achievements)
              }

              if (categories) {
                categories.forEach(({ achievements }) => {
                  if (achievements) {
                    data.push(...achievements)
                  }
                })
              }

              return data
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

      const queue = async.queue(async (task, callback) => {
        const { collection, key, parser, primaryKey, uri } = task

        const response = await this.request(uri)

        let data = get(response, key)

        if (parser) {
          data = parser(data)
        }

        console.log('collections', key, data.length)

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

        callback()
      }, 80)

      queue.drain = () => resolve()

      queue.push(tasks)
    })
  }

  static async data() {
    return new Promise(resolve => {
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
        },
        {
          collection: 'spells',
          max: Number(MAX_SPELLS),
          uri: '/spell/{id}'
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

      const queue = async.queue(async (task, callback) => {
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

        callback()
      }, 80)

      queue.drain = () => resolve()

      queue.push(tasks)
    })
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
