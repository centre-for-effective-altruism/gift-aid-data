const console = require('better-console')
const co = require('co')
const _ = require('lodash')
const throat = require('throat')
const pify = require('pify')
const promiseRetry = require('promise-retry')
const knex = require('./lib/db')
const {getWebformData} = require('./lib/webform')
const {geocodeAddress, normalizeAddress} = require('./lib/geocoding')

const path = require('path')
const fs = pify(require('fs'))

const {DATA_DIRECTORY} = require('./lib/helpers')

function * run () {
  try {
    const MissingAddresses = []
    const Webforms = yield getWebformData()
    // extract addresses from webforms where people have claimed Gift Aid
    const Addresses = yield Promise.all(Webforms
      .filter(Webform => Webform.giftaid && Webform.address)
      .map(throat(50, co.wrap(function * (Webform) {
        const AddressString = Webform.address.replace(/\n/g, ', ')
        console.info(`Geocoding ${AddressString}...`)
        const AddressData = yield promiseRetry((retry) => {
          return geocodeAddress(AddressString)
            .catch(err => {
              console.warn(err.toString())
              retry(err)
            })
        })
        const Address = AddressData[0]
        if (!Address) {
          MissingAddresses.push({
            email: Webform.email.toLowerCase().trim(),
            address: AddressString
          })
          console.warn(`${AddressString} could not be geocoded`)
          return null
        }
        return {
          email: Webform.email.toLowerCase().trim(),
          address: normalizeAddress(Address, AddressString)
        }
      })))
    )
      .then(Addresses => Addresses.filter(A => A)) // filter any addresses that couldn't be geocoded
      .then(Addresses => _.uniqWith(Addresses, (arrVal, othVal) => {
        // filter duplicates on email and place ID
        if (!arrVal.address.google_place_id) return false // don't compare undefined/nulls
        return (arrVal.email === othVal.email && arrVal.address.google_place_id === othVal.address.google_place_id)
      }))

    // create data directory
    try {
      yield fs.mkdir(DATA_DIRECTORY)
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err
      }
    }

    // write to data directory
    yield fs.writeFile(path.join(DATA_DIRECTORY, 'GeocodedAddresses.json'), JSON.stringify(Addresses))
    console.log(`Wrote ${Addresses.length} addresses to data directory`)
    // missing addresses
    if (MissingAddresses.length) {
      yield fs.writeFile(path.join(DATA_DIRECTORY, 'MissingAddresses.json'), JSON.stringify(MissingAddresses))
      console.warn(`There are ${MissingAddresses.length} gift aid claimants missing addresses`)
    }

  } catch (err) {
    console.error(err)
  } finally {
    knex.destroy()
  }
}

co(run)
